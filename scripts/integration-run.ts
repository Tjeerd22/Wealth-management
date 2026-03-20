/**
 * Full pipeline integration run — live AFM data + Exa enrichment.
 *
 * Usage:
 *   EXA_API_KEY=<key> npx tsx scripts/integration-run.ts
 *   # or with a local .env.local file:
 *   npx tsx --env-file=.env.local scripts/integration-run.ts
 *
 * Flags:
 *   --no-exa    Skip Exa enrichment and confirmation (faster, offline-safe)
 *   --no-substantial  Skip the AFM substantial holdings source
 */

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { writeFileSync, mkdirSync } from 'node:fs';
import { ingestAfmMar19 } from '../src/sources/afmMar19.js';
import { ingestAfmSubstantialHoldings } from '../src/sources/afmSubstantialHoldings.js';
import { dedupeSignalsWithStats } from '../src/dedupe/dedupeSignals.js';
import { applyInstitutionalFilter } from '../src/filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from '../src/filters/personConfidence.js';
import { scoreNlRelevance } from '../src/scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from '../src/scoring/scoreIssuerDesirability.js';
import { scoreSignal } from '../src/scoring/scoreSignal.js';
import { applySignalGates } from '../src/filters/signalGates.js';
import { rankReviewRecords } from '../src/export/exportReviewDataset.js';
import { confirmContextForTopReviewRecords } from '../src/enrich/confirmContextForTopReviewRecords.js';
import { enrichRecord } from '../src/enrich/enrichRecord.js';
import type { ActorInput, BlockedReason, NormalizedSignalRecord } from '../src/types.js';

// Fixture paths used as fallback when live AFM endpoints are unreachable.
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_MAR19 = path.resolve(__dirname, '../tests/fixtures/afm_mar19_integration.csv');
const FIXTURE_SUBSTANTIAL = path.resolve(__dirname, '../tests/fixtures/afm_substantial_integration.csv');

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

const useExa = !process.argv.includes('--no-exa');
const useSubstantial = !process.argv.includes('--no-substantial');

const EXA_API_KEY = process.env.EXA_API_KEY ?? '';
const exaEnabled = useExa && Boolean(EXA_API_KEY);

// ---------------------------------------------------------------------------
// Source URLs (date-filtered to Feb 6 2026 onwards)
// ---------------------------------------------------------------------------

const MAR19_URL = 'https://www.afm.nl/export.aspx?DateFrom=06-02-2026&type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=csv';
const SUBSTANTIAL_URL = 'https://www.afm.nl/export.aspx?DateFrom=06-02-2026&type=1331d46f-3fb6-4a36-b903-9584972675af&format=csv';
const LOOKBACK_DAYS = 60;

// ---------------------------------------------------------------------------
// Full ActorInput used throughout pipeline stages
// ---------------------------------------------------------------------------

const pipelineInput: ActorInput = {
  runAfmMar19: true,
  runAfmSubstantialHoldings: useSubstantial,
  runExaEnrichment: exaEnabled,
  runExaConfirmation: exaEnabled,
  afmMar19CsvUrl: MAR19_URL,
  afmSubstantialHoldingsCsvUrl: SUBSTANTIAL_URL,
  lookbackDays: LOOKBACK_DAYS,
  minSignalConfidence: 0.30,       // lowered from default 0.6 so we can observe more records
  minNaturalPersonConfidence: 0.30, // lowered for same reason
  excludeInstitutions: false,       // keep all so we can report on them
  maxReviewRecords: 200,
  maxMatchReadyRecords: 50,
  maxShortlistRecords: 100,
  topBucketBForExa: 5,
  exaApiKey: EXA_API_KEY,
  exaTopReviewConfirmations: 5,
  exaFreshnessMaxAgeHours: 72,
  debug: false,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function pct(n: number, total: number): string {
  if (!total) return '0%';
  return `${((n / total) * 100).toFixed(1)}%`;
}

function fmt(n: number): string {
  return n.toFixed(3);
}

function bar(n: number, total: number, width = 20): string {
  const filled = Math.round((n / Math.max(1, total)) * width);
  return `[${'█'.repeat(filled)}${'░'.repeat(width - filled)}]`;
}

function bucket(values: number[], thresholds: number[]): number[] {
  const counts = new Array<number>(thresholds.length + 1).fill(0);
  for (const v of values) {
    let placed = false;
    for (let i = 0; i < thresholds.length; i++) {
      if (v < thresholds[i]) { counts[i]++; placed = true; break; }
    }
    if (!placed) counts[thresholds.length]++;
  }
  return counts;
}

function section(title: string): void {
  console.log(`\n${'═'.repeat(72)}`);
  console.log(`  ${title}`);
  console.log('═'.repeat(72));
}

function sub(title: string): void {
  console.log(`\n  ── ${title}`);
}

function row(label: string, value: string | number): void {
  console.log(`  ${label.padEnd(36)} ${value}`);
}

function warn(msg: string): void {
  console.log(`  ⚠  ${msg}`);
}

function ok(msg: string): void {
  console.log(`  ✓  ${msg}`);
}

function bug(msg: string): void {
  console.log(`\n  🐛 BUG FOUND: ${msg}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const bugs: string[] = [];
const fixes: string[] = [];

async function main(): Promise<void> {
  console.log('\n╔══════════════════════════════════════════════════════════════════════╗');
  console.log('║   Dutch HNWI Signal Pipeline — Full Integration Run                 ║');
  console.log('╚══════════════════════════════════════════════════════════════════════╝');
  console.log(`\n  Date:        ${new Date().toISOString()}`);
  console.log(`  Exa:         ${exaEnabled ? `enabled (key: ${EXA_API_KEY.slice(0, 8)}…)` : 'disabled'}`);
  console.log(`  Substantial: ${useSubstantial ? 'enabled' : 'disabled'}`);
  console.log(`  Lookback:    ${LOOKBACK_DAYS} days`);

  // ── 1. INGESTION ──────────────────────────────────────────────────────────

  section('1. INGESTION');

  let mar19Records: NormalizedSignalRecord[] = [];
  let substantialRecords: NormalizedSignalRecord[] = [];
  let mar19Error: string | null = null;
  let substantialError: string | null = null;

  const t0 = Date.now();

  process.stdout.write('  Fetching AFM MAR 19… ');
  try {
    mar19Records = await ingestAfmMar19(MAR19_URL);
    console.log(`${mar19Records.length} records (${Date.now() - t0}ms)`);
  } catch (e) {
    mar19Error = e instanceof Error ? e.message : String(e);
    console.log(`FAILED — ${mar19Error}`);
    console.log('  Falling back to fixture file…');
    try {
      mar19Records = await ingestAfmMar19(FIXTURE_MAR19);
      mar19Error = null;
      console.log(`  Fixture loaded: ${mar19Records.length} records`);
    } catch (fe) {
      console.log(`  Fixture also failed: ${fe instanceof Error ? fe.message : String(fe)}`);
    }
  }

  if (useSubstantial) {
    const t1 = Date.now();
    process.stdout.write('  Fetching AFM Substantial Holdings… ');
    try {
      substantialRecords = await ingestAfmSubstantialHoldings(SUBSTANTIAL_URL, LOOKBACK_DAYS);
      console.log(`${substantialRecords.length} records (${Date.now() - t1}ms)`);
    } catch (e) {
      substantialError = e instanceof Error ? e.message : String(e);
      console.log(`FAILED — ${substantialError}`);
      console.log('  Falling back to fixture file…');
      try {
        substantialRecords = await ingestAfmSubstantialHoldings(FIXTURE_SUBSTANTIAL, LOOKBACK_DAYS);
        substantialError = null;
        console.log(`  Fixture loaded: ${substantialRecords.length} records`);
      } catch (fe) {
        console.log(`  Fixture also failed: ${fe instanceof Error ? fe.message : String(fe)}`);
      }
    }
  }

  if (mar19Error) {
    console.log('\n  FATAL: MAR 19 source failed and fixture fallback also failed. Cannot continue.\n');
    process.exit(1);
  }

  // Source-role sanity check
  const mar19WrongRole = mar19Records.filter((r) => r.source_role !== 'primary').length;
  const subWrongRole = substantialRecords.filter((r) => r.source_role !== 'secondary_confirmation').length;

  if (mar19WrongRole > 0) {
    bug(`${mar19WrongRole} MAR 19 records have source_role !== 'primary'`);
    bugs.push('MAR 19 source_role not set to primary');
  } else {
    ok('All MAR 19 records have source_role=primary');
  }

  if (substantialRecords.length > 0) {
    if (subWrongRole > 0) {
      bug(`${subWrongRole} substantial records have source_role !== 'secondary_confirmation'`);
      bugs.push('Substantial source_role not set to secondary_confirmation');
    } else {
      ok('All substantial holdings records have source_role=secondary_confirmation');
    }
  }

  // Signal interpretation field check
  const mar19ClarityOk = mar19Records.every((r) => r.signal_clarity === 'inferred' && r.signal_direction === 'unclear' && r.liquidity_relevance === 0.5);
  if (!mar19ClarityOk) {
    bug('Some MAR 19 records have unexpected signal interpretation fields');
    bugs.push('MAR 19 signal interpretation fields incorrect');
  } else {
    ok('MAR 19 signal interpretation fields correct (clarity=inferred, direction=unclear, liquidity_relevance=0.5)');
  }

  row('MAR 19 records', mar19Records.length);
  row('Substantial holdings records', substantialRecords.length);
  row('Total pre-dedupe', mar19Records.length + substantialRecords.length);

  // ── 2. DEDUPE ─────────────────────────────────────────────────────────────

  section('2. DEDUPE');

  const allRaw = [...mar19Records, ...substantialRecords];
  const dedupeResult = dedupeSignalsWithStats(allRaw);
  const records = dedupeResult.records;
  const { stats } = dedupeResult;

  row('Before dedupe', stats.recordsBefore);
  row('After dedupe', stats.recordsAfter);
  row('Merges performed', stats.mergesPerformed);
  row('Reduction ratio', `${(stats.reductionRatio * 100).toFixed(1)}%`);
  row('Suspicious groups', stats.suspiciousGroups.length);

  if (stats.reductionRatio > 0.5) {
    warn(`High reduction ratio (${(stats.reductionRatio * 100).toFixed(1)}%) — inspect suspicious groups`);
  }

  const mergedWithMar19 = records.filter((r) => r.source_role === 'secondary_confirmation' && (r.provenance_sources ?? []).includes('afm_mar19'));
  const standaloneSecondary = records.filter((r) => r.source_role === 'secondary_confirmation' && !(r.provenance_sources ?? []).includes('afm_mar19'));

  row('Secondary records merged with MAR 19', mergedWithMar19.length);
  row('Standalone secondary records', standaloneSecondary.length);

  if (stats.suspiciousGroups.length > 0) {
    sub('Suspicious dedupe groups (top 3)');
    for (const g of stats.suspiciousGroups.slice(0, 3)) {
      console.log(`    size=${g.size} key="${g.groupKey.slice(0, 60)}"`);
    }
  }

  // ── 3. SCORING ────────────────────────────────────────────────────────────

  section('3. SCORING');
  process.stdout.write('  Running full scoring pipeline');
  if (exaEnabled) process.stdout.write(' + Exa enrichment');
  console.log('…');

  const enrichErrors: string[] = [];

  // Pass 1: fast pre-scoring (no I/O) — sets NPC so Exa delta can add to it
  for (const record of records) {
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
  }

  // Pass 2: parallel Exa enrichment in batches of 10 (adds NPC delta + sets enrichment context)
  if (exaEnabled) {
    const BATCH = 10;
    for (let i = 0; i < records.length; i += BATCH) {
      await Promise.allSettled(
        records.slice(i, i + BATCH).map(async (r) => {
          try {
            await enrichRecord(r, pipelineInput);
          } catch (e) {
            enrichErrors.push(`${r.record_id}: ${e instanceof Error ? e.message : String(e)}`);
          }
        }),
      );
    }
  }

  // Pass 3: complete scoring (uses enrichment_context / role set by Exa)
  for (const record of records) {
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, LOOKBACK_DAYS);
    record.match_ready = true;
    applySignalGates(record, pipelineInput);
  }

  if (enrichErrors.length > 0) {
    bug(`${enrichErrors.length} Exa enrichment errors (first: ${enrichErrors[0]})`);
    bugs.push(`Exa enrichment threw on ${enrichErrors.length} records — needs try/catch in enrichRecord`);
  }

  console.log(`  Scored ${records.length} records`);

  // Distributions
  const npcValues = records.map((r) => r.natural_person_confidence);
  const scValues = records.map((r) => r.signal_confidence);
  const nlValues = records.map((r) => r.nl_relevance_score);
  const wrsValues = records.map((r) => r.wealth_relevance_score);
  const lrValues = records.map((r) => r.liquidity_relevance);

  const thresholds = [0.2, 0.4, 0.45, 0.6, 0.75, 0.9];

  function showDist(label: string, values: number[]): void {
    const avg = values.reduce((a, b) => a + b, 0) / Math.max(1, values.length);
    const sorted = [...values].sort((a, b) => a - b);
    const p50 = sorted[Math.floor(sorted.length * 0.5)] ?? 0;
    const p90 = sorted[Math.floor(sorted.length * 0.9)] ?? 0;
    console.log(`\n  ${label}`);
    console.log(`    avg=${fmt(avg)}  p50=${fmt(p50)}  p90=${fmt(p90)}`);
    const buckets = bucket(values, [0.2, 0.4, 0.6, 0.8]);
    const labels = ['<0.20', '0.20–0.40', '0.40–0.60', '0.60–0.80', '≥0.80'];
    for (let i = 0; i < labels.length; i++) {
      const count = buckets[i];
      console.log(`    ${labels[i].padEnd(12)} ${bar(count, values.length)} ${count} ${pct(count, values.length)}`);
    }
  }

  showDist('natural_person_confidence', npcValues);
  showDist('signal_confidence', scValues);
  showDist('nl_relevance_score', nlValues);
  showDist('wealth_relevance_score', wrsValues);

  // Signal confidence cap check: no pdmr record should exceed 0.58
  const pdmrOver58 = records.filter((r) => r.signal_type === 'pdmr_transaction_unconfirmed' && r.signal_confidence > 0.58 + 0.001);
  if (pdmrOver58.length > 0) {
    bug(`${pdmrOver58.length} pdmr_transaction_unconfirmed records have signal_confidence > 0.58 after scoring`);
    bugs.push('Phase 6 cap consolidation not working: pdmr records exceed 0.58');
  } else {
    ok('All pdmr_transaction_unconfirmed records have signal_confidence ≤ 0.58 (phase-6 cap correct)');
  }

  // Wealth relevance sanity: wealth_relevance_score should be 0 on first scoreSignal call
  // (i.e. we check it's non-zero, meaning the formula ran)
  const wrsZero = records.filter((r) => r.wealth_relevance_score === 0);
  if (wrsZero.length > 0) {
    bug(`${wrsZero.length} records still have wealth_relevance_score=0 after scoring`);
    bugs.push('wealth_relevance_score not being computed by scoreSignal');
  } else {
    ok(`All ${records.length} records have non-zero wealth_relevance_score`);
  }

  // context_summary check
  const badSummary = records.filter((r) => !r.context_summary || r.context_summary.trim().length < 10);
  if (badSummary.length > 0) {
    bug(`${badSummary.length} records have blank or very short context_summary`);
    bugs.push('context_summary not being generated in normalizeRecord');
  } else {
    ok('context_summary populated for all records');
  }

  // ── 4. INSTITUTIONAL FILTER ────────────────────────────────────────────────

  section('4. INSTITUTIONAL FILTER');

  const highRisk = records.filter((r) => r.institutional_risk === 'high');
  const lowRisk = records.filter((r) => r.institutional_risk === 'low');
  const unknownRisk = records.filter((r) => r.institutional_risk === 'unknown');

  row('High institutional risk', `${highRisk.length} ${pct(highRisk.length, records.length)}`);
  row('Low institutional risk', `${lowRisk.length} ${pct(lowRisk.length, records.length)}`);
  row('Unknown institutional risk', `${unknownRisk.length} ${pct(unknownRisk.length, records.length)}`);

  if (highRisk.length / Math.max(1, records.length) > 0.7) {
    warn('Over 70% of records flagged as high institutional risk — may indicate filter is over-broad');
  }

  // ── 5. GATE ANALYSIS ──────────────────────────────────────────────────────

  section('5. GATE ANALYSIS');

  const blockedCounts = new Map<string, number>();
  for (const record of records) {
    for (const reason of record.blocked_by) {
      blockedCounts.set(reason, (blockedCounts.get(reason) ?? 0) + 1);
    }
  }

  const blockedAny = records.filter((r) => r.blocked_by.length > 0);
  const blockedNone = records.filter((r) => r.blocked_by.length === 0);

  row('Records with at least one blocker', `${blockedAny.length} ${pct(blockedAny.length, records.length)}`);
  row('Records with no blockers', `${blockedNone.length} ${pct(blockedNone.length, records.length)}`);

  sub('Blocker breakdown');
  const sortedBlockers = [...blockedCounts.entries()].sort((a, b) => b[1] - a[1]);
  for (const [reason, count] of sortedBlockers) {
    console.log(`    ${reason.padEnd(40)} ${bar(count, records.length, 15)} ${count} ${pct(count, records.length)}`);
  }

  const secondaryBlocked = records.filter((r) => r.blocked_by.includes('secondary_source_no_primary_match' as BlockedReason));
  row('\n  Secondary records blocked (no MAR 19 merge)', secondaryBlocked.length);

  if (blockedNone.length === 0) {
    warn('No records passed all gates — match_ready will be 0 even with Exa enabled');
  }

  // ── 6. REVIEW BUCKETS ─────────────────────────────────────────────────────

  section('6. REVIEW BUCKETS');

  const bucketA = records.filter((r) => r.review_bucket === 'A');
  const bucketB = records.filter((r) => r.review_bucket === 'B');
  const bucketC = records.filter((r) => r.review_bucket === 'C');

  row('Bucket A (high-confidence, commercial interest)', `${bucketA.length} ${pct(bucketA.length, records.length)}`);
  row('Bucket B (likely person, needs context)', `${bucketB.length} ${pct(bucketB.length, records.length)}`);
  row('Bucket C (low confidence / unclear)', `${bucketC.length} ${pct(bucketC.length, records.length)}`);

  if (bucketA.length === 0 && bucketB.length === 0) {
    warn('No A or B bucket records — shortlist will be empty (scoring thresholds may need tuning)');
  }

  // ── 7. SHORTLIST ──────────────────────────────────────────────────────────

  section('7. SHORTLIST ELIGIBILITY');

  const shortlist = records.filter((r) => r.shortlist_eligible);
  const shortlistByBucket = {
    A: shortlist.filter((r) => r.review_bucket === 'A').length,
    B: shortlist.filter((r) => r.review_bucket === 'B').length,
  };

  row('shortlist_eligible records', shortlist.length);
  row('  from bucket A', shortlistByBucket.A);
  row('  from bucket B', shortlistByBucket.B);
  row('Target (spec)', '30–100');

  if (shortlist.length === 0) {
    warn('Shortlist is EMPTY — below the 30-record spec target');
  } else if (shortlist.length < 30) {
    warn(`Shortlist has ${shortlist.length} records — BELOW the 30-record spec minimum`);
  } else if (shortlist.length > 100) {
    warn(`Shortlist has ${shortlist.length} records — ABOVE the 100-record spec maximum (cap at ${pipelineInput.maxShortlistRecords})`);
  } else {
    ok(`Shortlist size ${shortlist.length} is within the 30–100 spec range`);
  }

  // Direction breakdown
  const sellSignals = shortlist.filter((r) => r.signal_direction === 'sell');
  const unclearSignals = shortlist.filter((r) => r.signal_direction === 'unclear');
  const buySignals = shortlist.filter((r) => r.signal_direction === 'buy');
  row('\n  signal_direction breakdown in shortlist', '');
  row('  sell', `${sellSignals.length} ${pct(sellSignals.length, shortlist.length)}`);
  row('  unclear', `${unclearSignals.length} ${pct(unclearSignals.length, shortlist.length)}`);
  row('  buy', `${buySignals.length} ${pct(buySignals.length, shortlist.length)}`);

  // ── 8. EXA CONFIRMATION ───────────────────────────────────────────────────

  if (exaEnabled) {
    section('8. EXA CONFIRMATION (top review records)');
    const ranked = rankReviewRecords(records);
    console.log(`  Running Exa confirmation on top ${Math.min(ranked.filter((r) => r.review_bucket === 'A').length, 999) + Math.min(pipelineInput.topBucketBForExa, 5)} records…`);
    try {
      await confirmContextForTopReviewRecords(ranked, pipelineInput);
    } catch (e) {
      warn(`Exa confirmation threw: ${e instanceof Error ? e.message : String(e)}`);
      bugs.push('confirmContextForTopReviewRecords threw an error at runtime');
    }

    const confirmed = records.filter((r) => r.context_confirmed);
    const disposalConfirmed = records.filter((r) => r.disposal_confirmed);
    const updatedReference = records.filter((r) => r.evidence_reference !== r.source_url);

    row('Records with context_confirmed=true', confirmed.length);
    row('Records with disposal_confirmed=true', disposalConfirmed.length);
    row('Records with evidence_reference updated to Exa URL', updatedReference.length);

    if (confirmed.length > 0 && updatedReference.length === 0) {
      bug('context_confirmed=true but no records had evidence_reference updated — applyConfirmation gate not firing');
      bugs.push('evidence_reference not updated despite context_confirmed=true');
    }
  }

  // ── 9. MATCH READY ────────────────────────────────────────────────────────

  section('9. MATCH READY');

  const matchReady = records.filter((r) => r.match_ready);
  row('match_ready records', matchReady.length);

  if (matchReady.length === 0 && exaEnabled) {
    warn('0 match-ready records even with Exa enabled — gates may be too strict or Exa enrichment not lifting context');
  } else if (matchReady.length === 0) {
    console.log('  (Expected: match_ready requires enrichment_context or role data from Exa; 0 is normal without Exa)');
  } else {
    ok(`${matchReady.length} records reached match_ready=true`);
  }

  // ── 10. TOP SHORTLIST RECORDS ─────────────────────────────────────────────

  section('10. TOP SHORTLIST RECORDS');

  const rankedShortlist = rankReviewRecords(records)
    .filter((r) => r.shortlist_eligible)
    .slice(0, 15);

  if (rankedShortlist.length === 0) {
    console.log('  (no shortlist records to display)');
  } else {
    console.log(`\n  ${'#'.padEnd(3)} ${'Person'.padEnd(28)} ${'Company'.padEnd(26)} ${'npc'.padEnd(5)} ${'sc'.padEnd(5)} ${'wrs'.padEnd(5)} ${'bucket'}`);
    console.log(`  ${'─'.repeat(85)}`);
    for (let i = 0; i < rankedShortlist.length; i++) {
      const r = rankedShortlist[i];
      console.log(
        `  ${String(i + 1).padEnd(3)} ${r.person_name.slice(0, 27).padEnd(28)} ${r.company_name.slice(0, 25).padEnd(26)}` +
        ` ${fmt(r.natural_person_confidence).padEnd(5)} ${fmt(r.signal_confidence).padEnd(5)} ${fmt(r.wealth_relevance_score).padEnd(5)} ${r.review_bucket}`,
      );
      if (r.context_summary) {
        console.log(`       context: ${r.context_summary.slice(0, 100)}${r.context_summary.length > 100 ? '…' : ''}`);
      }
      if (r.evidence_reference && r.evidence_reference !== r.source_url) {
        console.log(`       evidence_ref: ${r.evidence_reference.slice(0, 80)}`);
      }
    }
  }

  // ── 11. EXAMPLE CONTEXT SUMMARIES ─────────────────────────────────────────

  section('11. EXAMPLE CONTEXT SUMMARIES (first 5 records)');

  for (const r of records.slice(0, 5)) {
    console.log(`\n  Person: ${r.person_name} @ ${r.company_name}`);
    console.log(`  context_summary: ${r.context_summary}`);
    console.log(`  evidence_reference: ${r.evidence_reference}`);
    console.log(`  wealth_relevance_score: ${fmt(r.wealth_relevance_score)}`);
  }

  // ── 12. DESIGN ALIGNMENT ANALYSIS ─────────────────────────────────────────

  section('12. DESIGN ALIGNMENT ANALYSIS');

  console.log(`
  The pipeline spec targets the following outcomes. Here is how the live run compares:

  ┌─────────────────────────────────────┬──────────────────┬────────────────────────────┐
  │ Spec Target                         │ Actual           │ Status                     │
  ├─────────────────────────────────────┼──────────────────┼────────────────────────────┤`);

  function alignRow(label: string, actual: string, status: string): void {
    console.log(`  │ ${label.padEnd(35)} │ ${actual.padEnd(16)} │ ${status.padEnd(26)} │`);
  }

  const shortlistStatus = shortlist.length >= 30 && shortlist.length <= 100
    ? '✓ in range'
    : shortlist.length < 30
      ? `⚠ below min (${shortlist.length})`
      : `⚠ above max (${shortlist.length})`;

  alignRow('Shortlist: 30–100 records', String(shortlist.length), shortlistStatus);
  alignRow('MAR 19 = primary source_role', `${mar19Records.length - mar19WrongRole}/${mar19Records.length}`, mar19WrongRole === 0 ? '✓' : '⚠ wrong role on some');
  alignRow('Substantial = secondary_confirmation', `${substantialRecords.length - subWrongRole}/${substantialRecords.length}`, subWrongRole === 0 ? '✓' : '⚠ wrong role on some');
  alignRow('All pdmr sc ≤ 0.58', pdmrOver58.length === 0 ? 'all capped' : `${pdmrOver58.length} over`, pdmrOver58.length === 0 ? '✓ Phase 6 correct' : '⚠ cap not working');
  alignRow('wealth_relevance_score computed', wrsZero.length === 0 ? 'all non-zero' : `${wrsZero.length} zero`, wrsZero.length === 0 ? '✓' : '⚠ formula not firing');
  alignRow('context_summary populated', badSummary.length === 0 ? 'all populated' : `${badSummary.length} blank`, badSummary.length === 0 ? '✓' : '⚠ template missing');
  alignRow('Institutional filter < 70%', `${pct(highRisk.length, records.length)}`, highRisk.length / Math.max(1, records.length) < 0.7 ? '✓ reasonable' : '⚠ over-broad');

  console.log('  └─────────────────────────────────────┴──────────────────┴────────────────────────────┘');

  // Detailed shortlist diagnosis if below target
  if (shortlist.length < 30) {
    console.log(`
  WHY THE SHORTLIST IS BELOW 30:
  Shortlist requires:
    • natural_person_confidence ≥ 0.45 — records with < 0.45: ${records.filter((r) => r.natural_person_confidence < 0.45).length}
    • signal_confidence ≥ 0.40        — records with < 0.40: ${records.filter((r) => r.signal_confidence < 0.40).length}
    • review_bucket A or B            — records in A or B: ${bucketA.length + bucketB.length}
    • primary or merged secondary     — standalone secondary: ${standaloneSecondary.length}
    • signal_type 'unclear' only if
      liquidity_relevance ≥ 0.6       — unclear with lr < 0.6: ${records.filter((r) => r.signal_type.includes('unclear') && r.liquidity_relevance < 0.6).length}

  All criteria simultaneously:          ${shortlist.length} records`);
  }

  // ── 13. BUGS FOUND ────────────────────────────────────────────────────────

  section('13. BUGS FOUND & FIXES APPLIED');

  if (bugs.length === 0) {
    ok('No bugs found in this run');
  } else {
    for (let i = 0; i < bugs.length; i++) {
      console.log(`  ${i + 1}. ${bugs[i]}`);
    }
  }

  if (fixes.length > 0) {
    sub('Fixes applied during this run:');
    for (const fix of fixes) {
      console.log(`  • ${fix}`);
    }
  }

  // ── CSV EXPORT ────────────────────────────────────────────────────────────

  section('CSV EXPORT');

  const csvHeaders = [
    'record_id', 'person_name', 'company_name', 'signal_date', 'signal_type',
    'signal_direction', 'natural_person_confidence', 'signal_confidence',
    'wealth_relevance_score', 'nl_relevance_score', 'issuer_desirability_score',
    'review_bucket', 'shortlist_eligible', 'match_ready', 'institutional_risk',
    'blocked_by', 'context_summary', 'evidence_reference', 'role', 'company_country',
  ];

  function csvEscape(v: unknown): string {
    const s = v == null ? '' : String(v);
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  }

  const rankedAll = rankReviewRecords(records);
  const csvRows = [
    csvHeaders.join(','),
    ...rankedAll.map((r) =>
      [
        r.record_id, r.person_name, r.company_name, r.signal_date, r.signal_type,
        r.signal_direction,
        r.natural_person_confidence.toFixed(3), r.signal_confidence.toFixed(3),
        r.wealth_relevance_score.toFixed(3), r.nl_relevance_score.toFixed(3),
        r.issuer_desirability_score.toFixed(3),
        r.review_bucket ?? '', r.shortlist_eligible ? 'true' : 'false',
        r.match_ready ? 'true' : 'false', r.institutional_risk ?? '',
        (r.blocked_by ?? []).join('|'), r.context_summary ?? '',
        r.evidence_reference ?? '', r.role ?? '', r.company_country ?? '',
      ].map(csvEscape).join(','),
    ),
  ];

  const csvPath = path.resolve(__dirname, `../output/pipeline-results-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.csv`);
  mkdirSync(path.dirname(csvPath), { recursive: true });
  writeFileSync(csvPath, csvRows.join('\n'), 'utf8');
  console.log(`\n  CSV written: ${csvPath}`);
  console.log(`  Rows: ${rankedAll.length} records`);

  // ── Summary ───────────────────────────────────────────────────────────────

  section('SUMMARY');

  const totalMs = Date.now() - t0;
  row('Total runtime', `${(totalMs / 1000).toFixed(1)}s`);
  row('Records ingested', mar19Records.length + substantialRecords.length);
  row('Records after dedupe', records.length);
  row('Shortlist eligible', shortlist.length);
  row('Match ready', matchReady.length);
  row('Bugs found', bugs.length);
  console.log('');
}

main().catch((err) => {
  console.error('\n[FATAL]', err);
  process.exit(1);
});
