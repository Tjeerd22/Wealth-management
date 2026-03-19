import { execSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { defaultInput } from './src/config.ts';
import { ingestAfmMar19 } from './src/sources/afmMar19.ts';
import { ingestAfmSubstantialHoldings } from './src/sources/afmSubstantialHoldings.ts';
import { dedupeSignals } from './src/dedupe/dedupeSignals.ts';
import { applyInstitutionalFilter } from './src/filters/institutionalFilter.ts';
import { scoreNaturalPersonConfidence } from './src/filters/personConfidence.ts';
import { enrichRecord } from './src/enrich/enrichRecord.ts';
import { confirmContextForTopReviewRecords } from './src/enrich/confirmContextForTopReviewRecords.ts';
import { scoreNlRelevance } from './src/scoring/scoreNlRelevance.ts';
import { scoreIssuerDesirability } from './src/scoring/scoreIssuerDesirability.ts';
import { scoreSignal } from './src/scoring/scoreSignal.ts';
import { applySignalGates } from './src/filters/signalGates.ts';
import { rankReviewRecords, toReviewRecord, topByIssuer } from './src/export/exportReviewDataset.ts';
import { toMatchReadyRecord } from './src/export/exportMatchReady.ts';
import type { ActorInput, NormalizedSignalRecord } from './src/types.ts';


function readPreviousJson<T>(preferredPath: string, fallbackPath?: string): T {
  const candidates = [preferredPath, fallbackPath].filter((value): value is string => Boolean(value));
  for (const path of candidates) {
    try {
      return JSON.parse(execSync(`git show HEAD^:${path} 2>/dev/null`, { encoding: 'utf8', shell: '/bin/bash' }));
    } catch {
      if (existsSync(path)) return JSON.parse(execSync(`cat ${path}`, { encoding: 'utf8' }));
    }
  }
  return [] as T;
}

const input: ActorInput = {
  ...defaultInput,
  runExaEnrichment: false,
  afmMar19CsvUrl: 'audit_inputs/afm_mar19_current_2026-03-19.csv',
  afmSubstantialHoldingsCsvUrl: 'audit_inputs/afm_substantial_current_2026-03-19.csv',
  maxReviewRecords: 20,
  maxMatchReadyRecords: 30,
};

async function main() {
  mkdirSync('audit_outputs', { recursive: true });
  const beforeTopReview = readPreviousJson<Record<string, unknown>[]>('audit_outputs/top20_review_overall.json', 'audit_outputs/top30_review.json');
  const beforeSummary = readPreviousJson<Record<string, unknown>>('audit_outputs/run_summary.json');

  const sourceStats = { afm_mar19: 0, afm_substantial: 0, exa_enriched: 0 };
  const mar19 = await ingestAfmMar19(input.afmMar19CsvUrl);
  const substantial = await ingestAfmSubstantialHoldings(input.afmSubstantialHoldingsCsvUrl);
  sourceStats.afm_mar19 = mar19.length;
  sourceStats.afm_substantial = substantial.length;

  let records: NormalizedSignalRecord[] = dedupeSignals([...mar19, ...substantial]);
  let excludedInstitutions = 0;
  let lowConfidenceRecords = 0;

  for (const record of records) {
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    await enrichRecord(record, input);
    if (record.enrichment_context) sourceStats.exa_enriched += 1;
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, input.lookbackDays);
    record.match_ready = true;
    applySignalGates(record, input);
    if (input.excludeInstitutions && record.institutional_risk === 'high') excludedInstitutions += 1;
    if (record.signal_confidence < input.minSignalConfidence) lowConfidenceRecords += 1;
  }

  records = input.excludeInstitutions ? records.filter((record) => record.institutional_risk !== 'high') : records;
  const rankedReview = rankReviewRecords(records);
  await confirmContextForTopReviewRecords(rankedReview, input);

  const topOverall = rankedReview.slice(0, input.maxReviewRecords).map(toReviewRecord);
  const topPerIssuer = topByIssuer(records, 3);
  const confirmationSubset = rankedReview
    .filter((record) => record.confirmation_summary || record.confirmation_urls.length)
    .slice(0, Math.max(input.exaTopReviewConfirmations + 10, 20))
    .map(toReviewRecord);
  const matchReady = records
    .filter((record) => record.match_ready)
    .sort((a, b) => b.signal_confidence - a.signal_confidence)
    .slice(0, input.maxMatchReadyRecords)
    .map(toMatchReadyRecord);

  const summary = {
    raw_records: mar19.length + substantial.length,
    post_filter_records: records.length,
    review_records: topOverall.length,
    match_ready_records: matchReady.length,
    excluded_institutions: excludedInstitutions,
    low_confidence_records: lowConfidenceRecords,
    source_stats: sourceStats,
    review_bucket_stats: {
      A: records.filter((record) => record.review_bucket === 'A').length,
      B: records.filter((record) => record.review_bucket === 'B').length,
      C: records.filter((record) => record.review_bucket === 'C').length,
    },
    blocked_by_counts: Object.entries(records.reduce((acc, record) => {
      for (const reason of record.blocked_by) acc[reason] = (acc[reason] ?? 0) + 1;
      return acc;
    }, {} as Record<string, number>)).sort((a, b) => b[1] - a[1]),
  };

  const comparison = {
    before_summary: beforeSummary,
    after_summary: summary,
    before_top_review_sample: beforeTopReview.slice(0, 10).map((record: Record<string, unknown>) => ({
      record_id: record.record_id,
      company_name: record.company_name,
      person_name: record.person_name,
      review_bucket: record.review_bucket,
      signal_confidence: record.signal_confidence,
      review_action: record.review_action,
    })),
    after_top_review_sample: topOverall.slice(0, 10).map((record) => ({
      record_id: record.record_id,
      company_name: record.company_name,
      person_name: record.person_name,
      review_bucket: record.review_bucket,
      signal_confidence: record.signal_confidence,
      review_action: record.review_action,
      context_confirmed: record.context_confirmed,
      confirmation_evidence_strength: record.confirmation_evidence_strength,
      review_action_updated: record.review_action_updated,
    })),
  };

  writeFileSync('audit_outputs/top20_review_overall.json', JSON.stringify(topOverall, null, 2) + '\n');
  writeFileSync('audit_outputs/top3_review_by_issuer.json', JSON.stringify(topPerIssuer, null, 2) + '\n');
  writeFileSync('audit_outputs/confirmation_enriched_review_subset.json', JSON.stringify(confirmationSubset, null, 2) + '\n');
  writeFileSync('audit_outputs/top30_match_ready.json', JSON.stringify(matchReady, null, 2) + '\n');
  writeFileSync('audit_outputs/run_summary.json', JSON.stringify(summary, null, 2) + '\n');
  writeFileSync('audit_outputs/before_after_comparison.json', JSON.stringify(comparison, null, 2) + '\n');
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
