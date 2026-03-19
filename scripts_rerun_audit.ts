import { writeFileSync } from 'node:fs';
import { defaultInput } from './src/config.ts';
import { ingestAfmMar19 } from './src/sources/afmMar19.ts';
import { ingestAfmSubstantialHoldings } from './src/sources/afmSubstantialHoldings.ts';
import { dedupeSignals } from './src/dedupe/dedupeSignals.ts';
import { applyInstitutionalFilter } from './src/filters/institutionalFilter.ts';
import { scoreNaturalPersonConfidence } from './src/filters/personConfidence.ts';
import { enrichRecord } from './src/enrich/enrichRecord.ts';
import { scoreNlRelevance } from './src/scoring/scoreNlRelevance.ts';
import { scoreSignal } from './src/scoring/scoreSignal.ts';
import { applySignalGates } from './src/filters/signalGates.ts';
import { toReviewRecord } from './src/export/exportReviewDataset.ts';
import { toMatchReadyRecord } from './src/export/exportMatchReady.ts';
import type { ActorInput, NormalizedSignalRecord } from './src/types.ts';

const input: ActorInput = {
  ...defaultInput,
  runExaEnrichment: false,
  afmMar19CsvUrl: 'audit_inputs/afm_mar19_current_2026-03-19.csv',
  afmSubstantialHoldingsCsvUrl: 'audit_inputs/afm_substantial_current_2026-03-19.csv',
  maxReviewRecords: 30,
  maxMatchReadyRecords: 30,
};

const bucketOrder = { A: 0, B: 1, C: 2 } as const;

async function main() {
  const sourceStats = { afm_mar19: 0, afm_substantial: 0, exa_enriched: 0 };
  let records: NormalizedSignalRecord[] = [];
  const mar19 = await ingestAfmMar19(input.afmMar19CsvUrl);
  const substantial = await ingestAfmSubstantialHoldings(input.afmSubstantialHoldingsCsvUrl);
  sourceStats.afm_mar19 = mar19.length;
  sourceStats.afm_substantial = substantial.length;
  records = dedupeSignals([...mar19, ...substantial]);

  let excludedInstitutions = 0;
  let lowConfidenceRecords = 0;
  for (const record of records) {
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    await enrichRecord(record, input);
    if (record.enrichment_context) sourceStats.exa_enriched += 1;
    record.nl_relevance_score = scoreNlRelevance(record);
    scoreSignal(record, input.lookbackDays);
    record.match_ready = true;
    applySignalGates(record, input);
    if (input.excludeInstitutions && record.institutional_risk === 'high') excludedInstitutions += 1;
    if (record.signal_confidence < input.minSignalConfidence) lowConfidenceRecords += 1;
  }

  const postFilterRecords = input.excludeInstitutions ? records.filter((r) => r.institutional_risk !== 'high') : records;
  const review = postFilterRecords
    .filter((r) => r.signal_confidence >= 0.25)
    .sort((a, b) => bucketOrder[a.review_bucket] - bucketOrder[b.review_bucket] || b.signal_confidence - a.signal_confidence || b.nl_relevance_score - a.nl_relevance_score)
    .slice(0, input.maxReviewRecords)
    .map(toReviewRecord);
  const matchReady = postFilterRecords
    .filter((r) => r.match_ready)
    .sort((a, b) => b.signal_confidence - a.signal_confidence)
    .slice(0, input.maxMatchReadyRecords)
    .map(toMatchReadyRecord);
  const summary = {
    raw_records: records.length,
    post_filter_records: postFilterRecords.length,
    review_records: review.length,
    match_ready_records: matchReady.length,
    excluded_institutions: excludedInstitutions,
    low_confidence_records: lowConfidenceRecords,
    source_stats: sourceStats,
    review_bucket_stats: {
      A: postFilterRecords.filter((r) => r.review_bucket === 'A').length,
      B: postFilterRecords.filter((r) => r.review_bucket === 'B').length,
      C: postFilterRecords.filter((r) => r.review_bucket === 'C').length,
    },
    blocked_by_counts: Object.entries(postFilterRecords.reduce((acc, r) => {
      for (const reason of r.blocked_by) acc[reason] = (acc[reason] ?? 0) + 1;
      return acc;
    }, {} as Record<string, number>)).sort((a, b) => b[1] - a[1]),
  };

  writeFileSync('audit_outputs/top30_review.json', JSON.stringify(review, null, 2) + '\n');
  writeFileSync('audit_outputs/top30_match_ready.json', JSON.stringify(matchReady, null, 2) + '\n');
  writeFileSync('audit_outputs/run_summary.json', JSON.stringify(summary, null, 2) + '\n');
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
