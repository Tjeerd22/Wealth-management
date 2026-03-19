import { Actor } from 'apify';
import { defaultInput } from './config.js';
import { ingestAfmMar19 } from './sources/afmMar19.js';
import { ingestAfmSubstantialHoldings } from './sources/afmSubstantialHoldings.js';
import { applyInstitutionalFilter } from './filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from './filters/personConfidence.js';
import { enrichRecord } from './enrich/enrichRecord.js';
import { scoreSignal } from './scoring/scoreSignal.js';
import { applySignalGates } from './filters/signalGates.js';
import { dedupeSignals } from './dedupe/dedupeSignals.js';
import { exportRawArchive } from './export/exportRawArchive.js';
import { exportReviewDataset } from './export/exportReviewDataset.js';
import { exportMatchReady } from './export/exportMatchReady.js';
import { ActorInput, NormalizedSignalRecord, RunSummary } from './types.js';
import { logInfo } from './utils/logging.js';

async function run(): Promise<void> {
  await Actor.init();
  const input = { ...defaultInput, ...(await Actor.getInput<Partial<ActorInput>>() ?? {}) };
  const sourceStats = { afm_mar19: 0, afm_substantial: 0, exa_enriched: 0 };
  let records: NormalizedSignalRecord[] = [];

  if (input.runAfmMar19) {
    const mar19 = await ingestAfmMar19(input.afmMar19CsvUrl || defaultInput.afmMar19CsvUrl);
    sourceStats.afm_mar19 = mar19.length;
    records.push(...mar19);
  }

  if (input.runAfmSubstantialHoldings) {
    const substantial = await ingestAfmSubstantialHoldings(input.afmSubstantialHoldingsCsvUrl || defaultInput.afmSubstantialHoldingsCsvUrl);
    sourceStats.afm_substantial = substantial.length;
    records.push(...substantial);
  }

  records = dedupeSignals(records);
  let excludedInstitutions = 0;
  let lowConfidenceRecords = 0;

  for (const record of records) {
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    await enrichRecord(record, input);
    if (record.enrichment_context) sourceStats.exa_enriched += 1;
    scoreSignal(record, input.lookbackDays);
    record.match_ready = true;
    applySignalGates(record, input);
    if (input.excludeInstitutions && record.institutional_risk === 'high') excludedInstitutions += 1;
    if (record.signal_confidence < input.minSignalConfidence) lowConfidenceRecords += 1;
  }

  const postFilterRecords = input.excludeInstitutions
    ? records.filter((record) => record.institutional_risk !== 'high')
    : records;

  await exportRawArchive(records);
  const review = await exportReviewDataset(postFilterRecords, input.maxReviewRecords);
  const matchReady = await exportMatchReady(postFilterRecords, input.maxMatchReadyRecords);

  const summary: RunSummary = {
    raw_records: records.length,
    post_filter_records: postFilterRecords.length,
    review_records: review.length,
    match_ready_records: matchReady.length,
    excluded_institutions: excludedInstitutions,
    low_confidence_records: lowConfidenceRecords,
    source_stats: sourceStats,
  };

  logInfo('Run summary', summary);
  await Actor.setValue('RUN_SUMMARY', summary);
  await Actor.setValue('INPUT_SCHEMA', (await import('./inputSchema.js')).inputSchema);
  await Actor.exit();
}

void run();
