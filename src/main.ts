import { Actor } from 'apify';
import { fileURLToPath } from 'node:url';
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
import { exportReviewDataset, rankReviewRecords } from './export/exportReviewDataset.js';
import { exportMatchReady } from './export/exportMatchReady.js';
import { scoreNlRelevance } from './scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from './scoring/scoreIssuerDesirability.js';
import { ActorInput, NormalizedSignalRecord, RunSummary } from './types.js';
import { logInfo, logWarn } from './utils/logging.js';
import { confirmContextForTopReviewRecords } from './enrich/confirmContextForTopReviewRecords.js';

type InputAliasMap = Partial<ActorInput> & Record<string, unknown>;

interface NormalizedRuntimeConfig {
  runAfmMar19: boolean;
  runAfmSubstantialHoldings: boolean;
  runExaConfirmation: boolean;
  afmMar19CsvUrl: string;
  afmSubstantialHoldingsCsvUrl: string;
  lookbackDays: number;
  minSignalConfidence: number;
  minNaturalPersonConfidence: number;
  excludeInstitutions: boolean;
  maxReviewRecords: number;
  maxMatchReadyRecords: number;
  topBucketBForExa: number;
  exaFreshnessMaxAgeHours: number;
  debug: boolean;
  hasExaApiKey: boolean;
}

interface OutputWriteStats {
  defaultDatasetItems: number;
  reviewItems: number;
  matchReadyItems: number;
  kvRunSummaryWritten: boolean;
  kvInputSchemaWritten: boolean;
}

const RUNTIME_LOG_PREFIX = '[RUNTIME]';

function stageLog(stage: string, data?: unknown): void {
  logInfo(`${RUNTIME_LOG_PREFIX} ${stage}`, data);
}

function stageWarn(stage: string, data?: unknown): void {
  logWarn(`${RUNTIME_LOG_PREFIX} ${stage}`, data);
}

function stageError(stage: string, error: unknown): void {
  console.error(`[ERROR] ${RUNTIME_LOG_PREFIX} ${stage}`, error);
}

function pickBoolean(value: unknown, fallback: boolean): boolean {
  return typeof value === 'boolean' ? value : fallback;
}

function pickNumber(value: unknown, fallback: number): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function pickString(value: unknown, fallback: string): string {
  return typeof value === 'string' ? value : fallback;
}

function resolveInput(rawInput: InputAliasMap | null | undefined): ActorInput {
  const aliases = rawInput ?? {};
  const merged = { ...defaultInput, ...aliases };
  const runAfmMar19 = pickBoolean(aliases.runAfmMar19 ?? aliases.run_afm_mar19, defaultInput.runAfmMar19);
  const runAfmSubstantialHoldings = pickBoolean(
    aliases.runAfmSubstantialHoldings ?? aliases.run_afm_substantial_holdings ?? aliases.runSubstantialHoldings,
    defaultInput.runAfmSubstantialHoldings,
  );
  const afmMar19CsvUrl = pickString(aliases.afmMar19CsvUrl ?? aliases.afm_mar19_csv_url, defaultInput.afmMar19CsvUrl).trim();
  const afmSubstantialHoldingsCsvUrl = pickString(
    aliases.afmSubstantialHoldingsCsvUrl ?? aliases.afm_substantial_holdings_csv_url,
    defaultInput.afmSubstantialHoldingsCsvUrl,
  ).trim();
  const runExaConfirmation = pickBoolean(
    aliases.runExaConfirmation ?? aliases.runExaEnrichment ?? aliases.run_exa_confirmation ?? aliases.run_exa_enrichment,
    defaultInput.runExaConfirmation ?? defaultInput.runExaEnrichment,
  );
  const topBucketBForExa = pickNumber(
    aliases.topBucketBForExa ?? aliases.exaTopReviewConfirmations ?? aliases.top_bucket_b_for_exa ?? aliases.exa_top_review_confirmations,
    defaultInput.topBucketBForExa ?? defaultInput.exaTopReviewConfirmations,
  );
  const exaApiKey = pickString(aliases.exaApiKey ?? aliases.exa_api_key ?? merged.exaApiKey ?? process.env.EXA_API_KEY, '').trim();

  return {
    ...merged,
    runAfmMar19,
    runAfmSubstantialHoldings,
    afmMar19CsvUrl,
    afmSubstantialHoldingsCsvUrl,
    runExaConfirmation,
    runExaEnrichment: runExaConfirmation,
    topBucketBForExa,
    exaTopReviewConfirmations: topBucketBForExa,
    exaApiKey,
    lookbackDays: pickNumber(aliases.lookbackDays ?? aliases.lookback_days, defaultInput.lookbackDays),
    minSignalConfidence: pickNumber(aliases.minSignalConfidence ?? aliases.min_signal_confidence, defaultInput.minSignalConfidence),
    minNaturalPersonConfidence: pickNumber(aliases.minNaturalPersonConfidence ?? aliases.min_natural_person_confidence, defaultInput.minNaturalPersonConfidence),
    excludeInstitutions: pickBoolean(aliases.excludeInstitutions ?? aliases.exclude_institutions, defaultInput.excludeInstitutions),
    maxReviewRecords: pickNumber(aliases.maxReviewRecords ?? aliases.max_review_records, defaultInput.maxReviewRecords),
    maxMatchReadyRecords: pickNumber(aliases.maxMatchReadyRecords ?? aliases.max_match_ready_records, defaultInput.maxMatchReadyRecords),
    exaFreshnessMaxAgeHours: pickNumber(aliases.exaFreshnessMaxAgeHours ?? aliases.exa_freshness_max_age_hours, defaultInput.exaFreshnessMaxAgeHours),
    debug: pickBoolean(aliases.debug, defaultInput.debug),
  };
}

function toRuntimeConfig(input: ActorInput): NormalizedRuntimeConfig {
  return {
    runAfmMar19: input.runAfmMar19,
    runAfmSubstantialHoldings: input.runAfmSubstantialHoldings,
    runExaConfirmation: input.runExaConfirmation ?? false,
    afmMar19CsvUrl: input.afmMar19CsvUrl,
    afmSubstantialHoldingsCsvUrl: input.afmSubstantialHoldingsCsvUrl,
    lookbackDays: input.lookbackDays,
    minSignalConfidence: input.minSignalConfidence,
    minNaturalPersonConfidence: input.minNaturalPersonConfidence,
    excludeInstitutions: input.excludeInstitutions,
    maxReviewRecords: input.maxReviewRecords,
    maxMatchReadyRecords: input.maxMatchReadyRecords,
    topBucketBForExa: input.topBucketBForExa ?? input.exaTopReviewConfirmations,
    exaFreshnessMaxAgeHours: input.exaFreshnessMaxAgeHours,
    debug: input.debug,
    hasExaApiKey: Boolean(input.exaApiKey),
  };
}

function getSelectedSources(input: ActorInput): string[] {
  return [
    input.runAfmMar19 ? 'afm_mar19' : null,
    input.runAfmSubstantialHoldings ? 'afm_substantial' : null,
  ].filter((value): value is string => Boolean(value));
}

export function appendRecords(target: NormalizedSignalRecord[], source: NormalizedSignalRecord[]): number {
  for (const record of source) target.push(record);
  return target.length;
}

export async function run(): Promise<void> {
  let actorInitialized = false;
  let success = false;
  let outputsWritten = false;

  try {
    await Actor.init();
    actorInitialized = true;
    stageLog('actor initialized');

    const rawInput = await Actor.getInput<InputAliasMap>();
    stageLog('input loaded', rawInput ?? {});

    const input = resolveInput(rawInput);
    const runtimeConfig = toRuntimeConfig(input);
    stageLog('normalized input resolved');
    stageLog('normalized runtime config', runtimeConfig);

    const selectedSources = getSelectedSources(input);
    stageLog('source modules selected', { selectedSources });

    if (!selectedSources.length) {
      throw new Error('No source module is enabled after input normalization.');
    }

    if (!input.afmMar19CsvUrl && !input.afmSubstantialHoldingsCsvUrl) {
      throw new Error('Both AFM source URLs are empty after normalization.');
    }

    const sourceStats = { afm_mar19: 0, afm_substantial: 0, exa_enriched: 0 };
    const outputWriteStats: OutputWriteStats = {
      defaultDatasetItems: 0,
      reviewItems: 0,
      matchReadyItems: 0,
      kvRunSummaryWritten: false,
      kvInputSchemaWritten: false,
    };
    let records: NormalizedSignalRecord[] = [];

    if (input.runAfmMar19) {
      stageLog('AFM MAR 19 fetch starting', { url: input.afmMar19CsvUrl });
      const mar19 = await ingestAfmMar19(input.afmMar19CsvUrl || defaultInput.afmMar19CsvUrl);
      sourceStats.afm_mar19 = mar19.length;
      stageLog('AFM MAR 19 rows loaded', { rows: mar19.length });
      if (!mar19.length) stageWarn('AFM MAR 19 fetch returned empty rows unexpectedly', { url: input.afmMar19CsvUrl });
      stageLog('starting merge of source records', { source: 'afm_mar19', incomingRows: mar19.length, recordsBeforeMerge: records.length });
      appendRecords(records, mar19);
      stageLog('merge completed', { source: 'afm_mar19', recordsAfterMerge: records.length });
    }

    if (input.runAfmSubstantialHoldings) {
      stageLog('AFM substantial holdings fetch starting', { url: input.afmSubstantialHoldingsCsvUrl });
      const substantial = await ingestAfmSubstantialHoldings(input.afmSubstantialHoldingsCsvUrl || defaultInput.afmSubstantialHoldingsCsvUrl);
      sourceStats.afm_substantial = substantial.length;
      stageLog('AFM substantial rows loaded', { rows: substantial.length });
      if (!substantial.length) stageWarn('AFM substantial holdings fetch returned empty rows unexpectedly', { url: input.afmSubstantialHoldingsCsvUrl });
      stageLog('starting merge of source records', { source: 'afm_substantial', incomingRows: substantial.length, recordsBeforeMerge: records.length });
      appendRecords(records, substantial);
      stageLog('merge completed', { source: 'afm_substantial', recordsAfterMerge: records.length });
    }

    stageLog('normalization started', { records: records.length });
    stageLog('normalization completed', { records: records.length });
    stageLog('dedupe started', { recordsBeforeDedupe: records.length });
    records = dedupeSignals(records);
    stageLog('dedupe completed', { recordsAfterDedupe: records.length });

    let excludedInstitutions = 0;
    let lowConfidenceRecords = 0;

    stageLog('scoring started', { recordsAfterDedupe: records.length });
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
    stageLog('scoring completed', {
      recordsScored: records.length,
      excludedInstitutions,
      lowConfidenceRecords,
      exaEnriched: sourceStats.exa_enriched,
    });

    const postFilterRecords = input.excludeInstitutions
      ? records.filter((record) => record.institutional_risk !== 'high')
      : records;

    stageLog('exports started', {
      rawRecords: records.length,
      postFilterRecords: postFilterRecords.length,
    });

    await exportRawArchive(records);
    outputWriteStats.defaultDatasetItems = records.length;

    await confirmContextForTopReviewRecords(rankReviewRecords(postFilterRecords), input);
    const review = await exportReviewDataset(postFilterRecords, input.maxReviewRecords);
    outputWriteStats.reviewItems = review.length;
    if (!review.length) stageWarn('review export wrote zero records', { maxReviewRecords: input.maxReviewRecords });
    const matchReady = await exportMatchReady(postFilterRecords, input.maxMatchReadyRecords);
    outputWriteStats.matchReadyItems = matchReady.length;
    if (!matchReady.length) stageWarn('match-ready export wrote zero records', { maxMatchReadyRecords: input.maxMatchReadyRecords });
    const review_bucket_stats = {
      A: postFilterRecords.filter((record) => record.review_bucket === 'A').length,
      B: postFilterRecords.filter((record) => record.review_bucket === 'B').length,
      C: postFilterRecords.filter((record) => record.review_bucket === 'C').length,
    };

    const summary: RunSummary = {
      raw_records: records.length,
      post_filter_records: postFilterRecords.length,
      review_records: review.length,
      match_ready_records: matchReady.length,
      excluded_institutions: excludedInstitutions,
      low_confidence_records: lowConfidenceRecords,
      source_stats: sourceStats,
      review_bucket_stats,
    };

    if (!input.exaApiKey) {
      logInfo('Exa confirmation disabled', { reason: 'No input.exaApiKey or EXA_API_KEY was provided.' });
    }

    logInfo('Run summary', summary);
    await Actor.setValue('RUN_SUMMARY', summary);
    outputWriteStats.kvRunSummaryWritten = true;
    await Actor.setValue('INPUT_SCHEMA', (await import('./inputSchema.js')).inputSchema);
    outputWriteStats.kvInputSchemaWritten = true;

    outputsWritten = outputWriteStats.defaultDatasetItems > 0
      || outputWriteStats.reviewItems > 0
      || outputWriteStats.matchReadyItems > 0
      || outputWriteStats.kvRunSummaryWritten
      || outputWriteStats.kvInputSchemaWritten;

    stageLog('outputs written', outputWriteStats);

    if (!outputsWritten) {
      stageWarn('pipeline returned without writing outputs', outputWriteStats);
      throw new Error('Pipeline returned without writing outputs.');
    }

    success = true;
    stageLog('actor exiting successfully');
  } catch (error) {
    stageError('actor failed', error);
    throw error;
  } finally {
    if (actorInitialized && !success && !outputsWritten) {
      stageWarn('pipeline returned without writing outputs', { actorInitialized, success, outputsWritten });
    }
    if (actorInitialized) await Actor.exit();
  }
}

const entryFilePath = process.argv[1];
if (entryFilePath && fileURLToPath(import.meta.url) === entryFilePath) await run();
