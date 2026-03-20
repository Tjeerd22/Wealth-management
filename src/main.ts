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
import { dedupeSignalsWithStats } from './dedupe/dedupeSignals.js';
import { exportRawArchive } from './export/exportRawArchive.js';
import { exportReviewDataset, rankReviewRecords } from './export/exportReviewDataset.js';
import { exportMatchReady } from './export/exportMatchReady.js';
import { scoreNlRelevance } from './scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from './scoring/scoreIssuerDesirability.js';
import { ActorInput, NormalizedSignalRecord, RunState, RunSummary, SourceRuntimeStatus } from './types.js';
import { logInfo, logWarn } from './utils/logging.js';
import { confirmContextForTopReviewRecords } from './enrich/confirmContextForTopReviewRecords.js';

type InputAliasMap = Partial<ActorInput> & Record<string, unknown>;

type SourceKey = 'afm_mar19' | 'afm_substantial';

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
const SUBSTANTIAL_RETRY_LIMIT = 2;
const RETRY_BASE_DELAY_MS = 500;

function stageLog(stage: string, data?: unknown): void { logInfo(`${RUNTIME_LOG_PREFIX} ${stage}`, data); }
function stageWarn(stage: string, data?: unknown): void { logWarn(`${RUNTIME_LOG_PREFIX} ${stage}`, data); }
function stageError(stage: string, error: unknown): void { console.error(`[ERROR] ${RUNTIME_LOG_PREFIX} ${stage}`, error); }
const pickBoolean = (value: unknown, fallback: boolean): boolean => typeof value === 'boolean' ? value : fallback;
const pickNumber = (value: unknown, fallback: number): number => typeof value === 'number' && Number.isFinite(value) ? value : fallback;
const pickString = (value: unknown, fallback: string): string => typeof value === 'string' ? value : fallback;

function getElapsedMs(startedAt: number): number { return Date.now() - startedAt; }
function sleep(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
function getRetryDelayMs(attempt: number): number { return RETRY_BASE_DELAY_MS * (2 ** (attempt - 1)) + Math.floor(Math.random() * 200); }

function extractHttpStatus(error: unknown): number | undefined {
  if (!(error instanceof Error)) return undefined;
  const match = error.message.match(/(?:status|HTTP|fetch CSV from .*:)\s*:?(\d{3})/i) ?? error.message.match(/(\d{3})/);
  return match ? Number(match[1]) : undefined;
}

function isRetryableSubstantialFailure(error: unknown): boolean {
  const status = extractHttpStatus(error);
  return status === 504 || (typeof status === 'number' && status >= 500) || String(error).toLowerCase().includes('timeout');
}

function resolveInput(rawInput: InputAliasMap | null | undefined): ActorInput {
  const aliases = rawInput ?? {};
  const merged = { ...defaultInput, ...aliases };
  const runAfmMar19 = pickBoolean(aliases.runAfmMar19 ?? aliases.run_afm_mar19, defaultInput.runAfmMar19);
  const runAfmSubstantialHoldings = pickBoolean(aliases.runAfmSubstantialHoldings ?? aliases.run_afm_substantial_holdings ?? aliases.runSubstantialHoldings, defaultInput.runAfmSubstantialHoldings);
  const afmMar19CsvUrl = pickString(aliases.afmMar19CsvUrl ?? aliases.afm_mar19_csv_url, defaultInput.afmMar19CsvUrl).trim();
  const afmSubstantialHoldingsCsvUrl = pickString(aliases.afmSubstantialHoldingsCsvUrl ?? aliases.afm_substantial_holdings_csv_url, defaultInput.afmSubstantialHoldingsCsvUrl).trim();
  const runExaConfirmation = pickBoolean(aliases.runExaConfirmation ?? aliases.runExaEnrichment ?? aliases.run_exa_confirmation ?? aliases.run_exa_enrichment, defaultInput.runExaConfirmation ?? defaultInput.runExaEnrichment);
  const topBucketBForExa = pickNumber(aliases.topBucketBForExa ?? aliases.exaTopReviewConfirmations ?? aliases.top_bucket_b_for_exa ?? aliases.exa_top_review_confirmations, defaultInput.topBucketBForExa ?? defaultInput.exaTopReviewConfirmations);
  const exaApiKey = pickString(aliases.exaApiKey ?? aliases.exa_api_key ?? merged.exaApiKey ?? process.env.EXA_API_KEY, '').trim();
  return { ...merged, runAfmMar19, runAfmSubstantialHoldings, afmMar19CsvUrl, afmSubstantialHoldingsCsvUrl, runExaConfirmation, runExaEnrichment: runExaConfirmation, topBucketBForExa, exaTopReviewConfirmations: topBucketBForExa, exaApiKey, lookbackDays: pickNumber(aliases.lookbackDays ?? aliases.lookback_days, defaultInput.lookbackDays), minSignalConfidence: pickNumber(aliases.minSignalConfidence ?? aliases.min_signal_confidence, defaultInput.minSignalConfidence), minNaturalPersonConfidence: pickNumber(aliases.minNaturalPersonConfidence ?? aliases.min_natural_person_confidence, defaultInput.minNaturalPersonConfidence), excludeInstitutions: pickBoolean(aliases.excludeInstitutions ?? aliases.exclude_institutions, defaultInput.excludeInstitutions), maxReviewRecords: pickNumber(aliases.maxReviewRecords ?? aliases.max_review_records, defaultInput.maxReviewRecords), maxMatchReadyRecords: pickNumber(aliases.maxMatchReadyRecords ?? aliases.max_match_ready_records, defaultInput.maxMatchReadyRecords), exaFreshnessMaxAgeHours: pickNumber(aliases.exaFreshnessMaxAgeHours ?? aliases.exa_freshness_max_age_hours, defaultInput.exaFreshnessMaxAgeHours), debug: pickBoolean(aliases.debug, defaultInput.debug) };
}

function toRuntimeConfig(input: ActorInput): NormalizedRuntimeConfig {
  return { runAfmMar19: input.runAfmMar19, runAfmSubstantialHoldings: input.runAfmSubstantialHoldings, runExaConfirmation: input.runExaConfirmation ?? false, afmMar19CsvUrl: input.afmMar19CsvUrl, afmSubstantialHoldingsCsvUrl: input.afmSubstantialHoldingsCsvUrl, lookbackDays: input.lookbackDays, minSignalConfidence: input.minSignalConfidence, minNaturalPersonConfidence: input.minNaturalPersonConfidence, excludeInstitutions: input.excludeInstitutions, maxReviewRecords: input.maxReviewRecords, maxMatchReadyRecords: input.maxMatchReadyRecords, topBucketBForExa: input.topBucketBForExa ?? input.exaTopReviewConfirmations, exaFreshnessMaxAgeHours: input.exaFreshnessMaxAgeHours, debug: input.debug, hasExaApiKey: Boolean(input.exaApiKey) };
}

function getSelectedSources(input: ActorInput): string[] {
  return [input.runAfmMar19 ? 'afm_mar19' : null, input.runAfmSubstantialHoldings ? 'afm_substantial' : null].filter((value): value is string => Boolean(value));
}

export function appendRecords(target: NormalizedSignalRecord[], source: NormalizedSignalRecord[]): number {
  for (const record of source) target.push(record);
  return target.length;
}

export async function loadSourceWithPolicy(sourceKey: SourceKey, url: string, overrideIngest?: (url: string) => Promise<NormalizedSignalRecord[]>): Promise<{ records: NormalizedSignalRecord[]; status: SourceRuntimeStatus; degraded: boolean }> {
  const startedAt = Date.now();
  const status: SourceRuntimeStatus = { status: 'failed', row_count: 0, retries: 0, elapsed_ms: 0 };
  const ingest = overrideIngest ?? (sourceKey === 'afm_mar19' ? ingestAfmMar19 : ingestAfmSubstantialHoldings);
  const sourceLabel = sourceKey === 'afm_mar19' ? 'AFM MAR 19' : 'AFM substantial holdings';

  for (let attempt = 0; attempt <= (sourceKey === 'afm_substantial' ? SUBSTANTIAL_RETRY_LIMIT : 0); attempt += 1) {
    try {
      stageLog('source fetch started', { source: sourceKey, url, attempt: attempt + 1 });
      const records = await ingest(url);
      status.status = 'succeeded';
      status.row_count = records.length;
      status.elapsed_ms = getElapsedMs(startedAt);
      status.retries = attempt;
      stageLog('source fetch completed', { source: sourceKey, rows: records.length, attempt: attempt + 1, elapsedMs: status.elapsed_ms });
      stageLog('source parse completed', { source: sourceKey, rows: records.length });
      return { records, status, degraded: false };
    } catch (error) {
      status.http_status = extractHttpStatus(error);
      status.error_message = error instanceof Error ? error.message : String(error);
      status.retries = attempt;
      status.elapsed_ms = getElapsedMs(startedAt);
      if (sourceKey === 'afm_substantial' && attempt < SUBSTANTIAL_RETRY_LIMIT && isRetryableSubstantialFailure(error)) {
        const delayMs = getRetryDelayMs(attempt + 1);
        stageWarn('source fetch retry scheduled', { source: sourceKey, attempt: attempt + 1, delayMs, error: status.error_message, httpStatus: status.http_status });
        await sleep(delayMs);
        continue;
      }
      if (sourceKey === 'afm_substantial' && isRetryableSubstantialFailure(error)) {
        status.status = 'degraded';
        stageWarn('source entered degraded mode', { source: sourceKey, retries: status.retries, elapsedMs: status.elapsed_ms, error: status.error_message, httpStatus: status.http_status });
        return { records: [], status, degraded: true };
      }
      status.status = 'failed';
      throw Object.assign(new Error(`${sourceLabel} failed: ${status.error_message}`), { cause: error, sourceStatus: status });
    }
  }

  throw new Error(`${sourceLabel} exhausted retry policy unexpectedly.`);
}

export async function run(): Promise<void> {
  let actorInitialized = false;
  let runState: RunState = 'failed';
  const outputWriteStats: OutputWriteStats = { defaultDatasetItems: 0, reviewItems: 0, matchReadyItems: 0, kvRunSummaryWritten: false, kvInputSchemaWritten: false };
  const sourceStatus: Record<string, SourceRuntimeStatus> = {
    afm_mar19: { status: 'skipped', row_count: 0, retries: 0, elapsed_ms: 0 },
    afm_substantial: { status: 'skipped', row_count: 0, retries: 0, elapsed_ms: 0 },
  };

  try {
    await Actor.init();
    actorInitialized = true;
    stageLog('actor initialized');
    const rawInput = await Actor.getInput<InputAliasMap>();
    stageLog('input loaded', rawInput ?? {});
    const input = resolveInput(rawInput);
    const runtimeConfig = toRuntimeConfig(input);
    stageLog('normalized runtime config', runtimeConfig);
    const selectedSources = getSelectedSources(input);
    stageLog('source selected', { selectedSources });
    if (!selectedSources.length) throw new Error('No source module is enabled after input normalization.');
    if (!input.afmMar19CsvUrl && !input.afmSubstantialHoldingsCsvUrl) throw new Error('Both AFM source URLs are empty after normalization.');

    const sourceStats = { afm_mar19: 0, afm_substantial: 0, exa_enriched: 0 };
    let records: NormalizedSignalRecord[] = [];
    let degradedRun = false;

    if (input.runAfmMar19) {
      const result = await loadSourceWithPolicy('afm_mar19', input.afmMar19CsvUrl || defaultInput.afmMar19CsvUrl);
      sourceStatus.afm_mar19 = result.status;
      sourceStats.afm_mar19 = result.records.length;
      appendRecords(records, result.records);
    }

    if (input.runAfmSubstantialHoldings) {
      const result = await loadSourceWithPolicy('afm_substantial', input.afmSubstantialHoldingsCsvUrl || defaultInput.afmSubstantialHoldingsCsvUrl);
      sourceStatus.afm_substantial = result.status;
      sourceStats.afm_substantial = result.records.length;
      degradedRun ||= result.degraded;
      appendRecords(records, result.records);
    }

    if (!records.length) throw new Error('No valid source records available after source loading.');
    if (sourceStatus.afm_mar19.status === 'failed' || (input.runAfmMar19 && sourceStatus.afm_mar19.row_count === 0 && sourceStatus.afm_substantial.status !== 'succeeded')) {
      throw new Error('AFM MAR 19 failed or produced no usable records; run cannot continue.');
    }
    if (input.runAfmMar19 && input.runAfmSubstantialHoldings && sourceStatus.afm_mar19.status !== 'succeeded' && sourceStatus.afm_substantial.status !== 'succeeded') {
      throw new Error('Both AFM sources failed.');
    }

    stageLog('dedupe started', { recordsBeforeDedupe: records.length });
    const dedupeResult = dedupeSignalsWithStats(records);
    records = dedupeResult.records;
    stageLog('dedupe completed', { recordsAfterDedupe: records.length, mergesPerformed: dedupeResult.stats.mergesPerformed, topMergeReasons: dedupeResult.stats.topMergeReasons, suspiciousGroups: dedupeResult.stats.suspiciousGroups });

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
    stageLog('scoring completed', { recordsScored: records.length, excludedInstitutions, lowConfidenceRecords, exaEnriched: sourceStats.exa_enriched });

    const postFilterRecords = input.excludeInstitutions ? records.filter((record) => record.institutional_risk !== 'high') : records;
    stageLog('exports started', { rawRecords: records.length, postFilterRecords: postFilterRecords.length });
    const rawArchiveStats = await exportRawArchive(records);
    outputWriteStats.defaultDatasetItems = rawArchiveStats.itemsWritten;
    const rankedReview = rankReviewRecords(postFilterRecords);
    await confirmContextForTopReviewRecords(rankedReview, input);
    const review = await exportReviewDataset(postFilterRecords, input.maxReviewRecords);
    outputWriteStats.reviewItems = review.length;
    const matchReady = await exportMatchReady(postFilterRecords, input.maxMatchReadyRecords);
    outputWriteStats.matchReadyItems = matchReady.length;
    const review_bucket_stats = { A: postFilterRecords.filter((record) => record.review_bucket === 'A').length, B: postFilterRecords.filter((record) => record.review_bucket === 'B').length, C: postFilterRecords.filter((record) => record.review_bucket === 'C').length };

    runState = degradedRun ? 'degraded' : 'succeeded';
    const summary: RunSummary = {
      run_state: runState,
      degraded_run: degradedRun,
      raw_records: records.length,
      post_filter_records: postFilterRecords.length,
      review_records: review.length,
      match_ready_records: matchReady.length,
      excluded_institutions: excludedInstitutions,
      low_confidence_records: lowConfidenceRecords,
      source_stats: sourceStats,
      source_status: sourceStatus,
      review_bucket_stats,
      outputs_written: {
        default_dataset_items: outputWriteStats.defaultDatasetItems,
        review_items: outputWriteStats.reviewItems,
        match_ready_items: outputWriteStats.matchReadyItems,
        kv_run_summary_written: outputWriteStats.kvRunSummaryWritten,
        kv_input_schema_written: outputWriteStats.kvInputSchemaWritten,
      },
    };

    if (!input.exaApiKey) logInfo('Exa confirmation disabled', { reason: 'No input.exaApiKey or EXA_API_KEY was provided.' });
    await Actor.setValue('RUN_SUMMARY', summary);
    outputWriteStats.kvRunSummaryWritten = true;
    await Actor.setValue('INPUT_SCHEMA', (await import('./inputSchema.js')).inputSchema);
    outputWriteStats.kvInputSchemaWritten = true;
    stageLog('outputs written', outputWriteStats);
    stageLog('final run state', summary);

    if (!(outputWriteStats.defaultDatasetItems || outputWriteStats.reviewItems || outputWriteStats.matchReadyItems || outputWriteStats.kvRunSummaryWritten || outputWriteStats.kvInputSchemaWritten)) {
      throw new Error('Pipeline returned without writing outputs.');
    }
  } catch (error) {
    runState = 'failed';
    stageError('actor failed', error);
    throw error;
  } finally {
    if (actorInitialized) {
      stageLog('final run state', { runState, outputWriteStats, sourceStatus });
      await Actor.exit();
    }
  }
}

const entryFilePath = process.argv[1];
if (entryFilePath && fileURLToPath(import.meta.url) === entryFilePath) await run();
