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
import { Dataset } from 'apify';
import { exportRawArchive } from './export/exportRawArchive.js';
import { exportReviewDataset, rankReviewRecords, toReviewRecord } from './export/exportReviewDataset.js';
import { exportMatchReady } from './export/exportMatchReady.js';
import { scoreNlRelevance } from './scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from './scoring/scoreIssuerDesirability.js';
import { ActorInput, FinalRunState, NormalizedSignalRecord, RunSummary, SourceFetchStatus } from './types.js';
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
  maxShortlistRecords: number;
  topBucketBForExa: number;
  exaFreshnessMaxAgeHours: number;
  debug: boolean;
  hasExaApiKey: boolean;
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
  return { ...merged, runAfmMar19, runAfmSubstantialHoldings, afmMar19CsvUrl, afmSubstantialHoldingsCsvUrl, runExaConfirmation, runExaEnrichment: runExaConfirmation, topBucketBForExa, exaTopReviewConfirmations: topBucketBForExa, exaApiKey, lookbackDays: pickNumber(aliases.lookbackDays ?? aliases.lookback_days, defaultInput.lookbackDays), minSignalConfidence: pickNumber(aliases.minSignalConfidence ?? aliases.min_signal_confidence, defaultInput.minSignalConfidence), minNaturalPersonConfidence: pickNumber(aliases.minNaturalPersonConfidence ?? aliases.min_natural_person_confidence, defaultInput.minNaturalPersonConfidence), excludeInstitutions: pickBoolean(aliases.excludeInstitutions ?? aliases.exclude_institutions, defaultInput.excludeInstitutions), maxReviewRecords: pickNumber(aliases.maxReviewRecords ?? aliases.max_review_records, defaultInput.maxReviewRecords), maxMatchReadyRecords: pickNumber(aliases.maxMatchReadyRecords ?? aliases.max_match_ready_records, defaultInput.maxMatchReadyRecords), maxShortlistRecords: pickNumber(aliases.maxShortlistRecords ?? aliases.max_shortlist_records, defaultInput.maxShortlistRecords), exaFreshnessMaxAgeHours: pickNumber(aliases.exaFreshnessMaxAgeHours ?? aliases.exa_freshness_max_age_hours, defaultInput.exaFreshnessMaxAgeHours), debug: pickBoolean(aliases.debug, defaultInput.debug) };
}

function toRuntimeConfig(input: ActorInput): NormalizedRuntimeConfig {
  return { runAfmMar19: input.runAfmMar19, runAfmSubstantialHoldings: input.runAfmSubstantialHoldings, runExaConfirmation: input.runExaConfirmation ?? false, afmMar19CsvUrl: input.afmMar19CsvUrl, afmSubstantialHoldingsCsvUrl: input.afmSubstantialHoldingsCsvUrl, lookbackDays: input.lookbackDays, minSignalConfidence: input.minSignalConfidence, minNaturalPersonConfidence: input.minNaturalPersonConfidence, excludeInstitutions: input.excludeInstitutions, maxReviewRecords: input.maxReviewRecords, maxMatchReadyRecords: input.maxMatchReadyRecords, maxShortlistRecords: input.maxShortlistRecords, topBucketBForExa: input.topBucketBForExa ?? input.exaTopReviewConfirmations, exaFreshnessMaxAgeHours: input.exaFreshnessMaxAgeHours, debug: input.debug, hasExaApiKey: Boolean(input.exaApiKey) };
}

function getSelectedSources(input: ActorInput): string[] {
  return [input.runAfmMar19 ? 'afm_mar19' : null, input.runAfmSubstantialHoldings ? 'afm_substantial' : null].filter((value): value is string => Boolean(value));
}

export function appendRecords(target: NormalizedSignalRecord[], source: NormalizedSignalRecord[]): number {
  for (const record of source) target.push(record);
  return target.length;
}

function makeEmptySourceStatus(enabled: boolean): SourceFetchStatus {
  return { enabled, status: enabled ? 'failed' : 'skipped', row_count: 0, retries_attempted: 0, elapsed_ms: 0 };
}

export async function loadSourceWithPolicy(
  sourceKey: SourceKey,
  url: string,
  loader: () => Promise<NormalizedSignalRecord[]>,
): Promise<{ degraded: boolean; status: { status: SourceFetchStatus['status']; retries: number; elapsed_ms: number; error?: string; row_count: number } }> {
  const startedAt = Date.now();
  let retries = 0;

  while (true) {
    try {
      const records = await loader();
      return {
        degraded: false,
        status: {
          status: 'ok',
          retries,
          elapsed_ms: getElapsedMs(startedAt),
          row_count: records.length,
        },
      };
    } catch (error) {
      if (sourceKey === 'afm_substantial' && retries < SUBSTANTIAL_RETRY_LIMIT && isRetryableSubstantialFailure(error)) {
        retries += 1;
        await sleep(getRetryDelayMs(retries));
        continue;
      }

      if (sourceKey === 'afm_substantial') {
        return {
          degraded: true,
          status: {
            status: 'degraded',
            retries,
            elapsed_ms: getElapsedMs(startedAt),
            row_count: 0,
            error: error instanceof Error ? error.message : String(error),
          },
        };
      }

      throw new Error(`AFM MAR 19 failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}

export async function run(): Promise<void> {
  let actorInitialized = false;
  let finalRunState: FinalRunState = 'failed';

  const sourceStatus: RunSummary['source_status'] = {
    afm_mar19: makeEmptySourceStatus(false),
    afm_substantial: makeEmptySourceStatus(false),
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
    // Log Exa key availability without logging the key value.
    stageLog('exa key status', {
      exaKeyAvailable: runtimeConfig.hasExaApiKey,
      runExaConfirmation: runtimeConfig.runExaConfirmation,
      source: runtimeConfig.hasExaApiKey ? (input.exaApiKey ? 'actor_input' : 'env_EXA_API_KEY') : 'none',
    });
    const selectedSources = getSelectedSources(input);
    stageLog('source selected', { selectedSources });
    if (!selectedSources.length) throw new Error('No source module is enabled after input normalization.');
    if (!input.afmMar19CsvUrl && !input.afmSubstantialHoldingsCsvUrl) throw new Error('Both AFM source URLs are empty after normalization.');

    sourceStatus.afm_mar19 = makeEmptySourceStatus(input.runAfmMar19);
    sourceStatus.afm_substantial = makeEmptySourceStatus(input.runAfmSubstantialHoldings);

    const sourceStats = { afm_mar19: 0, afm_substantial: 0, exa_enriched: 0 };
    let records: NormalizedSignalRecord[] = [];
    let degradedRun = false;

    // --- MAR 19 fetch ---
    // Policy: MAR 19 failure is fatal. It is the primary source.
    if (input.runAfmMar19) {
      stageLog('AFM MAR 19 fetch starting', { url: input.afmMar19CsvUrl });
      const t0 = Date.now();
      try {
        const mar19 = await ingestAfmMar19(input.afmMar19CsvUrl || defaultInput.afmMar19CsvUrl);
        sourceStatus.afm_mar19.status = 'ok';
        sourceStatus.afm_mar19.row_count = mar19.length;
        sourceStatus.afm_mar19.elapsed_ms = Date.now() - t0;
        sourceStats.afm_mar19 = mar19.length;
        stageLog('AFM MAR 19 rows loaded', { rows: mar19.length });
        if (!mar19.length) stageWarn('AFM MAR 19 fetch returned empty rows unexpectedly', { url: input.afmMar19CsvUrl });
        appendRecords(records, mar19);
      } catch (error) {
        sourceStatus.afm_mar19.status = 'failed';
        sourceStatus.afm_mar19.elapsed_ms = Date.now() - t0;
        sourceStatus.afm_mar19.error = error instanceof Error ? error.message : String(error);
        stageError('AFM MAR 19 fetch failed — run is fatal', error);
        // MAR 19 failure is not recoverable as degraded. Both-failed is also fatal.
        throw error;
      }
    }

    // --- Substantial holdings fetch ---
    // Policy: failure after retries enters degraded mode if MAR 19 succeeded.
    if (input.runAfmSubstantialHoldings) {
      stageLog('AFM substantial holdings fetch starting', { url: input.afmSubstantialHoldingsCsvUrl });
      const t0 = Date.now();
      try {
        const substantial = await ingestAfmSubstantialHoldings(
          input.afmSubstantialHoldingsCsvUrl || defaultInput.afmSubstantialHoldingsCsvUrl,
          input.lookbackDays,
        );
        sourceStatus.afm_substantial.status = 'ok';
        sourceStatus.afm_substantial.row_count = substantial.length;
        sourceStatus.afm_substantial.elapsed_ms = Date.now() - t0;
        sourceStats.afm_substantial = substantial.length;
        stageLog('AFM substantial rows loaded', { rows: substantial.length });
        if (!substantial.length) stageWarn('AFM substantial holdings fetch returned empty rows', { url: input.afmSubstantialHoldingsCsvUrl });
        appendRecords(records, substantial);
      } catch (error) {
        sourceStatus.afm_substantial.status = 'degraded';
        sourceStatus.afm_substantial.elapsed_ms = Date.now() - t0;
        sourceStatus.afm_substantial.error = error instanceof Error ? error.message : String(error);

        // If MAR 19 is disabled (not run) and substantial fails, no source succeeded — fail.
        if (!input.runAfmMar19) {
          stageError('AFM substantial holdings failed and MAR 19 is disabled — run is fatal', error);
          throw error;
        }

        // MAR 19 succeeded: enter degraded mode and continue.
        degradedRun = true;
        stageWarn('AFM substantial holdings fetch failed after retries — entering degraded mode (MAR 19 only)', {
          error: sourceStatus.afm_substantial.error,
          degradedRun: true,
        });
      }
    }

    stageLog('dedupe started', { recordsBeforeDedupe: records.length });
    const dedupeResult = dedupeSignalsWithStats(records);
    records = dedupeResult.records;
    stageLog('dedupe completed', { recordsAfterDedupe: records.length, mergesPerformed: dedupeResult.stats.mergesPerformed, topMergeReasons: dedupeResult.stats.topMergeReasons, suspiciousGroups: dedupeResult.stats.suspiciousGroups });

    let excludedInstitutions = 0;
    let lowConfidenceRecords = 0;
    stageLog('scoring started', { recordsAfterDedupe: records.length });

    // Pass 1: fast synchronous pre-scoring (no I/O).
    // Sets institutional_risk and natural_person_confidence so Exa delta can add to the correct base.
    for (const record of records) {
      applyInstitutionalFilter(record);
      record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    }

    // Pass 2: parallel Exa per-record enrichment (only when explicitly enabled).
    // Serial enrichment of 50–100 records causes actor timeout in Apify.
    // Batches of 8 keep Exa load predictable while cutting wall-clock time substantially.
    if (input.runExaEnrichment && input.exaApiKey) {
      const EXA_ENRICH_CONCURRENCY = 8;
      for (let i = 0; i < records.length; i += EXA_ENRICH_CONCURRENCY) {
        await Promise.allSettled(
          records.slice(i, i + EXA_ENRICH_CONCURRENCY).map(async (r) => {
            try {
              await enrichRecord(r, input);
            } catch (e) {
              stageWarn('Exa per-record enrichment error (non-fatal)', {
                recordId: r.record_id,
                error: e instanceof Error ? e.message : String(e),
              });
            }
          }),
        );
      }
    }

    // Pass 3: complete scoring (uses enrichment_context / role data set by Exa in pass 2).
    for (const record of records) {
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
    stageLog('raw archive export completed', rawArchiveStats);

    await confirmContextForTopReviewRecords(rankReviewRecords(postFilterRecords), input);
    const review = await exportReviewDataset(postFilterRecords, input.maxReviewRecords);
    if (!review.length) stageWarn('review export wrote zero records', { maxReviewRecords: input.maxReviewRecords });
    const matchReady = await exportMatchReady(postFilterRecords, input.maxMatchReadyRecords);
    if (!matchReady.length) stageWarn('match-ready export wrote zero records — likely all records blocked by signal gates', {

      maxMatchReadyRecords: input.maxMatchReadyRecords,
      runExaConfirmation: runtimeConfig.runExaConfirmation,
      hasExaApiKey: runtimeConfig.hasExaApiKey,
      hint: runtimeConfig.hasExaApiKey ? undefined : 'match_ready requires enrichment_context or role; enable Exa or ensure source provides role data',
    });

    // --- Shortlist export ---
    // Exportable without Exa. Softer gate than match-ready: requires A/B bucket,
    // natural_person_confidence >= 0.45, signal_confidence >= 0.40, and either a
    // primary source or a secondary source corroborated by a MAR 19 filing.
    // Records are already scored by rankReviewRecords (called inside exportReviewDataset).
    const shortlistRecords = postFilterRecords
      .filter((r) => r.shortlist_eligible)
      .sort((a, b) => b.review_priority_score - a.review_priority_score)
      .slice(0, input.maxShortlistRecords);
    const shortlistDataset = await Dataset.open('shortlist');
    if (shortlistRecords.length) await shortlistDataset.pushData(shortlistRecords.map(toReviewRecord));
    stageLog('shortlist export completed', { shortlistItems: shortlistRecords.length });

    const review_bucket_stats = {
      A: postFilterRecords.filter((record) => record.review_bucket === 'A').length,
      B: postFilterRecords.filter((record) => record.review_bucket === 'B').length,
      C: postFilterRecords.filter((record) => record.review_bucket === 'C').length,
    };

    const outputsWritten = {
      default_dataset_items: rawArchiveStats.itemsWritten,
      review_items: review.length,
      match_ready_items: matchReady.length,
      shortlist_items: shortlistRecords.length,
      kv_run_summary: false,
      kv_input_schema: false,
    };

    // A run with no dataset output is suspicious but not always wrong
    // (e.g. all records within lookback window were institutions).
    // Warn but do not fail — the summary will show the state truthfully.
    if (rawArchiveStats.itemsWritten === 0) {
      stageWarn('raw archive wrote zero items — pipeline produced no records', {
        degradedRun,
        postFilterRecords: postFilterRecords.length,
      });
    }

    finalRunState = degradedRun ? 'degraded' : 'succeeded';

    if (!input.exaApiKey) {
      logInfo('Exa confirmation disabled', { reason: 'No input.exaApiKey or EXA_API_KEY was provided.' });
    }

    const summary: RunSummary = {
      final_run_state: finalRunState,
      degraded_run: degradedRun,
      source_status: sourceStatus,
      raw_records: records.length,
      post_filter_records: postFilterRecords.length,
      review_records: review.length,
      match_ready_records: matchReady.length,
      excluded_institutions: excludedInstitutions,
      low_confidence_records: lowConfidenceRecords,
      source_stats: sourceStats,
      review_bucket_stats,
      outputs_written: outputsWritten,
    };

    logInfo('Run summary', summary);

    await Actor.setValue('RUN_SUMMARY', summary);
    outputsWritten.kv_run_summary = true;
    await Actor.setValue('INPUT_SCHEMA', (await import('./inputSchema.js')).inputSchema);
    outputsWritten.kv_input_schema = true;

    stageLog('outputs written', outputsWritten);
    stageLog('actor exiting successfully', { finalRunState });
  } catch (error) {
    finalRunState = 'failed';
    stageError('actor failed', error);

    // Attempt to write a failure summary so the run state is observable in KV.
    if (actorInitialized) {
      try {
        const failureSummary: Partial<RunSummary> = {
          final_run_state: 'failed',
          degraded_run: false,
          source_status: sourceStatus,
        };
        await Actor.setValue('RUN_SUMMARY', failureSummary);
      } catch {
        // Best-effort only — do not suppress the original error.
      }
    }

    throw error;
  } finally {
    if (actorInitialized) {
      if (finalRunState !== 'failed') {
        // Clean shutdown for succeeded/degraded runs.
        // For failed runs, the re-thrown error propagates from catch and Apify
        // detects the non-zero process exit from the unhandled rejection.
        await Actor.exit();
      }
    }
  }
}

const entryFilePath = process.argv[1];
if (entryFilePath && fileURLToPath(import.meta.url) === entryFilePath) await run();
