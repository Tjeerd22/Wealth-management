import { Actor } from 'apify';
import { fileURLToPath } from 'node:url';
import { defaultInput } from './config.js';
import { ingestAfmMar19Html } from './sources/afmMar19Html.js';
import { applyInstitutionalFilter } from './filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from './filters/personConfidence.js';
import { enrichRecord } from './enrich/enrichRecord.js';
import { scoreSignal } from './scoring/scoreSignal.js';
import { applySignalGates } from './filters/signalGates.js';
import { exportRawArchive } from './export/exportRawArchive.js';
import { exportReviewDataset, rankReviewRecords } from './export/exportReviewDataset.js';
import { exportShortlistDataset } from './export/exportShortlistDataset.js';
import { scoreNlRelevance } from './scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from './scoring/scoreIssuerDesirability.js';
import { ActorInput, FinalRunState, NormalizedSignalRecord, RunSummary, SourceFetchStatus } from './types.js';
import { logInfo, logWarn } from './utils/logging.js';
import { confirmContextForTopReviewRecords } from './enrich/confirmContextForTopReviewRecords.js';
import { toAfmDateParam } from './utils/dates.js';

type InputAliasMap = Partial<ActorInput> & Record<string, unknown>;

const RUNTIME_LOG_PREFIX = '[RUNTIME]';

function stageLog(stage: string, data?: unknown): void { logInfo(`${RUNTIME_LOG_PREFIX} ${stage}`, data); }
function stageWarn(stage: string, data?: unknown): void { logWarn(`${RUNTIME_LOG_PREFIX} ${stage}`, data); }
function stageError(stage: string, error: unknown): void { console.error(`[ERROR] ${RUNTIME_LOG_PREFIX} ${stage}`, error); }
const pickBoolean = (value: unknown, fallback: boolean): boolean => typeof value === 'boolean' ? value : fallback;
const pickNumber = (value: unknown, fallback: number): number => typeof value === 'number' && Number.isFinite(value) ? value : fallback;
const pickString = (value: unknown, fallback: string): string => typeof value === 'string' ? value : fallback;

function resolveInput(rawInput: InputAliasMap | null | undefined): ActorInput {
  const aliases = rawInput ?? {};
  const exaApiKey = pickString(aliases.exaApiKey ?? aliases.exa_api_key ?? process.env.EXA_API_KEY, '').trim();
  return {
    dateFrom: pickString(aliases.dateFrom ?? aliases.date_from, defaultInput.dateFrom),
    maxPages: pickNumber(aliases.maxPages ?? aliases.max_pages, defaultInput.maxPages),
    runExaConfirmation: pickBoolean(aliases.runExaConfirmation ?? aliases.run_exa_confirmation, defaultInput.runExaConfirmation),
    minSignalConfidence: pickNumber(aliases.minSignalConfidence ?? aliases.min_signal_confidence, defaultInput.minSignalConfidence),
    minNaturalPersonConfidence: pickNumber(aliases.minNaturalPersonConfidence ?? aliases.min_natural_person_confidence, defaultInput.minNaturalPersonConfidence),
    minReviewPriorityScore: pickNumber(aliases.minReviewPriorityScore ?? aliases.min_review_priority_score, defaultInput.minReviewPriorityScore),
    excludeInstitutions: pickBoolean(aliases.excludeInstitutions ?? aliases.exclude_institutions, defaultInput.excludeInstitutions),
    maxReviewRecords: pickNumber(aliases.maxReviewRecords ?? aliases.max_review_records, defaultInput.maxReviewRecords),
    maxShortlistRecords: pickNumber(aliases.maxShortlistRecords ?? aliases.max_shortlist_records, defaultInput.maxShortlistRecords),
    topBucketBForExa: pickNumber(aliases.topBucketBForExa ?? aliases.top_bucket_b_for_exa, defaultInput.topBucketBForExa),
    exaApiKey,
    exaFreshnessMaxAgeHours: pickNumber(aliases.exaFreshnessMaxAgeHours ?? aliases.exa_freshness_max_age_hours, defaultInput.exaFreshnessMaxAgeHours),
    debug: pickBoolean(aliases.debug, defaultInput.debug),
  };
}

function computeDateFrom(input: ActorInput): string {
  if (input.dateFrom) return input.dateFrom;
  // Default: 30 days ago in dd-mm-yyyy format
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  return toAfmDateParam(thirtyDaysAgo);
}

function computeEffectiveLookbackDays(dateFrom: string): number {
  // Parse dd-mm-yyyy
  const parts = dateFrom.split('-');
  if (parts.length === 3) {
    const [dd, mm, yyyy] = parts;
    const date = new Date(`${yyyy}-${mm}-${dd}`);
    if (!isNaN(date.getTime())) {
      return Math.max(1, Math.ceil((Date.now() - date.getTime()) / (24 * 60 * 60 * 1000)));
    }
  }
  return 45; // fallback
}

export async function run(): Promise<void> {
  let actorInitialized = false;
  let finalRunState: FinalRunState = 'failed';

  const sourceStatus: RunSummary['source_status'] = {
    afm_mar19_html: { enabled: true, status: 'failed', row_count: 0, pages_fetched: 0, elapsed_ms: 0, retries_attempted: 0 } as SourceFetchStatus,
  };

  try {
    await Actor.init();
    actorInitialized = true;
    stageLog('actor initialized');
    const rawInput = await Actor.getInput<InputAliasMap>();
    stageLog('input loaded', rawInput ?? {});
    const input = resolveInput(rawInput);
    stageLog('normalized input', { ...input, exaApiKey: input.exaApiKey ? '[REDACTED]' : '' });

    const dateFrom = computeDateFrom(input);
    const lookbackDays = computeEffectiveLookbackDays(dateFrom);
    stageLog('date parameters', { dateFrom, lookbackDays });

    const sourceStats = { afm_mar19_html: 0, exa_enriched: 0 };

    // --- HTML scrape ---
    stageLog('AFM MAR 19 HTML scrape starting', { dateFrom, maxPages: input.maxPages });
    const t0 = Date.now();
    let records: NormalizedSignalRecord[];
    try {
      records = await ingestAfmMar19Html({ dateFrom, maxPages: input.maxPages, debug: input.debug });
      sourceStatus.afm_mar19_html.status = 'ok';
      sourceStatus.afm_mar19_html.row_count = records.length;
      sourceStatus.afm_mar19_html.elapsed_ms = Date.now() - t0;
      sourceStats.afm_mar19_html = records.length;
      stageLog('AFM MAR 19 HTML rows loaded', { rows: records.length });
      if (!records.length) stageWarn('AFM MAR 19 HTML scrape returned zero rows', { dateFrom });
    } catch (error) {
      sourceStatus.afm_mar19_html.status = 'failed';
      sourceStatus.afm_mar19_html.elapsed_ms = Date.now() - t0;
      sourceStatus.afm_mar19_html.error = error instanceof Error ? error.message : String(error);
      stageError('AFM MAR 19 HTML scrape failed — run is fatal', error);
      throw error;
    }

    let excludedInstitutions = 0;
    let lowConfidenceRecords = 0;
    stageLog('scoring started', { recordCount: records.length });

    // Pass 1: fast synchronous pre-scoring (no I/O).
    for (const record of records) {
      applyInstitutionalFilter(record);
      record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    }

    // Pass 2: parallel Exa per-record enrichment (only when explicitly enabled).
    if (input.runExaConfirmation && input.exaApiKey) {
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

    // Pass 3: complete scoring.
    for (const record of records) {
      if (record.enrichment_context) sourceStats.exa_enriched += 1;
      record.nl_relevance_score = scoreNlRelevance(record);
      record.issuer_desirability_score = scoreIssuerDesirability(record);
      scoreSignal(record, lookbackDays);
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

    // --- Shortlist export (default dataset) ---
    const shortlist = await exportShortlistDataset(postFilterRecords, input.maxShortlistRecords);
    stageLog('shortlist export completed', { shortlistItems: shortlist.length });

    const review_bucket_stats = {
      A: postFilterRecords.filter((record) => record.review_bucket === 'A').length,
      B: postFilterRecords.filter((record) => record.review_bucket === 'B').length,
      C: postFilterRecords.filter((record) => record.review_bucket === 'C').length,
    };

    const outputsWritten = {
      raw_archive_items: rawArchiveStats.itemsWritten,
      review_items: review.length,
      shortlist_items: shortlist.length,
      kv_run_summary: false,
      kv_input_schema: false,
    };

    if (rawArchiveStats.itemsWritten === 0) {
      stageWarn('raw archive wrote zero items — pipeline produced no records', {
        postFilterRecords: postFilterRecords.length,
      });
    }

    finalRunState = 'succeeded';

    if (!input.exaApiKey) {
      logInfo('Exa confirmation disabled', { reason: 'No input.exaApiKey or EXA_API_KEY was provided.' });
    }

    const summary: RunSummary = {
      final_run_state: finalRunState,
      source_status: sourceStatus,
      raw_records: records.length,
      post_filter_records: postFilterRecords.length,
      review_records: review.length,
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

    if (actorInitialized) {
      try {
        const failureSummary: Partial<RunSummary> = {
          final_run_state: 'failed',
          source_status: sourceStatus,
        };
        await Actor.setValue('RUN_SUMMARY', failureSummary);
      } catch {
        // Best-effort only.
      }
    }

    throw error;
  } finally {
    if (actorInitialized) {
      if (finalRunState !== 'failed') {
        await Actor.exit();
      }
    }
  }
}

const entryFilePath = process.argv[1];
if (entryFilePath && fileURLToPath(import.meta.url) === entryFilePath) await run();
