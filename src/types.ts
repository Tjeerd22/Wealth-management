export type PersonType = 'unknown' | 'natural_person' | 'legal_entity' | 'family_holding';

/**
 * Role of the source that produced this record.
 *
 * - primary: the record originated from a source that drives candidate generation
 *   (AFM MAR 19 HTML register).
 */
export type SourceRole = 'primary';

/**
 * Direction of the underlying transaction or position change.
 *
 * - buy:     person acquired or increased a stake / received securities
 * - sell:    person disposed of or reduced a stake
 * - unclear: direction cannot be determined from the available source data
 */
export type SignalDirection = 'buy' | 'sell' | 'unclear';

/**
 * How explicitly the signal data supports the inferred direction/event.
 *
 * - explicit: source data directly states the direction or event type
 * - inferred: direction is derived from context (e.g. a PDMR filing date alone)
 * - unclear:  insufficient data to classify
 */
export type SignalClarity = 'explicit' | 'inferred' | 'unclear';

/**
 * Explicit run state. Exactly one of these is reported in RUN_SUMMARY.
 *
 * - succeeded: source fetched successfully, valid outputs written.
 * - failed:    pipeline threw an unrecoverable error. No valid data outputs guaranteed.
 *              The actor exits with a non-zero exit code.
 */
export type FinalRunState = 'succeeded' | 'failed';

export interface SourceFetchStatus {
  /** Whether the source was configured to run. */
  enabled: boolean;
  /** Outcome of the fetch+parse stage for this source. */
  status: 'ok' | 'skipped' | 'failed';
  row_count: number;
  pages_fetched: number;
  elapsed_ms: number;
  http_status?: number;
  error?: string;
}
export type InstitutionalRisk = 'low' | 'medium' | 'high' | 'unknown';
export type ReviewBucket = 'A' | 'B' | 'C';
export type ReviewAction = 'manual_context_check' | 'manual_person_verify' | 'discard_low_relevance' | 'watchlist_only' | 'apollo_lookup_if_needed';
export type ConfirmationEvidenceStrength = 'none' | 'weak' | 'moderate' | 'strong';
export type BlockedReason =
  | 'missing_verified_context'
  | 'unconfirmed_disposal'
  | 'low_nl_relevance'
  | 'low_natural_person_confidence'
  | 'institutional_risk'
  | 'below_min_signal_confidence';

export interface ActorInput {
  dateFrom: string;
  maxPages: number;
  runExaConfirmation: boolean;
  minSignalConfidence: number;
  minNaturalPersonConfidence: number;
  minReviewPriorityScore: number;
  excludeInstitutions: boolean;
  maxReviewRecords: number;
  maxShortlistRecords: number;
  topBucketBForExa: number;
  exaApiKey: string;
  exaFreshnessMaxAgeHours: number;
  debug: boolean;
}

export interface SourceStats {
  afm_mar19_html: number;
  exa_enriched: number;
}

export interface ReviewBucketStats {
  A: number;
  B: number;
  C: number;
}

export interface ConfirmationSource {
  url: string;
  title: string;
  source_type: 'issuer' | 'news';
  domain: string;
  published_date?: string;
  summary?: string;
  highlights: string[];
}

export interface ConfirmationResult {
  context_confirmed: boolean;
  disposal_confirmed: boolean;
  role_confirmed: boolean;
  confirmation_urls: string[];
  confirmation_sources: ConfirmationSource[];
  confirmation_summary: string;
  confirmation_evidence_strength: ConfirmationEvidenceStrength;
  review_action_updated: ReviewAction;
}

export interface ExaConfirmationCandidate {
  url: string;
  title: string;
  published_date?: string;
  summary?: string;
  highlights: string[];
  score: number;
  text?: string;
}

export interface ExaConfirmationContent {
  url: string;
  title: string;
  published_date?: string;
  summary?: string;
  highlights: string[];
  text: string;
}

export interface RunSummary {
  final_run_state: FinalRunState;
  source_status: Record<'afm_mar19_html', SourceFetchStatus>;
  raw_records: number;
  post_filter_records: number;
  review_records: number;
  excluded_institutions: number;
  low_confidence_records: number;
  source_stats: SourceStats;
  review_bucket_stats: ReviewBucketStats;
  outputs_written: {
    raw_archive_items: number;
    review_items: number;
    shortlist_items: number;
    kv_run_summary: boolean;
    kv_input_schema: boolean;
  };
}

export interface NormalizedSignalRecord {
  record_id: string;
  person_name: string;
  person_last_name: string;
  person_type: PersonType;
  role: string;
  company_name: string;
  company_domain: string;
  company_country: string;
  signal_type: string;
  signal_date: string;
  signal_detail: string;
  signal_direction: SignalDirection;
  signal_clarity: SignalClarity;
  /** 0–1. Higher values indicate a more likely liquid wealth event. */
  liquidity_relevance: number;
  signal_value_estimate: number | null;
  signal_currency: string;
  capital_interest_before: number | null;
  capital_interest_after: number | null;
  transaction_value: number | null;
  source_name: string;
  source_role: SourceRole;
  source_url: string;
  evidence_type: string;
  evidence_strength: number;
  natural_person_confidence: number;
  nl_relevance_score: number;
  issuer_desirability_score: number;
  institutional_risk: InstitutionalRisk;
  contactability_confidence: number;
  signal_confidence: number;
  review_priority_score: number;
  review_bucket: ReviewBucket;
  review_action: ReviewAction;
  blocked_by: BlockedReason[];
  match_ready: boolean;
  shortlist_eligible: boolean;
  wealth_relevance_score: number;
  context_summary: string;
  evidence_reference: string;
  raw_source_payload_summary: string;
  notes: string[];
  provenance_sources?: string[];
  provenance_record_ids?: string[];
  enrichment_context?: string;
  context_confirmed: boolean;
  disposal_confirmed: boolean;
  role_confirmed: boolean;
  confirmation_urls: string[];
  confirmation_sources: ConfirmationSource[];
  confirmation_summary: string;
  confirmation_evidence_strength: ConfirmationEvidenceStrength;
  review_action_updated: ReviewAction;
  page_number?: number;
}

export interface ReviewRecord {
  record_id: string;
  person_name: string;
  role: string;
  company_name: string;
  signal_type: string;
  signal_date: string;
  signal_detail: string;
  signal_direction: SignalDirection;
  signal_clarity: SignalClarity;
  liquidity_relevance: number;
  source_name: string;
  source_role: SourceRole;
  source_url: string;
  natural_person_confidence: number;
  nl_relevance_score: number;
  issuer_desirability_score: number;
  review_priority_score: number;
  institutional_risk: InstitutionalRisk;
  review_bucket: ReviewBucket;
  review_action: ReviewAction;
  blocked_by: BlockedReason[];
  signal_confidence: number;
  match_ready: boolean;
  shortlist_eligible: boolean;
  wealth_relevance_score: number;
  context_summary: string;
  evidence_reference: string;
  notes: string;
  context_confirmed: boolean;
  disposal_confirmed: boolean;
  role_confirmed: boolean;
  confirmation_urls: string[];
  confirmation_sources: ConfirmationSource[];
  confirmation_summary: string;
  confirmation_evidence_strength: ConfirmationEvidenceStrength;
  review_action_updated: ReviewAction;
}

export interface ExaEnrichmentResult {
  role?: string;
  company_domain?: string;
  company_country?: string;
  person_type?: PersonType;
  natural_person_confidence_delta?: number;
  contactability_confidence_delta?: number;
  notes?: string[];
  contextSnippet?: string;
}
