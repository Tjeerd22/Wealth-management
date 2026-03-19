export type PersonType = 'unknown' | 'natural_person' | 'legal_entity' | 'family_holding';
export type InstitutionalRisk = 'low' | 'medium' | 'high' | 'unknown';
export type ReviewBucket = 'A' | 'B' | 'C';
export type ReviewAction = 'manual_context_check' | 'manual_person_verify' | 'discard_low_relevance' | 'watchlist_only';
export type ConfirmationEvidenceStrength = 'none' | 'weak' | 'moderate' | 'strong';
export type BlockedReason =
  | 'missing_verified_context'
  | 'unconfirmed_disposal'
  | 'low_nl_relevance'
  | 'low_natural_person_confidence'
  | 'institutional_risk'
  | 'below_min_signal_confidence'
  | 'strict_substantial_holder_gate';

export interface ActorInput {
  runAfmMar19: boolean;
  runAfmSubstantialHoldings: boolean;
  runExaEnrichment: boolean;
  runExaConfirmation?: boolean;
  afmMar19CsvUrl: string;
  afmSubstantialHoldingsCsvUrl: string;
  lookbackDays: number;
  minSignalConfidence: number;
  minNaturalPersonConfidence: number;
  excludeInstitutions: boolean;
  maxReviewRecords: number;
  maxMatchReadyRecords: number;
  topBucketBForExa?: number;
  exaApiKey: string;
  exaTopReviewConfirmations: number;
  exaFreshnessMaxAgeHours: number;
  debug: boolean;
}

export interface SourceStats {
  afm_mar19: number;
  afm_substantial: number;
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
  raw_records: number;
  post_filter_records: number;
  review_records: number;
  match_ready_records: number;
  excluded_institutions: number;
  low_confidence_records: number;
  source_stats: SourceStats;
  review_bucket_stats: ReviewBucketStats;
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
  signal_value_estimate: number | null;
  signal_currency: string;
  capital_interest_before: number | null;
  capital_interest_after: number | null;
  transaction_value: number | null;
  source_name: string;
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
}

export interface ReviewRecord {
  record_id: string;
  person_name: string;
  role: string;
  company_name: string;
  signal_type: string;
  signal_date: string;
  signal_detail: string;
  source_name: string;
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

export interface MatchReadyRecord {
  full_name: string;
  role: string;
  company_name: string;
  company_domain: string;
  signal_type: string;
  signal_date: string;
  signal_detail: string;
  source_url: string;
  signal_confidence: number;
  framing_hint: string;
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
