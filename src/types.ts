export type PersonType = 'unknown' | 'natural_person' | 'legal_entity' | 'family_holding';
export type InstitutionalRisk = 'low' | 'medium' | 'high' | 'unknown';

export interface ActorInput {
  runAfmMar19: boolean;
  runAfmSubstantialHoldings: boolean;
  runExaEnrichment: boolean;
  afmMar19CsvUrl: string;
  afmSubstantialHoldingsCsvUrl: string;
  lookbackDays: number;
  minSignalConfidence: number;
  minNaturalPersonConfidence: number;
  excludeInstitutions: boolean;
  maxReviewRecords: number;
  maxMatchReadyRecords: number;
  exaApiKey: string;
  debug: boolean;
}

export interface SourceStats {
  afm_mar19: number;
  afm_substantial: number;
  exa_enriched: number;
}

export interface RunSummary {
  raw_records: number;
  post_filter_records: number;
  review_records: number;
  match_ready_records: number;
  excluded_institutions: number;
  low_confidence_records: number;
  source_stats: SourceStats;
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
  institutional_risk: InstitutionalRisk;
  contactability_confidence: number;
  signal_confidence: number;
  match_ready: boolean;
  raw_source_payload_summary: string;
  notes: string[];
  provenance_sources?: string[];
  enrichment_context?: string;
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
  institutional_risk: InstitutionalRisk;
  signal_confidence: number;
  match_ready: boolean;
  notes: string;
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
