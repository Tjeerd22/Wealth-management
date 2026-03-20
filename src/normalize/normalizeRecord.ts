import { deterministicId, splitName, surnameKey, titleCase } from '../utils/strings.js';
import { toIsoDate } from '../utils/dates.js';
import { NormalizedSignalRecord, PersonType, SignalClarity, SignalDirection, SourceRole } from '../types.js';

interface NormalizeInput {
  personName: string;
  personLastName?: string;
  role?: string;
  companyName: string;
  signalType: string;
  signalDate: string;
  signalDetail: string;
  sourceName: string;
  sourceUrl: string;
  evidenceType: string;
  evidenceStrength: number;
  rawSummary: string;
  personType?: PersonType;
  sourceRole?: SourceRole;
  signalDirection?: SignalDirection;
  signalClarity?: SignalClarity;
  liquidityRelevance?: number;
  capitalInterestBefore?: number | null;
  capitalInterestAfter?: number | null;
  notes?: string[];
}

export function normalizeRecord(input: NormalizeInput): NormalizedSignalRecord {
  const normalizedPersonName = titleCase(input.personName.trim());
  const normalizedCompanyName = input.companyName.trim();
  const normalizedExplicitLastName = (input.personLastName ?? '').trim();
  const { firstName, lastName } = splitName(normalizedPersonName);
  const signalDate = toIsoDate(input.signalDate);
  return {
    record_id: deterministicId(input.sourceName, normalizedPersonName, normalizedCompanyName, signalDate, input.signalType),
    person_name: normalizedPersonName,
    person_last_name: surnameKey(normalizedExplicitLastName || lastName),
    person_type: input.personType ?? 'unknown',
    role: input.role ?? '',
    company_name: normalizedCompanyName,
    company_domain: '',
    // Hardcoded scope assumption: this pipeline only ingests Dutch issuers (AFM sources).
    // This is not a per-record inference; it makes the 0.4 issuerRelevance fallback in
    // scoreSignal.ts unreachable for all current sources. If non-Dutch sources are ever
    // added, this field must be derived from source data rather than set here.
    company_country: 'Netherlands',
    signal_type: input.signalType,
    signal_date: signalDate,
    signal_detail: input.signalDetail,
    signal_direction: input.signalDirection ?? 'unclear',
    signal_clarity: input.signalClarity ?? 'unclear',
    liquidity_relevance: input.liquidityRelevance ?? 0.3,
    signal_value_estimate: null,
    signal_currency: '',
    capital_interest_before: input.capitalInterestBefore ?? null,
    capital_interest_after: input.capitalInterestAfter ?? null,
    transaction_value: null,
    source_name: input.sourceName,
    source_role: input.sourceRole ?? 'primary',
    source_url: input.sourceUrl,
    evidence_type: input.evidenceType,
    evidence_strength: input.evidenceStrength,
    natural_person_confidence: firstName ? 0.45 : 0.3,
    nl_relevance_score: 0.5,
    issuer_desirability_score: 0.5,
    institutional_risk: 'unknown',
    contactability_confidence: 0.1,
    signal_confidence: 0,
    review_priority_score: 0,
    review_bucket: 'C',
    review_action: 'watchlist_only',
    blocked_by: [],
    match_ready: false,
    shortlist_eligible: false,
    wealth_relevance_score: 0,
    context_summary: `${normalizedPersonName} filed a ${input.signalType} for ${normalizedCompanyName} on ${signalDate}. ${input.signalDetail}`,
    evidence_reference: input.sourceUrl,
    raw_source_payload_summary: input.rawSummary,
    notes: input.notes ?? [],
    provenance_sources: [input.sourceName],
    provenance_record_ids: [],
    context_confirmed: false,
    disposal_confirmed: false,
    role_confirmed: false,
    confirmation_urls: [],
    confirmation_sources: [],
    confirmation_summary: '',
    confirmation_evidence_strength: 'none',
    review_action_updated: 'watchlist_only',
  };
}
