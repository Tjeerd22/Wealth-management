import { NormalizedSignalRecord, ReviewAction } from '../types.js';
import { clamp01, normalizeCompanyName } from '../utils/strings.js';
import { parseDate } from '../utils/dates.js';

export function getReviewContextAvailability(record: NormalizedSignalRecord): number {
  return Math.min(1, (record.role ? 0.45 : 0) + (record.enrichment_context ? 0.35 : 0) + (record.company_domain ? 0.2 : 0));
}

export function getReviewRecencyScore(signalDate: string, referenceDate = new Date()): number {
  const parsed = parseDate(signalDate);
  if (!parsed) return 0;
  const diffDays = Math.max(0, (referenceDate.getTime() - parsed.getTime()) / (24 * 60 * 60 * 1000));
  if (diffDays <= 7) return 1;
  if (diffDays <= 21) return 0.82;
  if (diffDays <= 45) return 0.62;
  if (diffDays <= 90) return 0.38;
  return 0.18;
}

export function getIssuerClusterKey(record: NormalizedSignalRecord): string {
  const normalized = normalizeCompanyName(record.company_name).replace(/[^a-z0-9]+/g, ' ').trim();
  return normalized || 'unknown_issuer';
}

export function getIssuerClusterPenalty(rankWithinIssuer: number): number {
  if (rankWithinIssuer <= 0) return 0;
  return Math.min(0.24, rankWithinIssuer * 0.09);
}

export function scoreReviewPriority(record: NormalizedSignalRecord, rankWithinIssuer = 0, referenceDate = new Date()): number {
  const recency = getReviewRecencyScore(record.signal_date, referenceDate);
  const contextAvailability = getReviewContextAvailability(record);
  const clusterPenalty = getIssuerClusterPenalty(rankWithinIssuer);

  const score = (record.natural_person_confidence * 0.25)
    + (record.nl_relevance_score * 0.2)
    + (record.issuer_desirability_score * 0.22)
    + (recency * 0.18)
    + (contextAvailability * 0.15)
    - clusterPenalty;

  return clamp01(score);
}

export function deriveReviewAction(record: NormalizedSignalRecord): ReviewAction {
  if (record.blocked_by.includes('low_nl_relevance') && record.nl_relevance_score < 0.35) return 'discard_low_relevance';
  if (record.blocked_by.includes('missing_verified_context') && record.natural_person_confidence >= 0.6) return 'manual_context_check';
  if (record.blocked_by.includes('low_natural_person_confidence') || record.person_type === 'family_holding') return 'manual_person_verify';
  return 'watchlist_only';
}
