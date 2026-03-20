import { Dataset } from 'apify';
import { NormalizedSignalRecord, ReviewRecord } from '../types.js';
import { getIssuerClusterKey, scoreReviewPriority } from '../scoring/reviewPriority.js';

const bucketOrder = { A: 0, B: 1, C: 2 } as const;

export function toReviewRecord(record: NormalizedSignalRecord): ReviewRecord {
  return {
    record_id: record.record_id,
    person_name: record.person_name,
    role: record.role,
    company_name: record.company_name,
    signal_type: record.signal_type,
    signal_date: record.signal_date,
    signal_detail: record.signal_detail,
    signal_direction: record.signal_direction,
    signal_clarity: record.signal_clarity,
    liquidity_relevance: record.liquidity_relevance,
    source_name: record.source_name,
    source_role: record.source_role,
    source_url: record.source_url,
    natural_person_confidence: record.natural_person_confidence,
    nl_relevance_score: record.nl_relevance_score,
    issuer_desirability_score: record.issuer_desirability_score,
    review_priority_score: record.review_priority_score,
    institutional_risk: record.institutional_risk,
    review_bucket: record.review_bucket,
    review_action: record.review_action,
    blocked_by: record.blocked_by,
    signal_confidence: record.signal_confidence,
    match_ready: record.match_ready,
    shortlist_eligible: record.shortlist_eligible,
    notes: record.notes.join(' | '),
    context_confirmed: record.context_confirmed,
    disposal_confirmed: record.disposal_confirmed,
    role_confirmed: record.role_confirmed,
    confirmation_urls: record.confirmation_urls,
    confirmation_sources: record.confirmation_sources,
    confirmation_summary: record.confirmation_summary,
    confirmation_evidence_strength: record.confirmation_evidence_strength,
    review_action_updated: record.review_action_updated,
  };
}

export function rankReviewRecords(records: NormalizedSignalRecord[]): NormalizedSignalRecord[] {
  const eligible = records.filter((record) => record.signal_confidence >= 0.25);
  const baseSorted = [...eligible].sort((a, b) => bucketOrder[a.review_bucket] - bucketOrder[b.review_bucket]
    || b.issuer_desirability_score - a.issuer_desirability_score
    || b.nl_relevance_score - a.nl_relevance_score
    || b.natural_person_confidence - a.natural_person_confidence
    || b.signal_confidence - a.signal_confidence);

  const issuerSeen = new Map<string, number>();
  for (const record of baseSorted) {
    const issuerKey = getIssuerClusterKey(record);
    const rankWithinIssuer = issuerSeen.get(issuerKey) ?? 0;
    record.review_priority_score = scoreReviewPriority(record, rankWithinIssuer);
    issuerSeen.set(issuerKey, rankWithinIssuer + 1);
  }

  return baseSorted.sort((a, b) => bucketOrder[a.review_bucket] - bucketOrder[b.review_bucket]
    || b.review_priority_score - a.review_priority_score
    || b.issuer_desirability_score - a.issuer_desirability_score
    || b.nl_relevance_score - a.nl_relevance_score
    || b.natural_person_confidence - a.natural_person_confidence
    || b.signal_confidence - a.signal_confidence);
}

export function topByIssuer(records: NormalizedSignalRecord[], perIssuer: number): ReviewRecord[] {
  const ranked = rankReviewRecords(records);
  const issuerCounts = new Map<string, number>();
  return ranked.filter((record) => {
    const key = getIssuerClusterKey(record);
    const count = issuerCounts.get(key) ?? 0;
    if (count >= perIssuer) return false;
    issuerCounts.set(key, count + 1);
    return true;
  }).map(toReviewRecord);
}

export async function exportReviewDataset(records: NormalizedSignalRecord[], maxReviewRecords: number): Promise<ReviewRecord[]> {
  const dataset = await Dataset.open('review');
  const reviewRecords = rankReviewRecords(records)
    .slice(0, maxReviewRecords)
    .map(toReviewRecord);
  if (reviewRecords.length) await dataset.pushData(reviewRecords);
  return reviewRecords;
}
