import { Dataset } from 'apify';
import { NormalizedSignalRecord, ReviewRecord } from '../types.js';

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
    source_name: record.source_name,
    source_url: record.source_url,
    natural_person_confidence: record.natural_person_confidence,
    nl_relevance_score: record.nl_relevance_score,
    institutional_risk: record.institutional_risk,
    review_bucket: record.review_bucket,
    blocked_by: record.blocked_by,
    signal_confidence: record.signal_confidence,
    match_ready: record.match_ready,
    notes: record.notes.join(' | '),
  };
}

export async function exportReviewDataset(records: NormalizedSignalRecord[], maxReviewRecords: number): Promise<ReviewRecord[]> {
  const dataset = await Dataset.open('review');
  const reviewRecords = records
    .filter((record) => record.signal_confidence >= 0.25)
    .sort((a, b) => bucketOrder[a.review_bucket] - bucketOrder[b.review_bucket] || b.signal_confidence - a.signal_confidence || b.nl_relevance_score - a.nl_relevance_score)
    .slice(0, maxReviewRecords)
    .map(toReviewRecord);
  if (reviewRecords.length) await dataset.pushData(reviewRecords);
  return reviewRecords;
}
