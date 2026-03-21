import { Dataset } from 'apify';
import { NormalizedSignalRecord, ReviewRecord } from '../types.js';
import { toReviewRecord } from './exportReviewDataset.js';

export async function exportShortlistDataset(
  records: NormalizedSignalRecord[],
  maxShortlistRecords: number,
): Promise<ReviewRecord[]> {
  const shortlistRecords = records
    .filter((r) => r.shortlist_eligible)
    .sort((a, b) => b.review_priority_score - a.review_priority_score)
    .slice(0, maxShortlistRecords)
    .map(toReviewRecord);
  if (shortlistRecords.length) await Dataset.pushData(shortlistRecords);
  return shortlistRecords;
}
