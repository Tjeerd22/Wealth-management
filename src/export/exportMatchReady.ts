import { Dataset } from 'apify';
import { MatchReadyRecord, NormalizedSignalRecord } from '../types.js';

export function framingHint(record: NormalizedSignalRecord): string {
  if (record.signal_type === 'substantial_holding_reduction') return 'Recent holding reduction linked to a potential liquidity event';
  return 'Recent insider transaction at a listed company';
}

export function toMatchReadyRecord(record: NormalizedSignalRecord): MatchReadyRecord {
  return {
    full_name: record.person_name,
    role: record.role,
    company_name: record.company_name,
    company_domain: record.company_domain,
    signal_type: record.signal_type,
    signal_date: record.signal_date,
    signal_detail: record.signal_detail,
    source_url: record.source_url,
    signal_confidence: record.signal_confidence,
    framing_hint: framingHint(record),
  };
}

export async function exportMatchReady(records: NormalizedSignalRecord[], maxMatchReadyRecords: number): Promise<MatchReadyRecord[]> {
  const dataset = await Dataset.open('match-ready');
  const matchReady = records
    .filter((record) => record.match_ready)
    .sort((a, b) => b.signal_confidence - a.signal_confidence)
    .slice(0, maxMatchReadyRecords)
    .map(toMatchReadyRecord);
  if (matchReady.length) await dataset.pushData(matchReady);
  return matchReady;
}
