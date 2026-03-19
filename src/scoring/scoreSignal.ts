import { NormalizedSignalRecord } from '../types.js';
import { clamp01 } from '../utils/strings.js';
import { isWithinLookback, parseDate } from '../utils/dates.js';

export function scoreSignal(record: NormalizedSignalRecord, lookbackDays: number): NormalizedSignalRecord {
  const sourceQuality = record.source_name === 'afm_mar19' ? 0.75 : 0.8;
  const evidence = record.evidence_strength;
  const recency = isWithinLookback(record.signal_date, lookbackDays) ? 1 : 0.25;
  const naturalPerson = record.natural_person_confidence;
  const signalTypeStrength = record.signal_type === 'substantial_holding_reduction'
    ? 0.9
    : record.signal_type === 'pdmr_transaction_unconfirmed'
      ? 0.68
      : 0.45;
  const enrichment = Math.min(1, 0.2 + (record.role ? 0.3 : 0) + (record.company_domain ? 0.3 : 0) + (record.enrichment_context ? 0.2 : 0));

  const score = (sourceQuality * 0.2) + (evidence * 0.2) + (recency * 0.15) + (naturalPerson * 0.2) + (signalTypeStrength * 0.2) + (enrichment * 0.05);
  record.signal_confidence = clamp01(score);

  if (record.signal_type === 'pdmr_transaction_unconfirmed') {
    record.signal_confidence = Math.min(record.signal_confidence, 0.74);
  }
  if (record.institutional_risk === 'high') {
    record.signal_confidence = Math.min(record.signal_confidence, 0.35);
  }
  const parsed = parseDate(record.signal_date);
  if (!parsed) {
    record.signal_confidence = Math.min(record.signal_confidence, 0.2);
    record.notes.push('Invalid signal date reduced confidence.');
  }
  return record;
}
