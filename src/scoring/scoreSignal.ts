import { NormalizedSignalRecord } from '../types.js';
import { clamp01 } from '../utils/strings.js';
import { isWithinLookback, parseDate } from '../utils/dates.js';

export function scoreSignal(record: NormalizedSignalRecord, lookbackDays: number): NormalizedSignalRecord {
  const sourceQuality = record.source_name === 'afm_mar19' ? 0.68 : 0.82;
  const evidence = record.evidence_strength;
  const recency = isWithinLookback(record.signal_date, lookbackDays) ? 1 : 0.18;
  const naturalPerson = record.natural_person_confidence;
  const nlRelevance = record.nl_relevance_score;
  const signalTypeStrength = record.signal_type === 'substantial_holding_reduction'
    ? 0.88
    : record.signal_type === 'pdmr_transaction_unconfirmed'
      ? 0.52
      : 0.34;
  const issuerRelevance = /\b(nv|n\.v\.|holding|group|amsterdam|netherlands|dutch)\b/i.test(record.company_name)
    ? 0.7
    : record.company_country.toLowerCase() === 'netherlands'
      ? 0.58
      : 0.4;
  const contextQuality = Math.min(1, (record.role ? 0.55 : 0) + (record.enrichment_context ? 0.3 : 0) + (record.company_domain ? 0.15 : 0));

  const score = (sourceQuality * 0.14)
    + (evidence * 0.19)
    + (recency * 0.12)
    + (naturalPerson * 0.24)
    + (nlRelevance * 0.17)
    + (signalTypeStrength * 0.08)
    + (issuerRelevance * 0.03)
    + (contextQuality * 0.03);
  record.signal_confidence = clamp01(score);
  record.wealth_relevance_score = clamp01(
    (record.liquidity_relevance * 0.4)
    + (record.natural_person_confidence * 0.35)
    + (record.issuer_desirability_score * 0.25),
  );

  if (record.signal_type === 'pdmr_transaction_unconfirmed') {
    record.signal_confidence = Math.min(record.signal_confidence, 0.74 - ((1 - nlRelevance) * 0.08));
  }
  if (record.signal_type.includes('unclear')) {
    record.signal_confidence = Math.min(record.signal_confidence, 0.48);
  }
  // Consolidated cap for all unclear/unconfirmed types. Previously split across scoreSignal
  // and signalGates.ts. For 'unclear' the 0.48 cap above is tighter and always wins;
  // for 'unconfirmed' (pdmr) this 0.58 is the operative ceiling (0.58 < 0.66–0.74).
  if (record.signal_type.includes('unclear') || record.signal_type.includes('unconfirmed')) {
    record.signal_confidence = Math.min(record.signal_confidence, 0.58);
  }
  if (record.institutional_risk === 'high') {
    record.signal_confidence = Math.min(record.signal_confidence, 0.28);
  }
  const parsed = parseDate(record.signal_date);
  if (!parsed) {
    record.signal_confidence = Math.min(record.signal_confidence, 0.2);
    record.notes.push('Invalid signal date reduced confidence.');
  }
  return record;
}
