import { ActorInput, NormalizedSignalRecord } from '../types.js';
import { applyBlockedByRules } from '../scoring/reviewClassification.js';

export function applySignalGates(record: NormalizedSignalRecord, input: ActorInput): NormalizedSignalRecord {
  const hasVerifiedContext = Boolean(record.role || record.enrichment_context);
  record.shortlist_eligible = false;

  if (record.natural_person_confidence < input.minNaturalPersonConfidence) {
    record.match_ready = false;
  }
  if (record.institutional_risk === 'high' && record.natural_person_confidence < 0.8) {
    record.match_ready = false;
  }
  // Confidence cap for unclear/unconfirmed types is handled in scoreSignal.ts.
  if (record.signal_type.includes('unclear') || record.signal_type.includes('unconfirmed')) {
    record.match_ready = false;
  }
  if (!hasVerifiedContext) {
    record.match_ready = false;
  }
  if (record.signal_confidence < input.minSignalConfidence) {
    record.match_ready = false;
  }

  applyBlockedByRules(record, input.minNaturalPersonConfidence, input.minSignalConfidence);

  const typeOk = !record.signal_type.includes('unclear') || record.liquidity_relevance >= 0.6;

  record.shortlist_eligible = (
    record.natural_person_confidence >= 0.45
    && record.signal_confidence >= 0.40
    && (record.review_bucket === 'A' || record.review_bucket === 'B')
    && typeOk
  );

  return record;
}
