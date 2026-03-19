import { ActorInput, NormalizedSignalRecord } from '../types.js';

export function applySignalGates(record: NormalizedSignalRecord, input: ActorInput): NormalizedSignalRecord {
  const hasVerifiedContext = Boolean(record.role || record.enrichment_context);
  if (record.natural_person_confidence < input.minNaturalPersonConfidence) {
    record.match_ready = false;
    record.notes.push('Failed natural person gate.');
  }
  if (record.institutional_risk === 'high' && record.natural_person_confidence < 0.8) {
    record.match_ready = false;
    record.notes.push('Failed institutional risk gate.');
  }
  if (record.signal_type.includes('unclear') || record.signal_type.includes('unconfirmed')) {
    record.signal_confidence = Math.min(record.signal_confidence, 0.58);
    record.match_ready = false;
    record.notes.push('Signal confidence capped due to incomplete evidence; review-only.');
  }
  if (!hasVerifiedContext) {
    record.match_ready = false;
    record.notes.push('Failed verified-context gate.');
  }
  if (record.signal_type === 'substantial_holding_reduction' && record.natural_person_confidence < 0.75) {
    record.match_ready = false;
    record.notes.push('Failed strict substantial-holding natural-person gate.');
  }
  if (record.signal_confidence < input.minSignalConfidence) {
    record.match_ready = false;
    record.notes.push('Below minimum signal confidence threshold.');
  }
  return record;
}
