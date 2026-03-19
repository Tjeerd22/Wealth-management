import { BlockedReason, NormalizedSignalRecord, ReviewBucket } from '../types.js';
import { deriveReviewAction } from './reviewPriority.js';

function addBlocked(record: NormalizedSignalRecord, reason: BlockedReason, note: string): void {
  if (!record.blocked_by.includes(reason)) record.blocked_by.push(reason);
  if (!record.notes.includes(note)) record.notes.push(note);
}

export function resetReviewFlags(record: NormalizedSignalRecord): void {
  record.blocked_by = [];
}

export function classifyReviewBucket(record: NormalizedSignalRecord): ReviewBucket {
  const likelyPerson = record.natural_person_confidence >= 0.6;
  const commercialInterest = record.nl_relevance_score >= 0.6 && record.signal_confidence >= 0.45;

  if (likelyPerson && commercialInterest) return 'A';
  if (likelyPerson && !commercialInterest) return 'B';
  if (record.natural_person_confidence >= 0.45 && record.nl_relevance_score >= 0.4) return 'B';
  return 'C';
}

export function applyBlockedByRules(record: NormalizedSignalRecord, minNaturalPersonConfidence: number, minSignalConfidence: number): void {
  resetReviewFlags(record);

  if (record.natural_person_confidence < minNaturalPersonConfidence) {
    addBlocked(record, 'low_natural_person_confidence', 'Failed natural person gate.');
  }
  if (record.institutional_risk === 'high' && record.natural_person_confidence < 0.8) {
    addBlocked(record, 'institutional_risk', 'Failed institutional risk gate.');
  }
  if (record.signal_type.includes('unclear') || record.signal_type.includes('unconfirmed')) {
    addBlocked(record, 'unconfirmed_disposal', 'Signal confidence capped due to incomplete evidence; review-only.');
  }
  if (!record.role && !record.enrichment_context) {
    addBlocked(record, 'missing_verified_context', 'Failed verified-context gate.');
  }
  if (record.signal_type === 'substantial_holding_reduction' && record.natural_person_confidence < 0.75) {
    addBlocked(record, 'strict_substantial_holder_gate', 'Failed strict substantial-holding natural-person gate.');
  }
  if (record.nl_relevance_score < 0.45) {
    addBlocked(record, 'low_nl_relevance', 'Low Netherlands wealth-management relevance for current v1 scope.');
  }
  if (record.signal_confidence < minSignalConfidence) {
    addBlocked(record, 'below_min_signal_confidence', 'Below minimum signal confidence threshold.');
  }

  record.review_bucket = classifyReviewBucket(record);
  record.review_action = deriveReviewAction(record);
}
