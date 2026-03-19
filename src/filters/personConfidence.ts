import { NormalizedSignalRecord } from '../types.js';
import { clamp01, normalizeName } from '../utils/strings.js';

export function scoreNaturalPersonConfidence(record: NormalizedSignalRecord): number {
  const normalized = normalizeName(record.person_name);
  let score = 0.2;
  if (/^\S+$/.test(normalized)) score -= 0.15;
  if (/^[a-z]\.\s+[a-z]+/.test(normalized)) score += 0.25;
  if (/^[a-z]+\s+[a-z][a-z'\-]+/.test(normalized)) score += 0.45;
  if (/\b(bv|nv|limited|ltd|plc|inc|gmbh|fund|capital|partners|holdings|bank|group|corporation|associates)\b/.test(normalized)) score -= 0.55;
  if (/\d/.test(normalized) || /trading as|\(|\)/.test(normalized)) score -= 0.45;
  if (record.role) score += 0.15;
  if (record.person_type === 'family_holding') score -= 0.15;
  if (record.person_type === 'natural_person') score += 0.1;
  if (record.institutional_risk === 'high') score -= 0.35;
  return clamp01(score);
}
