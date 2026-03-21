import { NormalizedSignalRecord } from '../types.js';
import { clamp01, normalizeName } from '../utils/strings.js';

const ENTITY_PATTERN = /\b(bv|nv|limited|ltd|plc|inc|gmbh|fund|capital|partners|holdings|bank|group|corporation|associates|management|trust|asset)\b/;
const HUMAN_PREFIX_PATTERN = /^(mr|mrs|ms|dr|prof)\b/;
const COMMA_PREFIX_PATTERN = /^[a-z'\-]+,\s*[a-z'\-]+(?:\s+[a-z'\-]+)*$/;
const DUTCH_COMMA_PREFIX_PATTERN = /^[a-z'\-]{2,}\s+(?:van|de|den|der|ter|ten)(?:\s+(?:de|den|der))?\s+[a-z]$/;
const FULL_NAME_PATTERN = /^[a-z'\-]{2,}\s+[a-z][a-z'\-]+(?:\s+[a-z][a-z'\-]+)*$/;
const INITIAL_SURNAME_PATTERN = /^(?:[a-z]\.?|[a-z]{1,2})\s+[a-z][a-z'\-]+(?:\s+[a-z][a-z'\-]+)*$/;
const SURNAME_INITIAL_PATTERN = /^[a-z][a-z'\-]+(?:\s+[a-z][a-z'\-]+)*\s+[a-z](?:\.[a-z])?\.?$/;

export function scoreNaturalPersonConfidence(record: NormalizedSignalRecord): number {
  const normalized = normalizeName(record.person_name);
  let score = 0.35;

  if (/^\S+$/.test(normalized)) score = 0.32;
  if (HUMAN_PREFIX_PATTERN.test(normalized) || COMMA_PREFIX_PATTERN.test(normalized)) score = 0.78;
  if (DUTCH_COMMA_PREFIX_PATTERN.test(normalized)) score = Math.max(score, 0.8);
  if (FULL_NAME_PATTERN.test(normalized)) score = Math.max(score, 0.76);
  if ((INITIAL_SURNAME_PATTERN.test(normalized) || SURNAME_INITIAL_PATTERN.test(normalized)) && !ENTITY_PATTERN.test(normalized)) score = Math.max(score, 0.58);
  if (/^[a-z]\.?$/.test(normalized) || /^\S+$/.test(normalized)) score = Math.min(score, 0.42);
  if (/\d/.test(normalized) || /trading as|\(|\)/.test(normalized)) score = Math.min(score, 0.12);
  if (ENTITY_PATTERN.test(normalized) && !DUTCH_COMMA_PREFIX_PATTERN.test(normalized)) score = Math.min(score, 0.12);

  // AFM MAR 19 explicitly populates MeldingsPlichtigeAchternaam only for natural persons.
  // Scoped to afm_mar19 only — other sources derive person_last_name from name parsing,
  // so the same field cannot be treated as an explicit person confirmation.
  if (record.source_name === 'afm_mar19' && record.person_last_name && record.person_last_name.length > 1) score += 0.10;
  if (record.role) score += 0.05;
  if (record.person_type === 'family_holding') score -= 0.12;
  if (record.person_type === 'natural_person') score += 0.06;
  if (record.person_type === 'legal_entity') score = Math.min(score, 0.1);
  if (record.institutional_risk === 'high') score = Math.min(score, 0.15);

  return clamp01(score);
}
