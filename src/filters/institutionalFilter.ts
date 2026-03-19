import { InstitutionalRisk, NormalizedSignalRecord } from '../types.js';
import { normalizeName } from '../utils/strings.js';

const institutionPatterns = [
  /\bbank\b/, /asset management/, /\bcapital\b/, /investments?/, /\bfund\b/, /partners?/, /holdings?/,
  /pension/, /pensioen/, /insurance/, /\bgroup\b/, /trust/, /advisors?/, /management/, /global/, /international/,
  /ventures?/, /equity/, /norges bank/, /blackrock/, /ubs/, /goldman/, /jpmorgan/, /morgan stanley/, /state street/,
  /\bb\.v\.?\b/, /\bn\.v\.?\b/, /\bltd\b/, /\bplc\b/, /\bs\.a\.?\b/, /\bsarl\b/, /\bllc\b/, /\binc\b/, /\bgmbh\b/,
];

export function assessInstitutionalRisk(name: string): { risk: InstitutionalRisk; notes: string[] } {
  const normalized = normalizeName(name);
  const matches = institutionPatterns.filter((pattern) => pattern.test(normalized)).map((pattern) => pattern.source);
  if (!matches.length) return { risk: 'low', notes: [] };
  const familyHolding = /family|familie/.test(normalized);
  if (familyHolding) {
    return { risk: 'high', notes: ['Family holding pattern detected; preserved for review instead of auto-exclusion.'] };
  }
  return { risk: 'high', notes: [`Institutional naming heuristics matched: ${matches.join(', ')}`] };
}

export function applyInstitutionalFilter(record: NormalizedSignalRecord): NormalizedSignalRecord {
  const targetName = `${record.person_name} ${record.company_name}`;
  const { risk, notes } = assessInstitutionalRisk(targetName);
  record.institutional_risk = risk;
  record.notes.push(...notes);
  if (risk === 'high') {
    record.natural_person_confidence = Math.max(0, record.natural_person_confidence - 0.45);
    if (record.person_type === 'unknown') record.person_type = 'legal_entity';
  }
  return record;
}
