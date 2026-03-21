import { InstitutionalRisk, NormalizedSignalRecord } from '../types.js';
import { normalizeName } from '../utils/strings.js';

const institutionPatterns = [
  /\bbank\b/, /asset management/, /\bcapital\b/, /investments?/, /\bfund\b/, /partners?/, /holdings?/,
  /pension/, /pensioen/, /insurance/, /\bgroup\b/, /trust/, /advisors?/, /management/, /global/, /international/,
  /ventures?/, /equity/, /norges bank/, /blackrock/, /ubs/, /goldman/, /jpmorgan/, /morgan stanley/, /state street/,
  /citigroup/, /barclays/, /corporation/, /associates/, /services company/, /financial services/, /chase \& co/,
  /trading as/, /\bcc\b/, /\bb\.v\.?\b/, /\bn\.v\.?\b/, /\blimited\b/, /\bltd\b/, /\bplc\b/, /\bs\.a\.?\b/,
  /\bsarl\b/, /\bllc\b/, /\binc\b/, /\bgmbh\b/, /\bag\b/,
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
  // MAR 19 (afm_mar19) explicitly populates MeldingsPlichtigeAchternaam for natural persons.
  // When that field is present, the notifier is a confirmed natural person — skip institutional
  // heuristics entirely. This check is scoped to afm_mar19 only; other sources derive
  // person_last_name from name parsing and cannot provide the same guarantee.
  if (record.source_name === 'afm_mar19' && record.person_last_name && record.person_last_name.length > 1) {
    record.institutional_risk = 'low';
    return record;
  }

  const targetName = record.person_name;
  const { risk, notes } = assessInstitutionalRisk(targetName);
  record.institutional_risk = risk;
  record.notes.push(...notes);
  if (/\d/.test(record.person_name) || /[()]/.test(record.person_name)) {
    record.institutional_risk = 'high';
    record.notes.push('Entity-style punctuation or numeric tokens detected in notifying party name.');
  }
  if (record.institutional_risk === 'high') {
    record.natural_person_confidence = Math.max(0, record.natural_person_confidence - 0.45);
    if (record.person_type === 'unknown') record.person_type = 'legal_entity';
  }
  return record;
}
