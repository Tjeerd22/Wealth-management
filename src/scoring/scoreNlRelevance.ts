import { NormalizedSignalRecord } from '../types.js';
import { clamp01, normalizeCompanyName, normalizeName } from '../utils/strings.js';

const DUTCH_LISTED_ISSUERS = [
  'asml holding', 'adyen', 'prosus', 'unilever', 'pharming group', 'be semiconductor industries', 'besi', 'cmcom',
  'heijmans', 'ceg', 'flow traders', 'just eat takeawaycom', 'shell', 'akzo nobel', 'randstad',
];

function isDutchListedIssuer(companyName: string): boolean {
  const normalized = normalizeCompanyName(companyName).replace(/[^a-z0-9]+/g, '');
  return DUTCH_LISTED_ISSUERS.some((issuer) => normalized.includes(issuer.replace(/[^a-z0-9]+/g, '')));
}

export function scoreNlRelevance(record: NormalizedSignalRecord): number {
  const company = normalizeName(record.company_name);
  const person = normalizeName(record.person_name);
  let score = 0.2;

  if (record.company_country.toLowerCase() === 'netherlands') score += 0.2;
  if (/\b(nv|n\.v\.|amsterdam|nederland|netherlands|dutch)\b/.test(company)) score += 0.15;
  if (isDutchListedIssuer(record.company_name)) score += 0.2;
  if (record.source_name.startsWith('afm_')) score += 0.08;
  if (record.signal_type === 'substantial_holding_reduction') score += 0.08;
  if (record.role && /\b(chair|ceo|cfo|coo|cto|director|executive|board|founder|pdmr)\b/i.test(record.role)) score += 0.07;
  if (/\b(van|de|der|den|ter|ten|te)\b/.test(person) || /ij|aa|oo|eu/.test(person)) score += 0.04;
  if (/\b(bank|fund|capital|asset management|holdings?|partners?|trust|insurance|group)\b/.test(person)) score -= 0.18;
  if (/plc|limited|inc|corp|corporation/.test(company) && !/netherlands|dutch|amsterdam/.test(company)) score -= 0.1;
  if (record.signal_type.includes('unclear')) score -= 0.05;

  return clamp01(score);
}
