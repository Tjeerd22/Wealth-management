import { NormalizedSignalRecord } from '../types.js';
import { clamp01, normalizeCompanyName } from '../utils/strings.js';

const TIER_ONE_ISSUERS = [
  'asml holding', 'adyen', 'prosus', 'universal music group', 'be semiconductor industries', 'besi',
  'pharming group', 'shell', 'unilever', 'aegon', 'flow traders', 'akzo nobel', 'randstad',
];

const TIER_TWO_ISSUERS = [
  'ceg', 'cmcom', 'cm.com', 'heijmans', 'fugro', 'rhi magnesita', 'coca-cola europacific partners',
];

export function scoreIssuerDesirability(record: NormalizedSignalRecord): number {
  const company = normalizeCompanyName(record.company_name);
  const companyCompact = company.replace(/[^a-z0-9]+/g, '');
  let score = 0.25;

  if (record.company_country.toLowerCase() === 'netherlands') score += 0.18;
  if (/\b(nv|n\.v\.|amsterdam|netherlands|dutch)\b/i.test(record.company_name)) score += 0.14;
  if (record.signal_type === 'pdmr_transaction_unconfirmed') score += 0.06;
  if (record.signal_type === 'substantial_holding_reduction') score += 0.08;
  if (record.role && /\b(chair|ceo|cfo|coo|cto|director|executive|board|founder|pdmr)\b/i.test(record.role)) score += 0.08;
  if (record.company_domain) score += 0.05;
  if (TIER_ONE_ISSUERS.some((issuer) => companyCompact.includes(issuer.replace(/[^a-z0-9]+/g, '')))) score += 0.18;
  if (TIER_TWO_ISSUERS.some((issuer) => companyCompact.includes(issuer.replace(/[^a-z0-9]+/g, '')))) score += 0.1;
  if (/\b(bank|fund|trust|insurance|capital management)\b/i.test(record.company_name)) score -= 0.12;
  if (/\b(holding|holdings)\b/i.test(record.company_name) && !/\b(asml|be semiconductor|universal music)\b/i.test(record.company_name)) score -= 0.05;
  if (/\b(plc|limited|inc|corp|corporation)\b/i.test(record.company_name) && record.company_country.toLowerCase() !== 'netherlands') score -= 0.08;

  return clamp01(score);
}
