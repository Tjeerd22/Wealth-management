import { DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const normalized = value.replace(',', '.').replace('%', '').trim();
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

export async function ingestAfmSubstantialHoldings(url = DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url, { sourceName: 'AFM substantial holdings' });
  return rows.map((row) => {
    const personName = row.NotifyingParty || row.notifying_party || row.Name || row.name || 'Unknown';
    const companyName = row.Issuer || row.issuer || row.Company || row.company || 'Unknown issuer';
    const signalDate = row.NotificationDate || row.notification_date || row.Date || row.date || '';
    const before = toNumber(row.CapitalInterestBefore || row.capital_interest_before || row.Before);
    const after = toNumber(row.CapitalInterestAfter || row.capital_interest_after || row.After);
    const reduction = before !== null && after !== null && after < before;
    return normalizeRecord({
      personName,
      companyName,
      signalDate,
      signalType: reduction ? 'substantial_holding_reduction' : 'substantial_holding_change_unclear',
      signalDetail: reduction
        ? `Substantial holding reduced from ${before}% to ${after}%.`
        : 'Substantial holding threshold crossed, but reduction direction is unclear from export.',
      sourceName: 'afm_substantial',
      sourceUrl: url,
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: reduction ? 0.82 : 0.55,
      rawSummary: `notification_date=${signalDate}; issuer=${companyName}; notifying_party=${personName}; before=${before}; after=${after}`,
      notes: [reduction ? 'Reduction appears explicit in AFM holdings export.' : 'Holding change direction unclear; preserve for review only.'],
      personType: 'unknown',
      capitalInterestBefore: before,
      capitalInterestAfter: after,
    });
  });
}
