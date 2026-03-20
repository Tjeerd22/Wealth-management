import { DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth, mapSourceField } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

const SUBSTANTIAL_FIELD_MAP = {
  signalDateRaw: ['Datum meldingsplicht', 'NotificationDate', 'notification_date', 'Date', 'date'],
  companyName: ['Uitgevende instelling', 'Issuer', 'issuer', 'Company', 'company'],
  personName: ['Meldingsplichtige', 'NotifyingParty', 'Name', 'name'],
  kvkNumber: ['Kvk-nr', 'KvkNr', 'kvk_number'],
  city: ['Plaats', 'City', 'city'],
  capitalInterestBefore: ['CapitalInterestBefore', 'capital_interest_before', 'Before'],
  capitalInterestAfter: ['CapitalInterestAfter', 'capital_interest_after', 'After'],
} as const;

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const normalized = value.replace(',', '.').replace('%', '').trim();
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

export async function ingestAfmSubstantialHoldings(url = DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url, { sourceName: 'AFM substantial holdings' });
  const records = rows.map((row) => {
    const signalDateRaw = mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.signalDateRaw]);
    const companyName = mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.companyName]);
    const personName = mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.personName]);
    const kvkNumber = mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.kvkNumber]);
    const city = mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.city]);
    const before = toNumber(mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.capitalInterestBefore]));
    const after = toNumber(mapSourceField(row, [...SUBSTANTIAL_FIELD_MAP.capitalInterestAfter]));
    const reduction = before !== null && after !== null && after < before;
    return normalizeRecord({
      personName,
      companyName,
      signalDate: signalDateRaw,
      signalType: reduction ? 'substantial_holding_reduction' : 'substantial_holding_change_unclear',
      signalDetail: reduction
        ? `Substantial holding reduced from ${before}% to ${after}%.`
        : 'Substantial holding threshold crossed, but reduction direction is unclear from export.',
      sourceName: 'afm_substantial',
      sourceUrl: url,
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: reduction ? 0.82 : 0.55,
      rawSummary: `datum_meldingsplicht=${signalDateRaw}; uitgevende_instelling=${companyName}; meldingsplichtige=${personName}; kvk_nr=${kvkNumber}; plaats=${city}; before=${before}; after=${after}`,
      notes: [reduction ? 'Reduction appears explicit in AFM holdings export.' : 'Holding change direction unclear; preserve for review only.'],
      personType: 'unknown',
      capitalInterestBefore: before,
      capitalInterestAfter: after,
    });
  });
  logNormalizationHealth('afm_substantial', rows, records);
  return records;
}
