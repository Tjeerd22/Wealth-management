import { DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth, mapRequiredSourceField, validateSourceSchema } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

export const AFM_SUBSTANTIAL_REQUIRED_COLUMNS = [
  'Datum meldingsplicht',
  'Uitgevende instelling',
  'Meldingsplichtige',
  'Kvk-nr',
  'Plaats',
] as const;

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const normalized = value.replace(',', '.').replace('%', '').trim();
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

export async function ingestAfmSubstantialHoldings(url = DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url, { sourceName: 'AFM substantial holdings' });
  validateSourceSchema(rows, { sourceName: 'AFM substantial holdings', requiredColumns: [...AFM_SUBSTANTIAL_REQUIRED_COLUMNS] });
  const records: NormalizedSignalRecord[] = [];
  for (const row of rows) {
    const signalDateRaw = mapRequiredSourceField(row, 'Datum meldingsplicht');
    const companyName = mapRequiredSourceField(row, 'Uitgevende instelling');
    const personName = mapRequiredSourceField(row, 'Meldingsplichtige');
    const kvkNumber = mapRequiredSourceField(row, 'Kvk-nr');
    const city = mapRequiredSourceField(row, 'Plaats');
    const before = toNumber(row['Kapitaalbelang voor melding']?.trim());
    const after = toNumber(row['Kapitaalbelang na melding']?.trim());
    const votingBefore = toNumber(row['Stemrecht voor melding']?.trim());
    const votingAfter = toNumber(row['Stemrecht na melding']?.trim());
    const reduction = before !== null && after !== null && after < before;
    records.push(normalizeRecord({
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
      rawSummary: [
        `datum_meldingsplicht=${signalDateRaw}`,
        `uitgevende_instelling=${companyName}`,
        `meldingsplichtige=${personName}`,
        `kvk_nr=${kvkNumber}`,
        `plaats=${city}`,
        `kapitaalbelang_voor=${before}`,
        `kapitaalbelang_na=${after}`,
        `stemrecht_voor=${votingBefore}`,
        `stemrecht_na=${votingAfter}`,
      ].join('; '),
      notes: [reduction ? 'Reduction appears explicit in AFM holdings export.' : 'Holding change direction unclear; preserve for review only.'],
      personType: 'unknown',
      capitalInterestBefore: before,
      capitalInterestAfter: after,
    }));
  }
  logNormalizationHealth('afm_substantial', records);
  return records;
}
