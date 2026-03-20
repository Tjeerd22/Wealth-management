import { DEFAULT_AFM_MAR19_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth, mapRequiredSourceField, validateSourceSchema } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

export const AFM_MAR19_REQUIRED_COLUMNS = [
  'Transactie',
  'Uitgevende instelling',
  'Meldingsplichtige',
  'MeldingsPlichtigeAchternaam',
] as const;

export async function ingestAfmMar19(url = DEFAULT_AFM_MAR19_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url, { sourceName: 'AFM MAR 19' });
  validateSourceSchema(rows, { sourceName: 'AFM MAR 19', requiredColumns: [...AFM_MAR19_REQUIRED_COLUMNS] });
  const records = rows.map((row) => {
    const signalDateRaw = mapRequiredSourceField(row, 'Transactie');
    const companyName = mapRequiredSourceField(row, 'Uitgevende instelling');
    const personName = mapRequiredSourceField(row, 'Meldingsplichtige');
    const personLastName = mapRequiredSourceField(row, 'MeldingsPlichtigeAchternaam');
    return normalizeRecord({
      personName,
      personLastName,
      companyName,
      signalDate: signalDateRaw,
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: `AFM MAR 19 filing for ${companyName || 'unmapped issuer'}; transaction details not fully disclosed in CSV export.`,
      sourceName: 'afm_mar19',
      sourceUrl: url,
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: `transactie=${signalDateRaw}; uitgevende_instelling=${companyName}; meldingsplichtige=${personName}; meldingsplichtigeachternaam=${personLastName}`,
      notes: ['MAR 19 CSV is timing-strong but thin; disposal not confirmed from source export alone.'],
      personType: 'unknown',
    });
  });
  logNormalizationHealth('afm_mar19', records);
  return records;
}
