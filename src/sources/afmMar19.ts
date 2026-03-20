import { DEFAULT_AFM_MAR19_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth, validateRequiredColumns } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

/**
 * Hard schema contract for AFM MAR 19 (Melding artikel 19 MAR).
 * These are the exact Dutch column names from the AFM export endpoint.
 * If any of these are absent, the run fails immediately — silent degradation
 * into empty-string fields is not allowed.
 */
export const AFM_MAR19_REQUIRED_COLUMNS = [
  'Transactie',
  'Uitgevende instelling',
  'Meldingsplichtige',
  'MeldingsPlichtigeAchternaam',
] as const;

export async function ingestAfmMar19(url = DEFAULT_AFM_MAR19_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url, { sourceName: 'AFM MAR 19' });

  // Hard fail if required columns are absent. This catches AFM schema changes before
  // any normalization runs and prevents silent field-to-empty-string corruption.
  validateRequiredColumns(rows, AFM_MAR19_REQUIRED_COLUMNS, 'AFM MAR 19');

  const records = rows.map((row) => {
    // Direct column access — no generic alias fallback for required fields.
    const signalDateRaw = row['Transactie'] ?? '';
    const companyName = row['Uitgevende instelling'] ?? '';
    const personName = row['Meldingsplichtige'] ?? '';
    const personLastName = row['MeldingsPlichtigeAchternaam'] ?? '';

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
