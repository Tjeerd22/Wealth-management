import { DEFAULT_AFM_MAR19_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth, mapSourceField } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

const MAR19_FIELD_MAP = {
  signalDateRaw: ['Transactie', 'TransactionDate', 'transaction_date', 'Date', 'date'],
  companyName: ['Uitgevende instelling', 'IssuingInstitution', 'issuer', 'Company', 'company'],
  personName: ['Meldingsplichtige', 'Notifiable', 'Name', 'name', 'LastName', 'last_name'],
  personLastName: ['MeldingsPlichtigeAchternaam', 'LastName', 'last_name'],
} as const;

export async function ingestAfmMar19(url = DEFAULT_AFM_MAR19_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url, { sourceName: 'AFM MAR 19' });
  const records = rows.map((row) => {
    const signalDateRaw = mapSourceField(row, [...MAR19_FIELD_MAP.signalDateRaw]);
    const companyName = mapSourceField(row, [...MAR19_FIELD_MAP.companyName]);
    const personName = mapSourceField(row, [...MAR19_FIELD_MAP.personName]);
    const personLastName = mapSourceField(row, [...MAR19_FIELD_MAP.personLastName]);
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
  logNormalizationHealth('afm_mar19', rows, records);
  return records;
}
