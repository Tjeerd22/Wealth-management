import { DEFAULT_AFM_MAR19_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchCsvRows } from '../utils/csv.js';

export async function ingestAfmMar19(url = DEFAULT_AFM_MAR19_CSV_URL): Promise<NormalizedSignalRecord[]> {
  const rows = await fetchCsvRows(url);
  return rows.map((row) => {
    const personName = row.Notifiable || row.notifiable || row.Name || row.name || row.LastName || row.last_name || 'Unknown';
    const companyName = row.IssuingInstitution || row.issuer || row.Company || row.company || 'Unknown issuer';
    const signalDate = row.TransactionDate || row.transaction_date || row.Date || row.date || '';
    const lastName = row.LastName || row.last_name || '';
    return normalizeRecord({
      personName,
      companyName,
      signalDate,
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: `AFM MAR 19 filing for ${companyName}; transaction details not fully disclosed in CSV export.`,
      sourceName: 'afm_mar19',
      sourceUrl: url,
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: `transaction_date=${signalDate}; issuing_institution=${companyName}; notifiable=${personName}; last_name=${lastName}`,
      notes: ['MAR 19 CSV is timing-strong but thin; disposal not confirmed from source export alone.'],
      personType: 'unknown',
    });
  });
}
