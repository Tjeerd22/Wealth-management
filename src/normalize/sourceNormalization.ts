import { NormalizedSignalRecord } from '../types.js';
import { logInfo, logWarn } from '../utils/logging.js';

const UNKNOWN_TOKENS = new Set(['', 'unknown', 'unknown issuer', 'unknown person', 'n/a', 'na', 'null', 'undefined']);
const CORE_IDENTITY_FAILURE_RATIO = 0.2;

export function mapSourceField(row: Record<string, string>, aliases: string[]): string {
  for (const alias of aliases) {
    const value = row[alias];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return '';
}

function isMissingIdentityValue(value: string): boolean {
  return UNKNOWN_TOKENS.has(value.trim().toLowerCase());
}

export function hasCanonicalIdentity(record: Pick<NormalizedSignalRecord, 'company_name' | 'person_name' | 'signal_date'>): boolean {
  return !isMissingIdentityValue(record.company_name) && !isMissingIdentityValue(record.person_name) && !isMissingIdentityValue(record.signal_date);
}

export function logNormalizationHealth(sourceName: string, rows: Record<string, string>[], records: NormalizedSignalRecord[]): void {
  const companyNameCount = records.filter((record) => !isMissingIdentityValue(record.company_name)).length;
  const personNameCount = records.filter((record) => !isMissingIdentityValue(record.person_name)).length;
  const signalDateCount = records.filter((record) => !isMissingIdentityValue(record.signal_date)).length;
  const completeIdentityCount = records.filter((record) => hasCanonicalIdentity(record)).length;
  const sampleRecords = records.slice(0, 3).map((record) => ({
    record_id: record.record_id,
    person_name: record.person_name,
    person_last_name: record.person_last_name,
    company_name: record.company_name,
    signal_date: record.signal_date,
    signal_type: record.signal_type,
  }));

  logInfo('source normalization health', {
    sourceName,
    totalRows: rows.length,
    rowsWithCompanyName: companyNameCount,
    rowsWithPersonName: personNameCount,
    rowsWithSignalDate: signalDateCount,
    rowsWithAllRequiredIdentityFields: completeIdentityCount,
    sampleNormalizedRecords: sampleRecords,
  });

  if (!rows.length) return;
  const missingCoreIdentityRatio = 1 - completeIdentityCount / rows.length;
  if (missingCoreIdentityRatio >= CORE_IDENTITY_FAILURE_RATIO) {
    logWarn('source normalization health failed', {
      sourceName,
      totalRows: rows.length,
      rowsWithAllRequiredIdentityFields: completeIdentityCount,
      missingCoreIdentityRatio,
      threshold: CORE_IDENTITY_FAILURE_RATIO,
    });
    throw new Error(`Normalization health check failed for ${sourceName}: missing canonical identity fields on ${(missingCoreIdentityRatio * 100).toFixed(2)}% of rows.`);
  }
}

export function isUnknownIdentityValue(value: string): boolean {
  return isMissingIdentityValue(value);
}
