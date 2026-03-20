import { NormalizedSignalRecord } from '../types.js';
import { logInfo, logWarn } from '../utils/logging.js';

const UNKNOWN_TOKENS = new Set(['', 'unknown', 'unknown issuer', 'unknown person', 'n/a', 'na', 'null', 'undefined']);
const CORE_IDENTITY_FAILURE_RATIO = 0.9;

export interface SourceSchemaContract {
  sourceName: string;
  requiredColumns: string[];
}

export interface SchemaValidationResult {
  columns: string[];
  missingColumns: string[];
  unexpectedColumns: string[];
}

export interface SourceNormalizationHealth {
  totalRows: number;
  rowsWithCompanyName: number;
  rowsWithPersonName: number;
  rowsWithSignalDate: number;
  rowsWithAllRequiredIdentityFields: number;
  missingCompanyOrPersonRatio: number;
  sampleNormalizedRecords: Array<{
    record_id: string;
    person_name: string;
    person_last_name: string;
    company_name: string;
    signal_date: string;
    signal_type: string;
  }>;
}

export function validateSourceSchema(rows: Record<string, string>[], contract: SourceSchemaContract): SchemaValidationResult {
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const missingColumns = contract.requiredColumns.filter((column) => !columns.includes(column));
  const unexpectedColumns = columns.filter((column) => !contract.requiredColumns.includes(column));

  logInfo('source schema validated', {
    sourceName: contract.sourceName,
    columns,
    missingColumns,
    unexpectedColumns,
  });

  if (missingColumns.length > 0) {
    throw new Error(`${contract.sourceName} is missing required columns: ${missingColumns.join(', ')}.`);
  }

  return { columns, missingColumns, unexpectedColumns };
}

export function mapRequiredSourceField(row: Record<string, string>, column: string): string {
  const value = row[column];
  return typeof value === 'string' ? value.trim() : '';
}

function isMissingIdentityValue(value: string): boolean {
  return UNKNOWN_TOKENS.has(value.trim().toLowerCase());
}

export function hasCanonicalIdentity(record: Pick<NormalizedSignalRecord, 'company_name' | 'person_name' | 'signal_date'>): boolean {
  return !isMissingIdentityValue(record.company_name) && !isMissingIdentityValue(record.person_name) && !isMissingIdentityValue(record.signal_date);
}

export function getNormalizationHealth(records: NormalizedSignalRecord[]): SourceNormalizationHealth {
  const rowsWithCompanyName = records.filter((record) => !isMissingIdentityValue(record.company_name)).length;
  const rowsWithPersonName = records.filter((record) => !isMissingIdentityValue(record.person_name)).length;
  const rowsWithSignalDate = records.filter((record) => !isMissingIdentityValue(record.signal_date)).length;
  const rowsWithAllRequiredIdentityFields = records.filter((record) => hasCanonicalIdentity(record)).length;
  const sampleNormalizedRecords = records.slice(0, 3).map((record) => ({
    record_id: record.record_id,
    person_name: record.person_name,
    person_last_name: record.person_last_name,
    company_name: record.company_name,
    signal_date: record.signal_date,
    signal_type: record.signal_type,
  }));

  return {
    totalRows: records.length,
    rowsWithCompanyName,
    rowsWithPersonName,
    rowsWithSignalDate,
    rowsWithAllRequiredIdentityFields,
    missingCompanyOrPersonRatio: records.length ? 1 - (records.filter((record) => !isMissingIdentityValue(record.company_name) && !isMissingIdentityValue(record.person_name)).length / records.length) : 0,
    sampleNormalizedRecords,
  };
}

export function logNormalizationHealth(sourceName: string, records: NormalizedSignalRecord[]): SourceNormalizationHealth {
  const health = getNormalizationHealth(records);

  logInfo('source normalization health', {
    sourceName,
    ...health,
  });

  if (!health.totalRows) return health;
  if (health.missingCompanyOrPersonRatio > CORE_IDENTITY_FAILURE_RATIO) {
    logWarn('source normalization health failed', {
      sourceName,
      missingCompanyOrPersonRatio: health.missingCompanyOrPersonRatio,
      threshold: CORE_IDENTITY_FAILURE_RATIO,
      rowsWithCompanyName: health.rowsWithCompanyName,
      rowsWithPersonName: health.rowsWithPersonName,
    });
    throw new Error(`Normalization health check failed for ${sourceName}: company_name or person_name missing on ${(health.missingCompanyOrPersonRatio * 100).toFixed(2)}% of rows.`);
  }

  return health;
}

export function isUnknownIdentityValue(value: string): boolean {
  return isMissingIdentityValue(value);
}
