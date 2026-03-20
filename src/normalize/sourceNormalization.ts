import { NormalizedSignalRecord } from '../types.js';
import { logInfo, logWarn } from '../utils/logging.js';

const UNKNOWN_TOKENS = new Set(['', 'unknown', 'unknown issuer', 'unknown person', 'n/a', 'na', 'null', 'undefined']);

// Fail if >90% of rows are missing any single required identity field.
// This catches total mapping failures (e.g. AFM renames a column) without
// rejecting real data that legitimately has some sparse rows.
const FIELD_MISSING_FAIL_RATIO = 0.9;

function isMissingIdentityValue(value: string): boolean {
  return UNKNOWN_TOKENS.has(value.trim().toLowerCase());
}

export function hasCanonicalIdentity(record: Pick<NormalizedSignalRecord, 'company_name' | 'person_name' | 'signal_date'>): boolean {
  return !isMissingIdentityValue(record.company_name) && !isMissingIdentityValue(record.person_name) && !isMissingIdentityValue(record.signal_date);
}

export function isUnknownIdentityValue(value: string): boolean {
  return isMissingIdentityValue(value);
}

/**
 * Validates that all required columns are present in the first row.
 * Logs unexpected extra columns as informational.
 * Throws immediately if any required column is missing — do not allow silent
 * field-to-empty-string degradation downstream.
 */
export function validateRequiredColumns(
  rows: Record<string, string>[],
  requiredColumns: readonly string[],
  sourceName: string,
): void {
  if (!rows.length) {
    throw new Error(`Source "${sourceName}" returned zero rows — cannot validate schema contract.`);
  }

  const presentColumns = new Set(Object.keys(rows[0]));
  const missingColumns = requiredColumns.filter((col) => !presentColumns.has(col));

  if (missingColumns.length) {
    throw new Error(
      `Source "${sourceName}" schema contract violated: missing required column(s): ${missingColumns.map((c) => JSON.stringify(c)).join(', ')}. ` +
      `Present columns: ${[...presentColumns].map((c) => JSON.stringify(c)).join(', ')}.`,
    );
  }

  const unexpectedColumns = [...presentColumns].filter((col) => !requiredColumns.includes(col));
  if (unexpectedColumns.length) {
    logInfo(`source schema: unexpected extra columns in "${sourceName}"`, { unexpectedColumns });
  }
}

/**
 * mapSourceField is retained for optional/non-required field lookups only.
 * Required fields must be accessed directly after validateRequiredColumns passes.
 */
export function mapSourceField(row: Record<string, string>, aliases: string[]): string {
  for (const alias of aliases) {
    const value = row[alias];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return '';
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

  const companyMissingRatio = 1 - companyNameCount / rows.length;
  const personMissingRatio = 1 - personNameCount / rows.length;
  const dateMissingRatio = 1 - signalDateCount / rows.length;

  const failures: string[] = [];
  if (companyMissingRatio >= FIELD_MISSING_FAIL_RATIO) {
    failures.push(`company_name missing on ${(companyMissingRatio * 100).toFixed(1)}% of rows`);
  }
  if (personMissingRatio >= FIELD_MISSING_FAIL_RATIO) {
    failures.push(`person_name missing on ${(personMissingRatio * 100).toFixed(1)}% of rows`);
  }
  if (dateMissingRatio >= FIELD_MISSING_FAIL_RATIO) {
    failures.push(`signal_date missing on ${(dateMissingRatio * 100).toFixed(1)}% of rows`);
  }

  if (failures.length) {
    logWarn('source normalization health failed', {
      sourceName,
      totalRows: rows.length,
      failures,
      threshold: FIELD_MISSING_FAIL_RATIO,
    });
    throw new Error(`Normalization health check failed for ${sourceName}: ${failures.join('; ')}.`);
  }
}
