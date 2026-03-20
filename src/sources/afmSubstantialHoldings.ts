import { DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth, mapSourceField, validateRequiredColumns } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchWithRetry, parseCsv } from '../utils/csv.js';
import { isWithinLookback, toIsoDate } from '../utils/dates.js';
import { logInfo } from '../utils/logging.js';
import { normalizeCompanyName, normalizeName } from '../utils/strings.js';

/**
 * Hard schema contract for AFM substantial holdings (Meldingen substantiële deelnemingen).
 * These are the exact Dutch column names from the AFM export endpoint.
 * If any of these are absent, the run fails immediately.
 */
export const AFM_SUBSTANTIAL_REQUIRED_COLUMNS = [
  'Datum meldingsplicht',
  'Uitgevende instelling',
  'Meldingsplichtige',
  'Kvk-nr',
  'Plaats',
] as const;

// Substantial holdings is a large bulk government file (~95 MB, 250k+ rows).
// It uses a dedicated retry-capable fetch path.
const SUBSTANTIAL_MAX_RETRIES = 2;

// Optional capital-interest columns — not required, present in some exports.
const CAPITAL_BEFORE_ALIASES = ['CapitalInterestBefore', 'capital_interest_before', 'Before'];
const CAPITAL_AFTER_ALIASES = ['CapitalInterestAfter', 'capital_interest_after', 'After'];

type CsvRow = Record<string, string>;

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const normalized = value.replace(',', '.').replace('%', '').trim();
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

// Footer markers present in real AFM bulk exports (case-insensitive).
const FOOTER_MARKERS = ['disclaimer csv', 'datum laatste update'];

/**
 * Remove non-data rows appended by the AFM export endpoint.
 * Real exports include rows like "Disclaimer CSV" and "Datum laatste update 20 Mar 2026"
 * which appear after the last data row. These rows have the required column keys (same CSV
 * structure) but carry no usable field values and must not enter normalization.
 */
function stripFooterRows(rows: CsvRow[]): { rows: CsvRow[]; dropped: number } {
  let dropped = 0;
  const clean = rows.filter((row) => {
    const allValues = Object.values(row).map((v) => (v ?? '').toLowerCase().trim());
    if (FOOTER_MARKERS.some((marker) => allValues.some((v) => v.includes(marker)))) {
      dropped += 1;
      return false;
    }
    // Also drop rows where all three identity fields are blank (orphan blank rows).
    const date = (row['Datum meldingsplicht'] ?? '').trim();
    const notifier = (row['Meldingsplichtige'] ?? '').trim();
    const issuer = (row['Uitgevende instelling'] ?? '').trim();
    if (!date && !notifier && !issuer) {
      dropped += 1;
      return false;
    }
    return true;
  });
  return { rows: clean, dropped };
}

/**
 * Collapse AFM substantial holdings rows into one record per filing.
 *
 * The AFM export produces multiple rows per filing notification: one row per
 * reported metric (capital interest, voting rights, direct/indirect, controlled
 * entities). The notifier+issuer+date triple uniquely identifies a single filing.
 *
 * Aggregation rules per filing group:
 *   - CapitalInterestBefore = max across group (highest reported prior stake)
 *   - CapitalInterestAfter  = min across group (lowest reported final stake)
 *   - All other fields taken from the first row of the group.
 *   - `_filing_component_count` is set to the number of source rows collapsed.
 */
function collapseFilingRows(rows: CsvRow[]): CsvRow[] {
  const groups = new Map<string, {
    canonical: CsvRow;
    beforeValues: number[];
    afterValues: number[];
    componentCount: number;
  }>();

  for (const row of rows) {
    const notifier = normalizeName(row['Meldingsplichtige'] ?? '');
    const issuer = normalizeCompanyName(row['Uitgevende instelling'] ?? '');
    const date = (row['Datum meldingsplicht'] ?? '').trim();
    const key = `${notifier}|${issuer}|${date}`;

    const before = toNumber(mapSourceField(row, CAPITAL_BEFORE_ALIASES));
    const after = toNumber(mapSourceField(row, CAPITAL_AFTER_ALIASES));
    const existing = groups.get(key);

    if (existing) {
      if (before !== null) existing.beforeValues.push(before);
      if (after !== null) existing.afterValues.push(after);
      existing.componentCount += 1;
    } else {
      groups.set(key, {
        canonical: row,
        beforeValues: before !== null ? [before] : [],
        afterValues: after !== null ? [after] : [],
        componentCount: 1,
      });
    }
  }

  return [...groups.values()].map(({ canonical, beforeValues, afterValues, componentCount }) => {
    const aggregated: CsvRow = { ...canonical, _filing_component_count: String(componentCount) };
    if (componentCount > 1) {
      if (beforeValues.length > 0) {
        const key = CAPITAL_BEFORE_ALIASES.find((k) => k in canonical) ?? CAPITAL_BEFORE_ALIASES[0];
        aggregated[key] = String(Math.max(...beforeValues));
      }
      if (afterValues.length > 0) {
        const key = CAPITAL_AFTER_ALIASES.find((k) => k in canonical) ?? CAPITAL_AFTER_ALIASES[0];
        aggregated[key] = String(Math.min(...afterValues));
      }
    }
    return aggregated;
  });
}

/**
 * Ingest AFM substantial holdings.
 *
 * @param lookbackDays - rows outside this window are dropped before normalization.
 *   Filtering early keeps memory bounded on the 250k+ row file.
 * @param url - override for the source URL (used in tests and config).
 */
export async function ingestAfmSubstantialHoldings(
  url = DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL,
  lookbackDays = 45,
): Promise<NormalizedSignalRecord[]> {
  const isLocalPath = !/^https?:\/\//i.test(url);
  let body: string;

  if (isLocalPath) {
    const { readFile } = await import('node:fs/promises');
    body = await readFile(url.replace(/^file:\/\//i, ''), 'utf8');
  } else {
    // Retry on 5xx/timeout — AFM substantial holdings endpoint intermittently returns 504.
    body = await fetchWithRetry(url, SUBSTANTIAL_MAX_RETRIES, 'AFM substantial holdings');
  }

  const allRows = parseCsv(body, { sourceName: 'AFM substantial holdings' });

  // Hard fail if required columns are absent.
  validateRequiredColumns(allRows, AFM_SUBSTANTIAL_REQUIRED_COLUMNS, 'AFM substantial holdings');

  // --- Strip non-data footer rows ---
  // Real AFM bulk exports append "Disclaimer CSV" and "Datum laatste update" rows
  // after the last data row. These must be removed before normalization.
  const { rows: cleanRows, dropped: droppedFooters } = stripFooterRows(allRows);
  if (droppedFooters > 0) {
    logInfo('AFM substantial holdings footer rows stripped', { droppedFooters });
  }

  // --- Early lookback filter ---
  // Drop rows outside the lookback window before normalization. This keeps
  // memory bounded for the ~95 MB / 250k+ row file.
  const now = new Date();
  let droppedStale = 0;
  const lookbackRows = cleanRows.filter((row) => {
    const dateRaw = (row['Datum meldingsplicht'] ?? '').trim();
    if (!dateRaw) return true;
    const isoDate = toIsoDate(dateRaw);
    if (!isoDate) return true;
    const within = isWithinLookback(isoDate, lookbackDays, now);
    if (!within) droppedStale += 1;
    return within;
  });

  logInfo('AFM substantial holdings lookback filter', {
    totalRows: allRows.length,
    rowsAfterFooterStrip: cleanRows.length,
    rowsAfterLookback: lookbackRows.length,
    droppedStaleRows: droppedStale,
    lookbackDays,
  });

  // --- Filing collapse ---
  // One AFM filing = multiple CSV rows (capital interest, voting rights, direct/indirect,
  // controlled entities). Collapse to one record per notifier+issuer+date before normalization.
  const rows = collapseFilingRows(lookbackRows);
  if (rows.length < lookbackRows.length) {
    logInfo('AFM substantial holdings filing collapse', {
      rowsBeforeCollapse: lookbackRows.length,
      rowsAfterCollapse: rows.length,
      componentRowsRemoved: lookbackRows.length - rows.length,
    });
  }

  const records = rows.map((row) => {
    // Direct column access for required fields.
    const signalDateRaw = row['Datum meldingsplicht'] ?? '';
    const companyName = row['Uitgevende instelling'] ?? '';
    const personName = row['Meldingsplichtige'] ?? '';
    const kvkNumber = row['Kvk-nr'] ?? '';
    const city = row['Plaats'] ?? '';
    const filingComponents = row['_filing_component_count'] ?? '1';

    // Optional fields — keep alias fallback since column names vary across AFM export versions.
    const before = toNumber(mapSourceField(row, CAPITAL_BEFORE_ALIASES));
    const after = toNumber(mapSourceField(row, CAPITAL_AFTER_ALIASES));
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
      rawSummary: [
        `datum_meldingsplicht=${signalDateRaw}`,
        `uitgevende_instelling=${companyName}`,
        `meldingsplichtige=${personName}`,
        `kvk_nr=${kvkNumber}`,
        `plaats=${city}`,
        `kapitaalbelang_voor=${before}`,
        `kapitaalbelang_na=${after}`,
        `filing_components=${filingComponents}`,
      ].join('; '),
      notes: [reduction ? 'Reduction appears explicit in AFM holdings export.' : 'Holding change direction unclear; preserve for review only.'],
      personType: 'unknown',
      capitalInterestBefore: before,
      capitalInterestAfter: after,
    });
  });

  logNormalizationHealth('afm_substantial', records);
  return records;
}
