import { readFile } from 'node:fs/promises';
import { parse } from 'csv-parse/sync';
import { logInfo } from './logging.js';

const CSV_PREVIEW_LENGTH = 200;
const DELIMITER_CANDIDATES = [';', ','] as const;

type CsvDelimiter = (typeof DELIMITER_CANDIDATES)[number];

interface ParseCsvOptions {
  sourceName?: string;
}

function getRawPreview(body: string): string {
  return body.slice(0, CSV_PREVIEW_LENGTH).replace(/\r/g, '\\r').replace(/\n/g, '\\n');
}

function getFirstLine(body: string): string {
  const [firstLine = ''] = body.replace(/^\uFEFF/, '').split(/\r?\n/, 1);
  return firstLine;
}

function countDelimiter(line: string, delimiter: CsvDelimiter): number {
  return Array.from(line).filter((char) => char === delimiter).length;
}

function looksLikeWrongDelimiter(body: string, delimiter: CsvDelimiter, rows: Record<string, string>[]): boolean {
  const firstLine = getFirstLine(body);
  if (!firstLine) return false;
  const headerCount = Object.keys(rows[0] ?? {}).length;
  if (headerCount > 1) return false;
  const currentCount = countDelimiter(firstLine, delimiter);
  const alternate = delimiter === ';' ? ',' : ';';
  const alternateCount = countDelimiter(firstLine, alternate);
  return alternateCount > currentCount;
}

function parseWithDelimiter(body: string, delimiter: CsvDelimiter): Record<string, string>[] {
  return parse(body, {
    columns: true,
    delimiter,
    bom: true,
    skip_empty_lines: true,
    trim: true,
    ltrim: true,
    rtrim: true,
    relax_quotes: true,
    relax_column_count: true,
  });
}

export async function fetchCsvRows(url: string, options: ParseCsvOptions = {}): Promise<Record<string, string>[]> {
  const sourceName = options.sourceName ?? url;
  const isLocalPath = !/^https?:\/\//i.test(url);
  const body = isLocalPath
    ? await readFile(url.replace(/^file:\/\//i, ''), 'utf8')
    : await fetch(url).then(async (response) => {
      if (!response.ok) throw new Error(`Failed to fetch CSV from ${url}: ${response.status}`);
      return response.text();
    });

  logInfo('CSV fetch completed', {
    source: sourceName,
    bytes: Buffer.byteLength(body, 'utf8'),
    preview: getRawPreview(body),
  });

  return parseCsv(body, { sourceName });
}

export function parseCsv(body: string, options: ParseCsvOptions = {}): Record<string, string>[] {
  const sourceName = options.sourceName ?? 'csv source';
  const firstLine = getFirstLine(body);
  const attempts: Array<{ delimiter: CsvDelimiter; reason: string }> = [];

  for (const delimiter of DELIMITER_CANDIDATES) {
    try {
      const rows = parseWithDelimiter(body, delimiter);
      if (looksLikeWrongDelimiter(body, delimiter, rows)) {
        attempts.push({ delimiter, reason: 'parsed into a single header while the first line favored another delimiter' });
        continue;
      }
      logInfo('CSV parse succeeded', {
        source: sourceName,
        delimiter,
        rows: rows.length,
      });
      return rows;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      attempts.push({ delimiter, reason: message });
    }
  }

  const attemptedDelimiters = attempts.map(({ delimiter }) => delimiter).join(', ');
  const attemptDetails = attempts.map(({ delimiter, reason }) => `${delimiter}: ${reason}`).join(' | ');
  throw new Error(
    `Failed to parse CSV for source "${sourceName}" after trying delimiter(s) ${attemptedDelimiters}. First line: "${firstLine}". Details: ${attemptDetails}`,
  );
}
