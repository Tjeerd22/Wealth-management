import { readFile } from 'node:fs/promises';
import { parse } from 'csv-parse/sync';
import { logInfo, logWarn } from './logging.js';

const CSV_PREVIEW_LENGTH = 200;
const DELIMITER_CANDIDATES = [';', ','] as const;

// 45-second timeout per attempt. AFM endpoints are slow but not indefinitely slow.
const FETCH_TIMEOUT_MS = 45_000;
// 256 MB hard cap. The substantial holdings file is ~95 MB; anything larger is anomalous.
const FETCH_MAX_BYTES = 256 * 1024 * 1024;

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

async function fetchBodyWithTimeout(url: string): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      const error = new Error(`HTTP ${response.status} fetching ${url}`);
      (error as NodeJS.ErrnoException).code = String(response.status);
      throw error;
    }
    // Stream body with size guard to avoid unbounded memory use.
    const reader = response.body?.getReader();
    if (!reader) {
      return response.text();
    }
    const chunks: Uint8Array[] = [];
    let totalBytes = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > FETCH_MAX_BYTES) {
        reader.cancel();
        throw new Error(`Response from ${url} exceeds max body size of ${FETCH_MAX_BYTES} bytes — aborting.`);
      }
      chunks.push(value);
    }
    return Buffer.concat(chunks).toString('utf8');
  } finally {
    clearTimeout(timer);
  }
}

/**
 * fetchWithRetry wraps fetchBodyWithTimeout with exponential-backoff retries.
 * Only retries on 5xx status codes and network/timeout errors.
 * 4xx errors (404, 403) are not retried — they indicate a configuration problem.
 *
 * @param maxRetries number of additional attempts after the first (0 = no retry)
 */
export async function fetchWithRetry(url: string, maxRetries: number, sourceName: string): Promise<string> {
  let lastError: Error = new Error(`fetchWithRetry: no attempt made for ${sourceName}`);
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (attempt > 0) {
      const baseDelayMs = Math.pow(2, attempt) * 1000;
      const jitterMs = Math.floor(Math.random() * 500);
      const delayMs = baseDelayMs + jitterMs;
      logWarn(`source fetch retry`, { sourceName, attempt, delayMs, previousError: lastError.message });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    try {
      return await fetchBodyWithTimeout(url);
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      const statusCode = Number((error as NodeJS.ErrnoException).code ?? 0);
      // Do not retry on 4xx — these are configuration/auth errors not transient.
      if (statusCode >= 400 && statusCode < 500) throw lastError;
    }
  }
  throw lastError;
}

export async function fetchCsvRows(url: string, options: ParseCsvOptions = {}): Promise<Record<string, string>[]> {
  const sourceName = options.sourceName ?? url;
  const isLocalPath = !/^https?:\/\//i.test(url);
  const body = isLocalPath
    ? await readFile(url.replace(/^file:\/\//i, ''), 'utf8')
    : await fetchBodyWithTimeout(url);

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
