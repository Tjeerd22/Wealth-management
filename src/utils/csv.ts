import { readFile } from 'node:fs/promises';
import { parse } from 'csv-parse/sync';

export async function fetchCsvRows(url: string): Promise<Record<string, string>[]> {
  const isLocalPath = !/^https?:\/\//i.test(url);
  const body = isLocalPath
    ? await readFile(url.replace(/^file:\/\//i, ''), 'utf8')
    : await fetch(url).then(async (response) => {
      if (!response.ok) throw new Error(`Failed to fetch CSV from ${url}: ${response.status}`);
      return response.text();
    });
  return parseCsv(body);
}

export function parseCsv(body: string): Record<string, string>[] {
  return parse(body, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
  });
}
