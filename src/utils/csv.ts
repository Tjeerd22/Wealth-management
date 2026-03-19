import { parse } from 'csv-parse/sync';

export async function fetchCsvRows(url: string): Promise<Record<string, string>[]> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch CSV from ${url}: ${response.status}`);
  const body = await response.text();
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
