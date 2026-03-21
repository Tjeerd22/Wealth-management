import * as cheerio from 'cheerio';
import { logInfo, logWarn } from './logging.js';

export interface AfmHtmlRow {
  signal_date: string;
  company_name: string;
  person_name: string;
}

const DATE_KEYWORDS = ['datum', 'transactie', 'date', 'transaction'];
const COMPANY_KEYWORDS = ['uitgevende', 'instelling', 'issuer', 'company'];
const PERSON_KEYWORDS = ['meldingsplichtige', 'notifier', 'person', 'name'];

function matchesKeywords(header: string, keywords: string[]): boolean {
  const lower = header.toLowerCase();
  return keywords.some((kw) => lower.includes(kw));
}

function findTable($: cheerio.CheerioAPI): cheerio.Cheerio<any> | null {
  // Try common selectors first
  for (const selector of ['table.table', 'table.search-results', 'table[data-table]']) {
    const table = $(selector).first();
    if (table.length && table.find('tr').length > 1) return table;
  }
  // Fall back to first table with data rows
  const tables = $('table');
  for (let i = 0; i < tables.length; i++) {
    const table = tables.eq(i);
    if (table.find('tr').length > 1) return table;
  }
  return null;
}

function extractHeaders(table: cheerio.Cheerio<any>, $: cheerio.CheerioAPI): string[] {
  const theadRow = table.find('thead tr').first();
  if (theadRow.length) {
    return theadRow.find('th').map((_, el) => $(el).text().trim()).get();
  }
  // Fall back to first row
  const firstRow = table.find('tr').first();
  return firstRow.find('th, td').map((_, el) => $(el).text().trim()).get();
}

function mapColumnIndices(headers: string[]): { dateIdx: number; companyIdx: number; personIdx: number } {
  let dateIdx = -1;
  let companyIdx = -1;
  let personIdx = -1;

  for (let i = 0; i < headers.length; i++) {
    if (dateIdx === -1 && matchesKeywords(headers[i], DATE_KEYWORDS)) dateIdx = i;
    else if (companyIdx === -1 && matchesKeywords(headers[i], COMPANY_KEYWORDS)) companyIdx = i;
    else if (personIdx === -1 && matchesKeywords(headers[i], PERSON_KEYWORDS)) personIdx = i;
  }

  return { dateIdx, companyIdx, personIdx };
}

export function parseAfmMar19Table(html: string, debug = false): AfmHtmlRow[] {
  const $ = cheerio.load(html);
  const table = findTable($);

  if (!table) {
    if (debug) logWarn('HTML parser: no table found');
    return [];
  }

  const headers = extractHeaders(table, $);
  if (debug) logInfo('HTML parser: headers found', { headers });

  const { dateIdx, companyIdx, personIdx } = mapColumnIndices(headers);

  if (dateIdx === -1 || companyIdx === -1 || personIdx === -1) {
    logWarn('HTML parser: could not map all required columns', { headers, dateIdx, companyIdx, personIdx });
    return [];
  }

  const rows: AfmHtmlRow[] = [];
  const dataRows = table.find('tbody tr').length
    ? table.find('tbody tr')
    : table.find('tr').slice(1); // skip header row

  dataRows.each((_, el) => {
    const cells = $(el).find('td');
    if (cells.length === 0) return; // skip header-only rows

    const signal_date = cells.eq(dateIdx).text().trim();
    const company_name = cells.eq(companyIdx).text().trim();
    const person_name = cells.eq(personIdx).text().trim();

    if (signal_date || company_name || person_name) {
      rows.push({ signal_date, company_name, person_name });
    }
  });

  if (debug) logInfo('HTML parser: rows extracted', { count: rows.length });
  return rows;
}

export function hasNextPage(html: string, currentPage: number): boolean {
  const $ = cheerio.load(html);
  const nextPage = currentPage + 1;

  // Check for page= links
  const pageLinks = $(`a[href*="page=${nextPage}"]`);
  if (pageLinks.length > 0) return true;

  // Check for "next" / "volgende" link text
  const nextLinks = $('a').filter((_, el) => {
    const text = $(el).text().toLowerCase().trim();
    return text === 'next' || text === 'volgende' || text === '›' || text === '»';
  });
  if (nextLinks.length > 0) return true;

  return false;
}
