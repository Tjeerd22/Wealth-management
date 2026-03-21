import { describe, expect, it, vi, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';
import { parseAfmMar19Table, hasNextPage } from '../../src/utils/html.js';

vi.mock('apify', () => ({
  Actor: { setValue: vi.fn(async () => undefined), init: vi.fn(), exit: vi.fn(), getInput: vi.fn(async () => null) },
  Dataset: { pushData: vi.fn(async () => undefined), open: vi.fn(async () => ({ pushData: vi.fn(async () => undefined) })) },
}));

const page1Html = readFileSync(new URL('../fixtures/afm_mar19_page1.html', import.meta.url), 'utf8');
const page2Html = readFileSync(new URL('../fixtures/afm_mar19_page2.html', import.meta.url), 'utf8');
const emptyHtml = readFileSync(new URL('../fixtures/afm_mar19_empty.html', import.meta.url), 'utf8');

describe('HTML parser', () => {
  it('parses AFM MAR 19 HTML table from fixture', () => {
    const rows = parseAfmMar19Table(page1Html);
    expect(rows).toHaveLength(3);
    expect(rows[0].signal_date).toBe('2026-03-19');
    expect(rows[0].company_name).toBe('Universal Music Group N.V.');
    expect(rows[0].person_name).toBe('Jansen, Eva');
    expect(rows[1].company_name).toBe('Adyen NV');
    expect(rows[2].company_name).toBe('ASML Holding NV');
  });

  it('returns empty array for a page with zero data rows', () => {
    const rows = parseAfmMar19Table(emptyHtml);
    expect(rows).toHaveLength(0);
  });

  it('returns empty array for HTML without any table', () => {
    const rows = parseAfmMar19Table('<html><body><p>No table here</p></body></html>');
    expect(rows).toHaveLength(0);
  });

  it('detects next page when pagination links exist', () => {
    expect(hasNextPage(page1Html, 1)).toBe(true);
  });

  it('detects last page when no next page link exists', () => {
    expect(hasNextPage(page2Html, 2)).toBe(false);
  });

  it('parses page 2 fixture correctly', () => {
    const rows = parseAfmMar19Table(page2Html);
    expect(rows).toHaveLength(2);
    expect(rows[0].company_name).toBe('Philips NV');
    expect(rows[1].person_name).toBe('Visser, Maria');
  });

  it('handles debug mode without errors', () => {
    const rows = parseAfmMar19Table(page1Html, true);
    expect(rows).toHaveLength(3);
  });
});

describe('HTML scraper integration', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('ingestAfmMar19Html fetches multiple pages and stops on empty', async () => {
    // Mock fetchHtml to return fixture data
    const { ingestAfmMar19Html } = await import('../../src/sources/afmMar19Html.js');
    const fetchHtmlModule = await import('../../src/utils/http.js');

    let callCount = 0;
    vi.spyOn(fetchHtmlModule, 'fetchHtml').mockImplementation(async () => {
      callCount++;
      if (callCount === 1) return page1Html;
      if (callCount === 2) return page2Html;
      return emptyHtml;
    });

    const records = await ingestAfmMar19Html({ dateFrom: '19-02-2026', maxPages: 10 });
    // page1 = 3 rows, page2 = 2 rows (no next page detected on page2)
    expect(records).toHaveLength(5);
    expect(records[0].page_number).toBe(1);
    expect(records[3].page_number).toBe(2);
    expect(records[0].source_url).toContain('page=1');
    expect(records[3].source_url).toContain('page=2');
  });

  it('respects maxPages limit', async () => {
    const { ingestAfmMar19Html } = await import('../../src/sources/afmMar19Html.js');
    const fetchHtmlModule = await import('../../src/utils/http.js');

    vi.spyOn(fetchHtmlModule, 'fetchHtml').mockImplementation(async () => page1Html);

    const records = await ingestAfmMar19Html({ dateFrom: '19-02-2026', maxPages: 1 });
    expect(records).toHaveLength(3);
  });

  it('throws when page 1 fails', async () => {
    const { ingestAfmMar19Html } = await import('../../src/sources/afmMar19Html.js');
    const fetchHtmlModule = await import('../../src/utils/http.js');

    vi.spyOn(fetchHtmlModule, 'fetchHtml').mockRejectedValue(new Error('HTTP 403'));

    await expect(ingestAfmMar19Html({ dateFrom: '19-02-2026', maxPages: 10 }))
      .rejects.toThrow(/page 1 failed/);
  });

  it('returns partial results when page N>1 fails', async () => {
    const { ingestAfmMar19Html } = await import('../../src/sources/afmMar19Html.js');
    const fetchHtmlModule = await import('../../src/utils/http.js');

    let callCount = 0;
    vi.spyOn(fetchHtmlModule, 'fetchHtml').mockImplementation(async () => {
      callCount++;
      if (callCount === 1) return page1Html;
      throw new Error('HTTP 500');
    });

    const records = await ingestAfmMar19Html({ dateFrom: '19-02-2026', maxPages: 10 });
    expect(records).toHaveLength(3); // only page 1 records
    expect(records[0].page_number).toBe(1);
  });

  it('sets source_name to afm_mar19_html', async () => {
    const { ingestAfmMar19Html } = await import('../../src/sources/afmMar19Html.js');
    const fetchHtmlModule = await import('../../src/utils/http.js');

    vi.spyOn(fetchHtmlModule, 'fetchHtml').mockImplementation(async () => page2Html);

    const records = await ingestAfmMar19Html({ dateFrom: '19-02-2026', maxPages: 1 });
    expect(records[0].source_name).toBe('afm_mar19_html');
    expect(records[0].evidence_type).toBe('afm_html_filing');
  });
});
