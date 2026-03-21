import { AFM_MAR19_HTML_BASE_URL } from '../config.js';
import { normalizeRecord } from '../normalize/normalizeRecord.js';
import { logNormalizationHealth } from '../normalize/sourceNormalization.js';
import { NormalizedSignalRecord } from '../types.js';
import { fetchHtml } from '../utils/http.js';
import { parseAfmMar19Table, hasNextPage } from '../utils/html.js';
import { logInfo, logWarn } from '../utils/logging.js';

export interface AfmHtmlIngestOptions {
  dateFrom: string;
  maxPages: number;
  baseUrl?: string;
  debug?: boolean;
}

export async function ingestAfmMar19Html(options: AfmHtmlIngestOptions): Promise<NormalizedSignalRecord[]> {
  const baseUrl = options.baseUrl ?? AFM_MAR19_HTML_BASE_URL;
  const allRecords: NormalizedSignalRecord[] = [];
  let pagesFetched = 0;

  for (let page = 1; page <= options.maxPages; page++) {
    const url = `${baseUrl}?DateFrom=${options.dateFrom}&page=${page}`;
    logInfo('AFM MAR 19 HTML page fetch', { page, url });

    let html: string;
    try {
      html = await fetchHtml(url);
    } catch (error) {
      if (page === 1) {
        // Page 1 failure is fatal
        throw new Error(`AFM MAR 19 HTML page 1 failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      // Page N>1 failure: return what we have
      logWarn('AFM MAR 19 HTML page fetch failed — returning partial results', {
        page,
        error: error instanceof Error ? error.message : String(error),
        recordsCollected: allRecords.length,
      });
      break;
    }

    pagesFetched++;
    const rows = parseAfmMar19Table(html, options.debug);

    if (rows.length === 0) {
      logInfo('AFM MAR 19 HTML page returned zero rows — stopping pagination', { page });
      break;
    }

    for (const row of rows) {
      const record = normalizeRecord({
        personName: row.person_name,
        personLastName: '',
        companyName: row.company_name,
        signalDate: row.signal_date,
        signalType: 'pdmr_transaction_unconfirmed',
        signalDetail: `AFM MAR 19 filing for ${row.company_name || 'unmapped issuer'}; scraped from HTML register.`,
        sourceName: 'afm_mar19_html',
        sourceRole: 'primary',
        sourceUrl: url,
        signalDirection: 'unclear',
        signalClarity: 'inferred',
        liquidityRelevance: 0.5,
        evidenceType: 'afm_html_filing',
        evidenceStrength: 0.66,
        rawSummary: `signal_date=${row.signal_date}; company=${row.company_name}; person=${row.person_name}; page=${page}`,
        notes: ['MAR 19 HTML is timing-strong but thin; disposal not confirmed from register page alone.'],
        personType: 'unknown',
      });
      record.page_number = page;
      allRecords.push(record);
    }

    logInfo('AFM MAR 19 HTML page parsed', { page, rowsOnPage: rows.length, totalRecords: allRecords.length });

    if (!hasNextPage(html, page)) {
      logInfo('AFM MAR 19 HTML no next page detected — stopping', { page });
      break;
    }
  }

  logInfo('AFM MAR 19 HTML ingestion complete', { pagesFetched, totalRecords: allRecords.length });
  logNormalizationHealth('afm_mar19_html', allRecords);
  return allRecords;
}
