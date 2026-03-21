import { logInfo, logWarn } from './logging.js';

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RETRIES = 2;
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

export interface FetchHtmlOptions {
  timeoutMs?: number;
  maxRetries?: number;
  userAgent?: string;
  headers?: Record<string, string>;
}

async function fetchOnce(url: string, timeoutMs: number, headers: Record<string, string>): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers,
    });
    if (!response.ok) {
      const error = new Error(`HTTP ${response.status} fetching ${url}`);
      (error as NodeJS.ErrnoException).code = String(response.status);
      throw error;
    }
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchHtml(url: string, options: FetchHtmlOptions = {}): Promise<string> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
  const headers: Record<string, string> = {
    'User-Agent': options.userAgent ?? DEFAULT_USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,nl;q=0.8',
    ...options.headers,
  };

  let lastError: Error = new Error(`fetchHtml: no attempt made for ${url}`);
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (attempt > 0) {
      const delayMs = Math.pow(2, attempt) * 1000 + Math.floor(Math.random() * 500);
      logWarn('HTML fetch retry', { url, attempt, delayMs, previousError: lastError.message });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    try {
      logInfo('Fetching HTML', { url, attempt });
      const html = await fetchOnce(url, timeoutMs, headers);
      logInfo('HTML fetch completed', { url, bytes: Buffer.byteLength(html, 'utf8') });
      return html;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      const statusCode = Number((error as NodeJS.ErrnoException).code ?? 0);
      // Do not retry on 4xx — these are configuration/auth errors not transient.
      if (statusCode >= 400 && statusCode < 500) {
        logWarn('HTML fetch non-retryable error', { url, status: statusCode, message: lastError.message });
        throw lastError;
      }
    }
  }
  throw lastError;
}
