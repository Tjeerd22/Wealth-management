import { ExaConfirmationCandidate, ExaConfirmationContent, ExaEnrichmentResult, NormalizedSignalRecord } from '../types.js';

const EXA_BASE_URL = 'https://api.exa.ai';

interface ExaSearchRequest {
  query: string;
  numResults: number;
  includeDomains?: string[];
  startPublishedDate?: string;
  endPublishedDate?: string;
  highlights?: { numSentences?: number };
  summary?: { query?: string };
}

interface ExaContentsRequest {
  urls: string[];
  text?: boolean;
  highlights?: { query?: string; numSentences?: number };
  summary?: { query?: string };
  maxAgeHours?: number;
}

interface ExaSearchResponse {
  results?: Array<{
    title?: string;
    url?: string;
    publishedDate?: string;
    summary?: string;
    highlights?: string[];
    score?: number;
    text?: string;
  }>;
}

interface ExaContentsResponse {
  results?: Array<{
    url?: string;
    title?: string;
    publishedDate?: string;
    summary?: string;
    highlights?: string[];
    text?: string;
  }>;
}

async function postExa<TResponse>(path: string, apiKey: string, body: object): Promise<TResponse | null> {
  const response = await fetch(`${EXA_BASE_URL}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': apiKey,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    return null;
  }

  return await response.json() as TResponse;
}

export async function searchExa(request: ExaSearchRequest, apiKey: string): Promise<ExaConfirmationCandidate[]> {
  const payload = await postExa<ExaSearchResponse>('/search', apiKey, {
    query: request.query,
    numResults: request.numResults,
    type: 'keyword',
    useAutoprompt: false,
    includeDomains: request.includeDomains,
    startPublishedDate: request.startPublishedDate,
    endPublishedDate: request.endPublishedDate,
    highlights: request.highlights,
    summary: request.summary,
  });

  return (payload?.results ?? [])
    .filter((result): result is NonNullable<typeof result> & { url: string } => Boolean(result?.url))
    .map((result) => ({
      url: result.url,
      title: result.title ?? '',
      published_date: result.publishedDate,
      summary: result.summary,
      highlights: result.highlights ?? [],
      score: result.score ?? 0,
      text: result.text,
    }));
}

export async function fetchExaContents(request: ExaContentsRequest, apiKey: string): Promise<ExaConfirmationContent[]> {
  if (!request.urls.length) return [];

  const payload = await postExa<ExaContentsResponse>('/contents', apiKey, request);
  return (payload?.results ?? [])
    .filter((result): result is NonNullable<typeof result> & { url: string } => Boolean(result?.url))
    .map((result) => ({
      url: result.url,
      title: result.title ?? '',
      published_date: result.publishedDate,
      summary: result.summary,
      highlights: result.highlights ?? [],
      text: result.text ?? '',
    }));
}

export async function enrichWithExa(record: NormalizedSignalRecord, apiKey: string): Promise<ExaEnrichmentResult | null> {
  const issuerDomain = record.company_domain ? [record.company_domain] : undefined;
  const [first] = await searchExa({
    query: `${record.person_name} ${record.company_name} Netherlands executive founder`,
    numResults: 3,
    includeDomains: issuerDomain,
  }, apiKey);

  if (!first) {
    record.notes.push('Exa enrichment returned no contextual result.');
    return null;
  }

  const companyDomain = first.url ? new URL(first.url).hostname.replace(/^www\./, '') : undefined;
  return {
    role: /founder/i.test(first.title ?? '') ? 'Founder' : /chief|ceo|cfo|director/i.test(first.title ?? '') ? 'Executive' : undefined,
    company_domain: companyDomain,
    company_country: 'Netherlands',
    natural_person_confidence_delta: 0.1,
    contactability_confidence_delta: 0.15,
    notes: ['Exa enrichment added lightweight contextual evidence.'],
    contextSnippet: first.text?.slice(0, 240) ?? first.summary?.slice(0, 240),
  };
}
