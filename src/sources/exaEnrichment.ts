import { ExaEnrichmentResult, NormalizedSignalRecord } from '../types.js';

export async function enrichWithExa(record: NormalizedSignalRecord, apiKey: string): Promise<ExaEnrichmentResult | null> {
  const query = `${record.person_name} ${record.company_name} Netherlands executive founder`;
  const response = await fetch('https://api.exa.ai/search', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': apiKey,
    },
    body: JSON.stringify({
      query,
      numResults: 3,
      type: 'keyword',
      useAutoprompt: false,
    }),
  });

  if (!response.ok) {
    record.notes.push(`Exa enrichment failed with status ${response.status}.`);
    return null;
  }

  const payload = await response.json() as { results?: Array<{ title?: string; url?: string; text?: string }> };
  const first = payload.results?.[0];
  if (!first) return null;
  const companyDomain = first.url ? new URL(first.url).hostname.replace(/^www\./, '') : undefined;
  return {
    role: /founder/i.test(first.title ?? '') ? 'Founder' : /chief|ceo|cfo|director/i.test(first.title ?? '') ? 'Executive' : undefined,
    company_domain: companyDomain,
    company_country: 'Netherlands',
    natural_person_confidence_delta: 0.1,
    contactability_confidence_delta: 0.15,
    notes: ['Exa enrichment added lightweight contextual evidence.'],
    contextSnippet: first.text?.slice(0, 240),
  };
}
