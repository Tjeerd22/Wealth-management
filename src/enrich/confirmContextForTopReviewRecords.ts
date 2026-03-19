import { ActorInput, ConfirmationEvidenceStrength, ConfirmationResult, ConfirmationSource, ExaConfirmationCandidate, NormalizedSignalRecord } from '../types.js';
import { fetchExaContents, searchExa } from '../sources/exaEnrichment.js';
import { parseDate } from '../utils/dates.js';

function shiftDate(date: string, days: number): string | undefined {
  const parsed = parseDate(date);
  if (!parsed) return undefined;
  const shifted = new Date(parsed);
  shifted.setUTCDate(shifted.getUTCDate() + days);
  return shifted.toISOString();
}

function getKnownDomains(record: NormalizedSignalRecord): string[] {
  const domains = new Set<string>();
  if (record.company_domain) domains.add(record.company_domain.replace(/^www\./, ''));
  domains.add('euronext.com');
  if (/\bafm\b/i.test(record.source_name) || /afm\.nl/.test(record.source_url)) domains.add('afm.nl');
  return [...domains].filter(Boolean);
}

function buildSearchPasses(record: NormalizedSignalRecord): Array<{ source_type: 'issuer' | 'news'; query: string; includeDomains?: string[] }> {
  const signalDate = record.signal_date;
  const personCompany = `${record.person_name} ${record.company_name}`.trim();
  return [
    {
      source_type: 'issuer',
      query: `${personCompany} ${signalDate} board executive biography investor relations`,
      includeDomains: getKnownDomains(record),
    },
    {
      source_type: 'news',
      query: `${personCompany} ${signalDate} disposal share sale insider transaction news`,
    },
  ];
}

function scoreCandidate(candidate: ExaConfirmationCandidate, sourceType: 'issuer' | 'news', record: NormalizedSignalRecord): number {
  const haystack = `${candidate.title} ${candidate.summary ?? ''} ${(candidate.highlights ?? []).join(' ')} ${candidate.text ?? ''}`.toLowerCase();
  let score = candidate.score || 0;
  if (haystack.includes(record.person_last_name)) score += 1.1;
  if (haystack.includes(record.company_name.toLowerCase())) score += 1.1;
  if (record.role && haystack.includes(record.role.toLowerCase())) score += 0.7;
  if (sourceType === 'issuer') score += /investor|board|management|leadership|governance/.test(haystack) ? 0.8 : 0;
  if (sourceType === 'news') score += /disposal|sold|sale|disposed|transaction/.test(haystack) ? 0.8 : 0;
  return score;
}

function summarizeEvidence(sources: ConfirmationSource[], record: NormalizedSignalRecord): ConfirmationResult {
  const joined = sources.flatMap((source) => [source.title, source.summary ?? '', ...source.highlights]).join(' ').toLowerCase();
  const contextConfirmed = sources.some((source) => source.source_type === 'issuer')
    && (joined.includes(record.person_last_name) || joined.includes(record.company_name.toLowerCase()));
  const roleConfirmed = Boolean(record.role)
    ? joined.includes(record.role.toLowerCase()) || /ceo|cfo|director|board|executive|founder/.test(joined)
    : /ceo|cfo|director|board|executive|founder/.test(joined);
  const disposalConfirmed = /disposal|disposed|sale|sold|selldown|share sale|transaction/.test(joined);

  const evidenceStrength: ConfirmationEvidenceStrength = sources.length === 0
    ? 'none'
    : (contextConfirmed && roleConfirmed) || (contextConfirmed && disposalConfirmed)
      ? sources.length >= 2 ? 'strong' : 'moderate'
      : sources.length >= 2
        ? 'moderate'
        : 'weak';

  const summaryBits = [
    contextConfirmed ? 'Issuer/news context aligns with AFM record.' : 'Exa context remains weak and non-decisive.',
    roleConfirmed ? 'Role context appears corroborated.' : 'Role context not clearly corroborated.',
    disposalConfirmed ? 'Coverage mentions disposal-like transaction language.' : 'No clear disposal confirmation found.',
  ];

  const reviewActionUpdated = evidenceStrength === 'none' || !contextConfirmed
    ? record.review_action
    : record.review_action === 'manual_context_check'
      ? 'watchlist_only'
      : record.review_action;

  return {
    context_confirmed: contextConfirmed,
    disposal_confirmed: disposalConfirmed,
    role_confirmed: roleConfirmed,
    confirmation_urls: sources.map((source) => source.url),
    confirmation_sources: sources,
    confirmation_summary: summaryBits.join(' '),
    confirmation_evidence_strength: evidenceStrength,
    review_action_updated: reviewActionUpdated,
  };
}

function defaultConfirmation(record: NormalizedSignalRecord, reason: string): ConfirmationResult {
  if (!record.notes.includes(reason)) record.notes.push(reason);
  return {
    context_confirmed: false,
    disposal_confirmed: false,
    role_confirmed: false,
    confirmation_urls: [],
    confirmation_sources: [],
    confirmation_summary: reason,
    confirmation_evidence_strength: 'none',
    review_action_updated: record.review_action,
  };
}

function applyConfirmation(record: NormalizedSignalRecord, confirmation: ConfirmationResult): void {
  record.context_confirmed = confirmation.context_confirmed;
  record.disposal_confirmed = confirmation.disposal_confirmed;
  record.role_confirmed = confirmation.role_confirmed;
  record.confirmation_urls = confirmation.confirmation_urls;
  record.confirmation_sources = confirmation.confirmation_sources;
  record.confirmation_summary = confirmation.confirmation_summary;
  record.confirmation_evidence_strength = confirmation.confirmation_evidence_strength;
  record.review_action_updated = confirmation.review_action_updated;
}

export async function confirmContextForTopReviewRecords(records: NormalizedSignalRecord[], input: ActorInput): Promise<NormalizedSignalRecord[]> {
  const ranked = [...records];
  const shortlist = ranked.filter((record) => record.review_bucket === 'A');
  const bucketB = ranked.filter((record) => record.review_bucket === 'B').slice(0, input.exaTopReviewConfirmations);
  const targets = [...shortlist, ...bucketB];

  if (!input.runExaEnrichment || !input.exaApiKey) {
    for (const record of targets) applyConfirmation(record, defaultConfirmation(record, 'Exa confirmation skipped because API access was not configured.'));
    return records;
  }

  for (const record of targets) {
    const startPublishedDate = shiftDate(record.signal_date, -14);
    const endPublishedDate = shiftDate(record.signal_date, 14);
    const candidates: Array<ExaConfirmationCandidate & { source_type: 'issuer' | 'news' }> = [];

    for (const pass of buildSearchPasses(record)) {
      const results = await searchExa({
        query: pass.query,
        numResults: 4,
        includeDomains: pass.includeDomains,
        startPublishedDate,
        endPublishedDate,
        highlights: { numSentences: 2 },
        summary: { query: `${record.person_name} ${record.company_name} ${record.signal_date}` },
      }, input.exaApiKey);
      candidates.push(...results.map((result) => ({ ...result, source_type: pass.source_type })));
    }

    const bestCandidates = [...candidates]
      .sort((a, b) => scoreCandidate(b, b.source_type, record) - scoreCandidate(a, a.source_type, record))
      .filter((candidate, index, arr) => arr.findIndex((other) => other.url === candidate.url) === index)
      .slice(0, 3);

    if (!bestCandidates.length) {
      applyConfirmation(record, defaultConfirmation(record, 'Exa confirmation returned no shortlisted URLs.'));
      continue;
    }

    const contents = await fetchExaContents({
      urls: bestCandidates.map((candidate) => candidate.url),
      text: true,
      highlights: { query: `${record.person_name} ${record.company_name} role disposal`, numSentences: 2 },
      summary: { query: `${record.person_name} ${record.company_name} context` },
      maxAgeHours: input.exaFreshnessMaxAgeHours,
    }, input.exaApiKey);

    const sources: ConfirmationSource[] = bestCandidates.map((candidate) => {
      const content = contents.find((item) => item.url === candidate.url);
      const domain = new URL(candidate.url).hostname.replace(/^www\./, '');
      return {
        url: candidate.url,
        title: content?.title || candidate.title,
        source_type: candidate.source_type,
        domain,
        published_date: content?.published_date || candidate.published_date,
        summary: content?.summary || candidate.summary,
        highlights: content?.highlights?.length ? content.highlights : candidate.highlights,
      };
    });

    applyConfirmation(record, summarizeEvidence(sources, record));
    if (record.context_confirmed) record.notes.push('Exa confirmation strengthened review context without changing AFM source-of-truth status.');
    if (record.disposal_confirmed) record.notes.push('Exa context mentioned disposal-like language; AFM evidence remains authoritative.');
  }

  return records;
}
