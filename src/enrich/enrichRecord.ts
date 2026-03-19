import { ActorInput, ExaEnrichmentResult, NormalizedSignalRecord } from '../types.js';
import { clamp01 } from '../utils/strings.js';
import { enrichWithExa } from '../sources/exaEnrichment.js';

function inferCompanyDomain(companyName: string): string {
  return companyName ? `${companyName.toLowerCase().replace(/[^a-z0-9]+/g, '')}.nl` : '';
}

export async function enrichRecord(record: NormalizedSignalRecord, input: ActorInput): Promise<NormalizedSignalRecord> {
  if (!record.company_domain) {
    record.company_domain = inferCompanyDomain(record.company_name);
    if (record.company_domain) record.notes.push('Inferred company domain from company name heuristic; not treated as verified context.');
  }

  let exa: ExaEnrichmentResult | null = null;
  if (input.runExaEnrichment && input.exaApiKey) {
    exa = await enrichWithExa(record, input.exaApiKey);
  }

  if (exa) {
    record.role ||= exa.role ?? '';
    record.company_domain ||= exa.company_domain ?? '';
    record.company_country = exa.company_country ?? record.company_country;
    record.person_type = exa.person_type ?? record.person_type;
    record.enrichment_context = exa.contextSnippet;
    record.natural_person_confidence = clamp01(record.natural_person_confidence + (exa.natural_person_confidence_delta ?? 0));
    record.contactability_confidence = clamp01(record.contactability_confidence + (exa.contactability_confidence_delta ?? 0));
    record.notes.push(...(exa.notes ?? []));
  }

  return record;
}
