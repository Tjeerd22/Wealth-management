import { NormalizedSignalRecord } from '../types.js';
import { dayDistance } from '../utils/dates.js';
import { normalizeCompanyName, normalizeName } from '../utils/strings.js';

function mergeRecords(base: NormalizedSignalRecord, candidate: NormalizedSignalRecord): NormalizedSignalRecord {
  base.notes.push(`Merged duplicate record ${candidate.record_id}.`);
  base.provenance_sources = Array.from(new Set([...(base.provenance_sources ?? []), ...(candidate.provenance_sources ?? []), candidate.source_name]));
  base.evidence_strength = Math.max(base.evidence_strength, candidate.evidence_strength);
  base.natural_person_confidence = Math.max(base.natural_person_confidence, candidate.natural_person_confidence);
  base.signal_confidence = Math.max(base.signal_confidence, candidate.signal_confidence);
  base.role ||= candidate.role;
  base.company_domain ||= candidate.company_domain;
  return base;
}

export function dedupeSignals(records: NormalizedSignalRecord[]): NormalizedSignalRecord[] {
  const deduped: NormalizedSignalRecord[] = [];
  for (const record of records) {
    const exactMatch = deduped.find((existing) =>
      normalizeName(existing.person_name) === normalizeName(record.person_name)
      && normalizeCompanyName(existing.company_name) === normalizeCompanyName(record.company_name)
      && existing.signal_date === record.signal_date,
    );
    if (exactMatch) {
      mergeRecords(exactMatch, record);
      continue;
    }

    const likelyMatch = deduped.find((existing) =>
      existing.person_last_name === record.person_last_name
      && normalizeCompanyName(existing.company_name) === normalizeCompanyName(record.company_name)
      && dayDistance(existing.signal_date, record.signal_date) <= 3,
    );
    if (likelyMatch) {
      likelyMatch.notes.push(`Likely duplicate retained via surname/date heuristic with ${record.record_id}.`);
      mergeRecords(likelyMatch, record);
      continue;
    }

    deduped.push(record);
  }
  return deduped;
}
