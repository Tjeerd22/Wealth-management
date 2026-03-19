import { NormalizedSignalRecord } from '../types.js';
import { dayDistance } from '../utils/dates.js';
import { normalizeCompanyName, normalizeName } from '../utils/strings.js';

function mergeRecords(base: NormalizedSignalRecord, candidate: NormalizedSignalRecord): NormalizedSignalRecord {
  if (base === candidate) return base;
  base.notes.push(`Merged duplicate record ${candidate.record_id}.`);
  base.provenance_sources = Array.from(new Set([...(base.provenance_sources ?? []), ...(candidate.provenance_sources ?? []), candidate.source_name]));
  base.provenance_record_ids = Array.from(new Set([...(base.provenance_record_ids ?? []), base.record_id, candidate.record_id, ...(candidate.provenance_record_ids ?? [])]));
  base.evidence_strength = Math.max(base.evidence_strength, candidate.evidence_strength);
  base.natural_person_confidence = Math.max(base.natural_person_confidence, candidate.natural_person_confidence);
  base.signal_confidence = Math.max(base.signal_confidence, candidate.signal_confidence);
  base.nl_relevance_score = Math.max(base.nl_relevance_score, candidate.nl_relevance_score);
  base.role ||= candidate.role;
  base.company_domain ||= candidate.company_domain;
  return base;
}

function samePersonVariant(a: NormalizedSignalRecord, b: NormalizedSignalRecord): boolean {
  const nameA = normalizeName(a.person_name);
  const nameB = normalizeName(b.person_name);
  const sameSurname = Boolean(a.person_last_name) && a.person_last_name === b.person_last_name;
  const initialA = nameA.split(' ')[0]?.replace(/\./g, '').charAt(0);
  const initialB = nameB.split(' ')[0]?.replace(/\./g, '').charAt(0);
  return sameSurname && Boolean(initialA) && initialA === initialB;
}

export function dedupeSignals(records: NormalizedSignalRecord[]): NormalizedSignalRecord[] {
  const deduped: NormalizedSignalRecord[] = [];
  for (const record of records) {
    const exactMatch = deduped.find((existing) =>
      existing !== record
      && normalizeName(existing.person_name) === normalizeName(record.person_name)
      && normalizeCompanyName(existing.company_name) === normalizeCompanyName(record.company_name)
      && existing.signal_date === record.signal_date
      && existing.signal_type === record.signal_type,
    );
    if (exactMatch) {
      mergeRecords(exactMatch, record);
      continue;
    }

    const likelyMatch = deduped.find((existing) =>
      existing !== record
      && samePersonVariant(existing, record)
      && normalizeCompanyName(existing.company_name) === normalizeCompanyName(record.company_name)
      && existing.signal_type === record.signal_type
      && dayDistance(existing.signal_date, record.signal_date) === 0,
    );
    if (likelyMatch) {
      likelyMatch.notes.push(`Likely duplicate retained via initial/surname same-day heuristic with ${record.record_id}.`);
      mergeRecords(likelyMatch, record);
      continue;
    }

    deduped.push(record);
  }
  return deduped;
}
