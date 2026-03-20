import { logInfo, logWarn } from '../utils/logging.js';
import { NormalizedSignalRecord } from '../types.js';
import { normalizeCompanyName, normalizeName } from '../utils/strings.js';

const SUSPICIOUS_GROUP_THRESHOLD = 25;
const IMPLAUSIBLE_DEDUPE_RATIO = 0.98;

export interface DedupeStats {
  recordsBefore: number;
  recordsAfter: number;
  mergesPerformed: number;
  topMergeReasons: Array<{ reason: string; count: number }>;
  suspiciousGroups: Array<{
    groupKey: string;
    size: number;
    reason: string;
    recordIdsSample: string[];
  }>;
  reductionRatio: number;
}

export interface DedupeResult {
  records: NormalizedSignalRecord[];
  stats: DedupeStats;
}

interface DedupeGroup {
  canonical: NormalizedSignalRecord;
  recordIds: string[];
  reason: string;
  groupKey: string;
}

function incrementReason(counter: Map<string, number>, reason: string): void {
  counter.set(reason, (counter.get(reason) ?? 0) + 1);
}

function pushUnique(target: string[], values: Array<string | undefined>, cap?: number): void {
  for (const value of values) {
    if (!value || target.includes(value)) continue;
    target.push(value);
    if (cap && target.length >= cap) return;
  }
}

function mergeRecords(base: NormalizedSignalRecord, candidate: NormalizedSignalRecord, reason: string): NormalizedSignalRecord {
  if (base === candidate) return base;
  base.notes.push(`Merged duplicate record ${candidate.record_id} (${reason}).`);
  pushUnique(base.provenance_sources ??= [], [...(candidate.provenance_sources ?? []), candidate.source_name]);
  pushUnique(base.provenance_record_ids ??= [], [base.record_id, ...(candidate.provenance_record_ids ?? []), candidate.record_id]);
  pushUnique(base.confirmation_urls, candidate.confirmation_urls);
  if (candidate.confirmation_sources?.length) {
    const existing = new Set(base.confirmation_sources.map((source) => `${source.url}|${source.title}`));
    for (const source of candidate.confirmation_sources) {
      const key = `${source.url}|${source.title}`;
      if (existing.has(key)) continue;
      existing.add(key);
      base.confirmation_sources.push(source);
    }
  }
  base.evidence_strength = Math.max(base.evidence_strength, candidate.evidence_strength);
  base.natural_person_confidence = Math.max(base.natural_person_confidence, candidate.natural_person_confidence);
  base.signal_confidence = Math.max(base.signal_confidence, candidate.signal_confidence);
  base.nl_relevance_score = Math.max(base.nl_relevance_score, candidate.nl_relevance_score);
  base.issuer_desirability_score = Math.max(base.issuer_desirability_score, candidate.issuer_desirability_score);
  base.review_priority_score = Math.max(base.review_priority_score, candidate.review_priority_score);
  base.role ||= candidate.role;
  base.company_domain ||= candidate.company_domain;
  if (!base.raw_source_payload_summary.includes(candidate.raw_source_payload_summary)) {
    base.raw_source_payload_summary = `${base.raw_source_payload_summary} | ${candidate.raw_source_payload_summary}`;
  }
  if (!base.confirmation_summary && candidate.confirmation_summary) base.confirmation_summary = candidate.confirmation_summary;
  if (!base.enrichment_context && candidate.enrichment_context) base.enrichment_context = candidate.enrichment_context;
  return base;
}

function firstTokenInitial(value: string): string {
  return normalizeName(value).split(' ')[0]?.replace(/[^a-z]/g, '').charAt(0) ?? '';
}

function hasAbbreviatedGivenName(value: string): boolean {
  const tokens = value.split(/\s+/).filter(Boolean);
  return tokens.some((token, index) => index < Math.max(1, tokens.length - 1) && token.replace(/[^A-Za-z]/g, '').length <= 1);
}

function samePersonVariant(a: NormalizedSignalRecord, b: NormalizedSignalRecord): boolean {
  const surnameA = a.person_last_name || '';
  const surnameB = b.person_last_name || '';
  if (!surnameA || surnameA !== surnameB) return false;
  const initialA = firstTokenInitial(a.person_name);
  const initialB = firstTokenInitial(b.person_name);
  if (!initialA || initialA !== initialB) return false;

  const normalizedA = normalizeName(a.person_name);
  const normalizedB = normalizeName(b.person_name);
  if (normalizedA === normalizedB) return true;

  return hasAbbreviatedGivenName(a.person_name) || hasAbbreviatedGivenName(b.person_name);
}

function exactGroupKey(record: NormalizedSignalRecord): string {
  return [
    normalizeName(record.person_name),
    normalizeCompanyName(record.company_name),
    record.signal_date,
    record.signal_type,
    record.source_name,
  ].join('|');
}

function variantGroupKey(record: NormalizedSignalRecord): string {
  return [
    record.person_last_name,
    firstTokenInitial(record.person_name),
    normalizeCompanyName(record.company_name),
    record.signal_date,
    record.signal_type,
    record.source_name,
  ].join('|');
}

export function dedupeSignalsWithStats(records: NormalizedSignalRecord[]): DedupeResult {
  const exactGroups = new Map<string, DedupeGroup>();
  const variantGroups = new Map<string, DedupeGroup>();
  const deduped: NormalizedSignalRecord[] = [];
  const mergeReasons = new Map<string, number>();
  const suspiciousGroups: DedupeStats['suspiciousGroups'] = [];
  let mergesPerformed = 0;

  for (const record of records) {
    const exactKey = exactGroupKey(record);
    const exactGroup = exactGroups.get(exactKey);
    if (exactGroup) {
      mergeRecords(exactGroup.canonical, record, exactGroup.reason);
      exactGroup.recordIds.push(record.record_id);
      mergesPerformed += 1;
      incrementReason(mergeReasons, exactGroup.reason);
      continue;
    }

    const variantKey = variantGroupKey(record);
    const variantGroup = variantGroups.get(variantKey);
    if (variantGroup && samePersonVariant(variantGroup.canonical, record)) {
      variantGroup.canonical.notes.push(`Likely duplicate retained via initial/surname same-day heuristic with ${record.record_id}.`);
      mergeRecords(variantGroup.canonical, record, variantGroup.reason);
      variantGroup.recordIds.push(record.record_id);
      mergesPerformed += 1;
      incrementReason(mergeReasons, variantGroup.reason);
      continue;
    }

    deduped.push(record);
    const newExactGroup: DedupeGroup = {
      canonical: record,
      recordIds: [record.record_id],
      reason: 'exact_identity_same_source_same_day',
      groupKey: exactKey,
    };
    exactGroups.set(exactKey, newExactGroup);
    variantGroups.set(variantKey, {
      canonical: record,
      recordIds: [record.record_id],
      reason: 'initial_surname_same_company_same_source_same_day',
      groupKey: variantKey,
    });
  }

  for (const group of [...exactGroups.values(), ...variantGroups.values()]) {
    if (group.recordIds.length > SUSPICIOUS_GROUP_THRESHOLD) {
      suspiciousGroups.push({
        groupKey: group.groupKey,
        size: group.recordIds.length,
        reason: group.reason,
        recordIdsSample: group.recordIds.slice(0, 10),
      });
    }
  }

  const stats: DedupeStats = {
    recordsBefore: records.length,
    recordsAfter: deduped.length,
    mergesPerformed,
    topMergeReasons: [...mergeReasons.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5).map(([reason, count]) => ({ reason, count })),
    suspiciousGroups: suspiciousGroups.sort((a, b) => b.size - a.size).slice(0, 10),
    reductionRatio: records.length ? (records.length - deduped.length) / records.length : 0,
  };

  logInfo('dedupe stats', stats);
  if (stats.reductionRatio >= IMPLAUSIBLE_DEDUPE_RATIO) {
    logWarn('dedupe reduction ratio is implausibly high', {
      reductionRatio: stats.reductionRatio,
      candidateCause: 'Many records are sharing the same dedupe key. Inspect normalized person/company/date/source fields and suspiciousGroups.',
      suspiciousGroups: stats.suspiciousGroups,
    });
  }

  return { records: deduped, stats };
}

export function dedupeSignals(records: NormalizedSignalRecord[]): NormalizedSignalRecord[] {
  return dedupeSignalsWithStats(records).records;
}
