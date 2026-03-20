import { Actor, Dataset } from 'apify';
import { NormalizedSignalRecord } from '../types.js';

const APIFY_SAFE_ITEM_BYTES = 8_000_000;
const NOTES_CAP = 40;
const PROVENANCE_IDS_CAP = 100;
const PROVENANCE_SOURCES_CAP = 20;
const CONFIRMATION_URLS_CAP = 20;
const CONFIRMATION_SOURCES_CAP = 10;
const SUMMARY_MAX_CHARS = 4_000;
const CONTEXT_MAX_CHARS = 4_000;
const SIGNAL_DETAIL_MAX_CHARS = 1_500;
const CONTEXT_SUMMARY_MAX_CHARS = 1_500;
const NAME_MAX_CHARS = 512;
const ROLE_MAX_CHARS = 512;
const OVERSIZED_AUDIT_PREFIX = 'RAW_ARCHIVE_AUDIT_';

export interface RawArchiveExportStats {
  itemsWritten: number;
  compactedItems: number;
  kvAuditItems: number;
  maxSerializedBytes: number;
}

function truncateText(value: string, maxChars: number): string {
  if (!value || value.length <= maxChars) return value;
  return `${value.slice(0, Math.max(0, maxChars - 14))}…[truncated]`;
}

function dedupeStrings(values: string[] | undefined, cap: number): string[] {
  if (!values?.length) return [];
  const unique: string[] = [];
  for (const value of values) {
    if (!value || unique.includes(value)) continue;
    unique.push(value);
    if (unique.length >= cap) break;
  }
  return unique;
}

function estimateSerializedSizeBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), 'utf8');
}

async function persistOversizedAuditDetail(record: NormalizedSignalRecord): Promise<string> {
  const key = `${OVERSIZED_AUDIT_PREFIX}${record.record_id}`;
  await Actor.setValue(key, {
    record_id: record.record_id,
    notes: record.notes,
    provenance_record_ids: record.provenance_record_ids ?? [],
    provenance_sources: record.provenance_sources ?? [],
    confirmation_urls: record.confirmation_urls,
    confirmation_sources: record.confirmation_sources,
    raw_source_payload_summary: record.raw_source_payload_summary,
    confirmation_summary: record.confirmation_summary,
    enrichment_context: record.enrichment_context ?? '',
    signal_detail: record.signal_detail,
  });
  return key;
}

export async function makeRawArchiveItemSizeSafe(record: NormalizedSignalRecord): Promise<{ item: NormalizedSignalRecord; compacted: boolean; auditKey?: string; sizeBytes: number }> {
  const compactedRecord: NormalizedSignalRecord = {
    ...record,
    notes: dedupeStrings(record.notes, NOTES_CAP),
    provenance_record_ids: dedupeStrings(record.provenance_record_ids, PROVENANCE_IDS_CAP),
    provenance_sources: dedupeStrings(record.provenance_sources, PROVENANCE_SOURCES_CAP),
    confirmation_urls: dedupeStrings(record.confirmation_urls, CONFIRMATION_URLS_CAP),
    confirmation_sources: record.confirmation_sources.slice(0, CONFIRMATION_SOURCES_CAP).map((source) => ({
      ...source,
      summary: truncateText(source.summary ?? '', 400),
      highlights: dedupeStrings(source.highlights, 6).map((highlight) => truncateText(highlight, 240)),
    })),
    person_name: truncateText(record.person_name, NAME_MAX_CHARS),
    company_name: truncateText(record.company_name, NAME_MAX_CHARS),
    role: truncateText(record.role, ROLE_MAX_CHARS),
    raw_source_payload_summary: truncateText(record.raw_source_payload_summary, SUMMARY_MAX_CHARS),
    confirmation_summary: truncateText(record.confirmation_summary, SUMMARY_MAX_CHARS),
    enrichment_context: truncateText(record.enrichment_context ?? '', CONTEXT_MAX_CHARS),
    signal_detail: truncateText(record.signal_detail, SIGNAL_DETAIL_MAX_CHARS),
    context_summary: truncateText(record.context_summary, CONTEXT_SUMMARY_MAX_CHARS),
  };

  let sizeBytes = estimateSerializedSizeBytes(compactedRecord);
  let compacted = sizeBytes !== estimateSerializedSizeBytes(record);
  let auditKey: string | undefined;

  if (sizeBytes > APIFY_SAFE_ITEM_BYTES) {
    auditKey = await persistOversizedAuditDetail(record);
    compactedRecord.notes = dedupeStrings(compactedRecord.notes, 10);
    compactedRecord.provenance_record_ids = dedupeStrings(compactedRecord.provenance_record_ids, 10);
    compactedRecord.provenance_sources = dedupeStrings(compactedRecord.provenance_sources, 5);
    compactedRecord.confirmation_urls = dedupeStrings(compactedRecord.confirmation_urls, 5);
    compactedRecord.confirmation_sources = [];
    compactedRecord.person_name = truncateText(compactedRecord.person_name, 128);
    compactedRecord.company_name = truncateText(compactedRecord.company_name, 128);
    compactedRecord.role = truncateText(compactedRecord.role, 128);
    compactedRecord.raw_source_payload_summary = `Oversized audit detail moved to KV store key ${auditKey}. ${truncateText(record.raw_source_payload_summary, 512)}`;
    compactedRecord.confirmation_summary = truncateText(compactedRecord.confirmation_summary, 512);
    compactedRecord.enrichment_context = truncateText(compactedRecord.enrichment_context ?? '', 512);
    compactedRecord.signal_detail = truncateText(compactedRecord.signal_detail, 512);
    compactedRecord.context_summary = truncateText(compactedRecord.context_summary, 512);
    compactedRecord.notes.push(`Oversized audit detail moved to KV store key ${auditKey}.`);
    compacted = true;
    sizeBytes = estimateSerializedSizeBytes(compactedRecord);
  }

  if (sizeBytes > APIFY_SAFE_ITEM_BYTES) {
    throw new Error(`Raw archive item ${record.record_id} remains oversized after compaction (${sizeBytes} bytes).`);
  }

  return { item: compactedRecord, compacted, auditKey, sizeBytes };
}

export async function exportRawArchive(records: NormalizedSignalRecord[]): Promise<RawArchiveExportStats> {
  let compactedItems = 0;
  let kvAuditItems = 0;
  let maxSerializedBytes = 0;

  for (const record of records) {
    const prepared = await makeRawArchiveItemSizeSafe(record);
    if (prepared.compacted) {
      compactedItems += 1;
      console.log('[INFO] raw archive record compacted before Dataset.pushData', {
        recordId: record.record_id,
        sizeBytes: prepared.sizeBytes,
        auditKey: prepared.auditKey ?? null,
      });
    }
    if (prepared.auditKey) kvAuditItems += 1;
    maxSerializedBytes = Math.max(maxSerializedBytes, prepared.sizeBytes);
    await Dataset.pushData(prepared.item);
  }

  return {
    itemsWritten: records.length,
    compactedItems,
    kvAuditItems,
    maxSerializedBytes,
  };
}

export { estimateSerializedSizeBytes, APIFY_SAFE_ITEM_BYTES };
