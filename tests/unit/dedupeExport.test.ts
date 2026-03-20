import { beforeEach, describe, expect, it, vi } from 'vitest';
import { normalizeRecord } from '../../src/normalize/normalizeRecord.js';
import { dedupeSignalsWithStats } from '../../src/dedupe/dedupeSignals.js';
import { APIFY_SAFE_ITEM_BYTES, estimateSerializedSizeBytes, makeRawArchiveItemSizeSafe } from '../../src/export/exportRawArchive.js';

const { setValueMock, pushDataMock } = vi.hoisted(() => ({
  setValueMock: vi.fn(async () => undefined),
  pushDataMock: vi.fn(async () => undefined),
}));

vi.mock('apify', () => ({
  Actor: {
    setValue: setValueMock,
  },
  Dataset: {
    pushData: pushDataMock,
  },
}));

describe('dedupe and raw archive guardrails', () => {
  beforeEach(() => {
    setValueMock.mockClear();
  });

  it('does not collapse many same-issuer same-day records with distinct people into one merge group', () => {
    const records = Array.from({ length: 250 }, (_, index) => normalizeRecord({
      personName: `Person ${index}`,
      companyName: 'ASML Holding NV',
      signalDate: '2026-03-02',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: `Event ${index}`,
      sourceName: 'afm_mar19',
      sourceUrl: `fixture-${index}`,
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: `fixture-${index}`,
    }));

    const result = dedupeSignalsWithStats(records);
    expect(result.records).toHaveLength(250);
    expect(result.stats.mergesPerformed).toBe(0);
  });

  it('keeps same issuer and same person distinct when the dates differ', () => {
    const a = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });

    const result = dedupeSignalsWithStats([a, b]);
    expect(result.records).toHaveLength(2);
    expect(result.stats.mergesPerformed).toBe(0);
  });

  it('compacts large provenance and notes payloads below the item-size guard', async () => {
    const record = normalizeRecord({
      personName: 'Jan de Vries',
      companyName: 'Adyen NV',
      signalDate: '2026-03-01',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'X'.repeat(20_000),
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'summary'.repeat(2_000),
    });
    record.notes = Array.from({ length: 500 }, (_, index) => `note-${index}-${'x'.repeat(200)}`);
    record.provenance_record_ids = Array.from({ length: 5_000 }, (_, index) => `prov-${index}`);
    record.provenance_sources = Array.from({ length: 500 }, (_, index) => `source-${index}`);
    record.confirmation_urls = Array.from({ length: 500 }, (_, index) => `https://example.com/${index}`);
    record.confirmation_sources = Array.from({ length: 50 }, (_, index) => ({
      url: `https://example.com/${index}`,
      title: `Title ${index}`,
      source_type: 'news' as const,
      domain: 'example.com',
      summary: 'y'.repeat(2_000),
      highlights: Array.from({ length: 20 }, () => 'z'.repeat(400)),
    }));

    const prepared = await makeRawArchiveItemSizeSafe(record);
    expect(prepared.compacted).toBe(true);
    expect(estimateSerializedSizeBytes(prepared.item)).toBeLessThan(APIFY_SAFE_ITEM_BYTES);
    expect((prepared.item.notes ?? []).length).toBeLessThanOrEqual(40);
    expect((prepared.item.provenance_record_ids ?? []).length).toBeLessThanOrEqual(100);
  });

  it('moves oversized audit detail to KV and keeps the raw archive item bounded', async () => {
    const record = normalizeRecord({
      personName: 'Jan de Vries',
      companyName: 'Adyen NV',
      signalDate: '2026-03-01',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'X'.repeat(9_000_000),
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'summary',
    });
    record.notes = Array.from({ length: 200 }, (_, index) => `note-${index}-${'x'.repeat(1000)}`);
    record.provenance_record_ids = Array.from({ length: 20_000 }, (_, index) => `prov-${index}`);
    record.confirmation_urls = Array.from({ length: 2_000 }, (_, index) => `https://example.com/${index}`);
    record.confirmation_sources = Array.from({ length: 20 }, (_, index) => ({
      url: `https://oversized.example.com/${index}`,
      title: 'T'.repeat(1_000_000),
      source_type: 'news' as const,
      domain: 'D'.repeat(200_000),
      summary: 'S'.repeat(2_000),
      highlights: ['H'.repeat(1_000)],
    }));

    const prepared = await makeRawArchiveItemSizeSafe(record);
    expect(prepared.auditKey).toBe(`RAW_ARCHIVE_AUDIT_${record.record_id}`);
    expect(setValueMock).toHaveBeenCalledTimes(1);
    expect(estimateSerializedSizeBytes(prepared.item)).toBeLessThan(APIFY_SAFE_ITEM_BYTES);
    expect(prepared.item.raw_source_payload_summary).toContain(prepared.auditKey as string);
  });
});
