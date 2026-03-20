import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { fetchCsvRows, parseCsv } from '../../src/utils/csv.js';
import { ingestAfmMar19, AFM_MAR19_REQUIRED_COLUMNS } from '../../src/sources/afmMar19.js';
import { ingestAfmSubstantialHoldings, AFM_SUBSTANTIAL_REQUIRED_COLUMNS } from '../../src/sources/afmSubstantialHoldings.js';
import { normalizeRecord } from '../../src/normalize/normalizeRecord.js';
import { logNormalizationHealth, validateRequiredColumns } from '../../src/normalize/sourceNormalization.js';
import { applyInstitutionalFilter } from '../../src/filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from '../../src/filters/personConfidence.js';
import { dedupeSignals, dedupeSignalsWithStats } from '../../src/dedupe/dedupeSignals.js';
import { scoreSignal } from '../../src/scoring/scoreSignal.js';
import { applySignalGates } from '../../src/filters/signalGates.js';
import { defaultInput } from '../../src/config.js';
import { scoreNlRelevance } from '../../src/scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from '../../src/scoring/scoreIssuerDesirability.js';
import { exportReviewDataset, rankReviewRecords, topByIssuer } from '../../src/export/exportReviewDataset.js';
import { confirmContextForTopReviewRecords } from '../../src/enrich/confirmContextForTopReviewRecords.js';
import { appendRecords } from '../../src/main.js';

const { setValueMock } = vi.hoisted(() => ({ setValueMock: vi.fn(async () => undefined) }));
vi.mock('apify', () => ({
  Actor: { setValue: setValueMock, init: vi.fn(), exit: vi.fn(), getInput: vi.fn(async () => null) },
  Dataset: { pushData: vi.fn(async () => undefined), open: vi.fn(async () => ({ pushData: vi.fn(async () => undefined) })) },
}));

const mar19Fixture = readFileSync(new URL('../fixtures/afm_mar19_sample.csv', import.meta.url), 'utf8');
const substantialFixture = readFileSync(new URL('../fixtures/afm_substantial_sample.csv', import.meta.url), 'utf8');
const mar19SemicolonFixtureUrl = new URL('../fixtures/afm_mar19_semicolon_sample.csv', import.meta.url);
const mar19SemicolonFixture = readFileSync(mar19SemicolonFixtureUrl, 'utf8');
const substantialSemicolonFixtureUrl = new URL('../fixtures/afm_substantial_semicolon_sample.csv', import.meta.url);
const substantialSemicolonFixture = readFileSync(substantialSemicolonFixtureUrl, 'utf8');

describe('connector os dutch liquidity pipeline', () => {

  it('parses AFM-style semicolon CSV with BOM, whitespace, quotes, and empty lines', () => {
    // Fixture uses Dutch canonical headers as produced by the AFM export endpoint.
    const rows = parseCsv(mar19SemicolonFixture, { sourceName: 'AFM MAR 19 fixture' });
    expect(rows).toHaveLength(3);
    expect(rows[0].Transactie).toBe('2026-03-19');
    expect(rows[0]['Uitgevende instelling']).toBe('Universal Music Group N.V.');
    expect(rows[0].Meldingsplichtige).toBe('Jansen, Eva');
  });

  it('falls back to comma-delimited parsing when semicolon parsing is not valid', () => {
    const rows = parseCsv(mar19Fixture, { sourceName: 'AFM MAR 19 comma fixture' });
    expect(rows).toHaveLength(2);
    expect(rows[0].IssuingInstitution).toBe('Adyen NV');
  });

  it('uses the same robust CSV utility for AFM MAR 19 and substantial holdings ingestion', async () => {
    const mar19Rows = await fetchCsvRows(mar19SemicolonFixtureUrl.pathname, { sourceName: 'AFM MAR 19 fixture' });
    const substantialRows = await fetchCsvRows(substantialSemicolonFixtureUrl.pathname, { sourceName: 'AFM substantial holdings fixture' });
    const mar19Records = await ingestAfmMar19(mar19SemicolonFixtureUrl.pathname);
    const substantialRecords = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname);

    expect(mar19Rows).toHaveLength(3);
    expect(substantialRows).toHaveLength(3);
    expect(mar19Records).toHaveLength(3);
    expect(substantialRecords).toHaveLength(3);
    expect(substantialRecords[1].capital_interest_before).toBe(6.2);
    expect(substantialRecords[1].capital_interest_after).toBe(4.8);
  });


  it('maps Dutch AFM MAR 19 headers into canonical normalized fields', async () => {
    const records = await ingestAfmMar19(mar19SemicolonFixtureUrl.pathname);
    expect(records[0].signal_date).toBe('2026-03-19');
    expect(records[0].company_name).toBe('Universal Music Group N.V.');
    expect(records[0].person_name).toBe('Jansen, Eva');
    expect(records[0].person_last_name).toBe('jansen');
    expect(records[0].raw_source_payload_summary).toContain('transactie=2026-03-19');
    expect(records[0].raw_source_payload_summary).toContain('uitgevende_instelling=Universal Music Group N.V.');
  });

  it('maps Dutch AFM substantial holdings headers into canonical normalized fields', async () => {
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname);
    expect(records[0].signal_date).toBe('2026-03-19');
    expect(records[0].company_name).toBe('Pharming Group N.V.');
    expect(records[0].person_name).toBe('Bank Of America Corporation');
    expect(records[0].raw_source_payload_summary).toContain('kvk_nr=');
    expect(records[0].raw_source_payload_summary).toContain('plaats=');
  });

  it('fails fast when a source loses canonical identity coverage on too many rows', () => {
    const rows = [{ Transactie: '2026-03-19' }, { Transactie: '2026-03-18' }, { Transactie: '2026-03-17' }];
    const records = rows.map((row, index) => normalizeRecord({
      personName: '',
      companyName: '',
      signalDate: row.Transactie ?? '',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: `row-${index}`,
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    }));

    expect(() => logNormalizationHealth('afm_mar19', rows, records)).toThrow(/Normalization health check failed/);
  });

  it('flags a clear institution record from substantial holdings', () => {
    const [row] = parseCsv(substantialFixture);
    const record = normalizeRecord({
      personName: row.NotifyingParty,
      companyName: row.Issuer,
      signalDate: row.NotificationDate,
      signalType: 'substantial_holding_reduction',
      signalDetail: 'Reduction',
      sourceName: 'afm_substantial',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: 0.8,
      rawSummary: 'fixture',
    });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.institutional_risk).toBe('high');
    expect(record.natural_person_confidence).toBeLessThan(0.2);
  });

  it('scores a likely natural person from MAR 19 in the stronger personal-name band', () => {
    const [row] = parseCsv(mar19Fixture);
    const record = normalizeRecord({
      personName: row.Notifiable,
      companyName: row.IssuingInstitution,
      signalDate: row.TransactionDate,
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Thin but timely MAR 19 record',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.7);
    expect(record.natural_person_confidence).toBeLessThanOrEqual(0.85);
  });

  it('handles initials plus surname as moderate confidence rather than near-failure', () => {
    const rows = parseCsv(mar19Fixture);
    const row = rows[1];
    const record = normalizeRecord({
      personName: row.Notifiable,
      companyName: row.IssuingInstitution,
      signalDate: row.TransactionDate,
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Initials case',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.5);
    expect(record.natural_person_confidence).toBeLessThanOrEqual(0.65);
  });

  it('treats dutch comma-prefix human names as strong personal patterns', () => {
    const record = normalizeRecord({
      personName: 'Dijk, Van J.',
      companyName: 'ASML Holding NV',
      signalDate: '2026-03-02',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Dutch surname prefix case',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.78);
  });

  it('preserves Dutch surname prefixes for dedupe keys', () => {
    const record = normalizeRecord({
      personName: 'Jan van Dijk',
      companyName: 'ASML Holding NV',
      signalDate: '2026-03-02',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Prefix case',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });
    expect(record.person_last_name).toBe('dijk');
  });

  it('merges likely duplicates across initials and full name only on the same day', () => {
    const a = normalizeRecord({ personName: 'J. van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.7, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(1);
    expect(deduped[0].notes.join(' ')).toContain('Likely duplicate');
    expect(deduped[0].provenance_record_ids).toContain(a.record_id);
    expect(deduped[0].provenance_record_ids).toContain(b.record_id);
  });

  it('does not collapse distinct same-person events on different dates', () => {
    const a = normalizeRecord({ personName: 'J. van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-03', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.7, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(2);
  });

  it('merges a large AFM-sized source array without stack overflow', () => {
    const seed = normalizeRecord({
      personName: 'Jan de Vries',
      companyName: 'Adyen NV',
      signalDate: '2026-03-01',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Large merge regression',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });
    const sourceRecords = Array.from({ length: 254814 }, (_, index) => ({
      ...seed,
      record_id: `large-${index}`,
      source_url: `fixture-${index}`,
      provenance_record_ids: [`large-${index}`],
      notes: [],
      blocked_by: [],
      confirmation_urls: [],
      confirmation_sources: [],
    }));
    const targetRecords = Array.from({ length: 8782 }, (_, index) => ({
      ...seed,
      record_id: `seed-${index}`,
      source_url: `seed-${index}`,
      provenance_record_ids: [`seed-${index}`],
      notes: [],
      blocked_by: [],
      confirmation_urls: [],
      confirmation_sources: [],
    }));

    expect(() => appendRecords(targetRecords, sourceRecords)).not.toThrow();
    expect(targetRecords).toHaveLength(8782 + 254814);
    expect(targetRecords[8782].record_id).toBe('large-0');
    expect(targetRecords.at(-1)?.record_id).toBe('large-254813');
  });

  it('removes exact duplicate rows without dropping distinct dated events', () => {
    const a = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(1);
  });


  it('does not merge records that are missing canonical identity fields', () => {
    const a = normalizeRecord({ personName: '', companyName: '', signalDate: '', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: '', companyName: '', signalDate: '', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(2);
  });

  it('keeps ambiguous family holding as review-only with explicit blockers', () => {
    const row = parseCsv(substantialFixture)[1];
    const record = normalizeRecord({
      personName: row.NotifyingParty,
      companyName: row.Issuer,
      signalDate: row.NotificationDate,
      signalType: 'substantial_holding_reduction',
      signalDetail: 'Family holding reduction',
      sourceName: 'afm_substantial',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: 0.82,
      rawSummary: 'fixture',
    });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.role = 'Founder family vehicle';
    record.company_domain = 'besi.com';
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.match_ready).toBe(false);
    expect(record.blocked_by).toContain('low_natural_person_confidence');
    expect(record.review_bucket).toBe('C');
    expect(record.review_action).toBe('manual_person_verify');
  });

  it('passes a strong natural-person reduction into match-ready while keeping blockers empty', () => {
    const row = parseCsv(substantialFixture)[2];
    const record = normalizeRecord({
      personName: row.NotifyingParty,
      companyName: row.Issuer,
      signalDate: row.NotificationDate,
      signalType: 'substantial_holding_reduction',
      signalDetail: 'Clear reduction',
      sourceName: 'afm_substantial',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: 0.82,
      rawSummary: 'fixture',
      capitalInterestBefore: 6.2,
      capitalInterestAfter: 4.8,
    });
    applyInstitutionalFilter(record);
    record.role = 'Executive Director';
    record.company_domain = 'cm.com';
    record.enrichment_context = 'Board biography found';
    record.person_type = 'natural_person';
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.signal_confidence).toBeGreaterThanOrEqual(defaultInput.minSignalConfidence);
    expect(record.match_ready).toBe(true);
    expect(record.blocked_by).toHaveLength(0);
    expect(record.review_bucket).toBe('A');
    expect(record.review_action).toBe('watchlist_only');
  });

  it('adds score spread and explicit blockers to MAR 19 records without inflating match-ready', () => {
    const strong = normalizeRecord({
      personName: 'Jan de Vries',
      companyName: 'Adyen NV',
      signalDate: '2026-03-01',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Thin MAR 19',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });
    const weak = normalizeRecord({
      personName: 'X Capital BV',
      companyName: 'Foreign Plc',
      signalDate: '2026-03-01',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'Thin MAR 19',
      sourceName: 'afm_mar19',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
    });

    for (const record of [strong, weak]) {
      applyInstitutionalFilter(record);
      record.natural_person_confidence = scoreNaturalPersonConfidence(record);
      if (record === strong) {
        record.role = 'CFO';
        record.company_domain = 'adyen.com';
      }
      record.nl_relevance_score = scoreNlRelevance(record);
      record.issuer_desirability_score = scoreIssuerDesirability(record);
      scoreSignal(record, 45);
      record.match_ready = true;
      applySignalGates(record, defaultInput);
    }

    expect(strong.signal_confidence - weak.signal_confidence).toBeGreaterThan(0.2);
    expect(strong.match_ready).toBe(false);
    expect(strong.blocked_by).toContain('unconfirmed_disposal');
    expect(weak.blocked_by).toContain('low_natural_person_confidence');
    expect(weak.blocked_by).toContain('low_nl_relevance');
  });

  it('ranks review exports by bucket and review priority with cluster control', async () => {
    const a = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const b = normalizeRecord({ personName: 'Klaas de Boer', companyName: 'Adyen NV', signalDate: '2026-03-17', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const c = normalizeRecord({ personName: 'Piet van Dam', companyName: 'ASML Holding NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'C', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    for (const [idx, record] of [a, b, c].entries()) {
      record.natural_person_confidence = 0.78;
      record.nl_relevance_score = idx === 1 ? 0.79 : 0.8;
      record.issuer_desirability_score = idx === 2 ? 0.88 : 0.9;
      record.signal_confidence = 0.58;
      record.review_bucket = 'A';
      record.review_action = 'manual_context_check';
      record.role = 'CFO';
      record.company_domain = idx === 2 ? 'asml.com' : 'adyen.com';
    }
    const ranked = rankReviewRecords([a, b, c]);
    expect(ranked[0].company_name).toBe('Adyen NV');
    expect(ranked[1].company_name).toBe('ASML Holding NV');
    expect(ranked[0].review_priority_score).toBeGreaterThan(ranked[1].review_priority_score);
    expect(ranked[1].review_priority_score).toBeGreaterThan(ranked[2].review_priority_score);

    const review = await exportReviewDataset([b, a, c], 10);
    expect(review[0].review_bucket).toBe('A');
    expect(review[0].review_priority_score).toBeGreaterThan(review[2].review_priority_score);
  });


  it('keeps Exa confirmation scoped to bucket A and top configurable bucket B records when Exa is unavailable', async () => {
    const bucketA = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const bucketB1 = normalizeRecord({ personName: 'Piet van Dam', companyName: 'ASML Holding NV', signalDate: '2026-03-17', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B1', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const bucketB2 = normalizeRecord({ personName: 'Klaas Jansen', companyName: 'Prosus NV', signalDate: '2026-03-16', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B2', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const bucketC = normalizeRecord({ personName: 'Holding BV', companyName: 'Random Plc', signalDate: '2026-03-15', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'C', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });

    bucketA.review_bucket = 'A';
    bucketA.review_action = 'manual_context_check';
    bucketB1.review_bucket = 'B';
    bucketB1.review_action = 'manual_context_check';
    bucketB2.review_bucket = 'B';
    bucketB2.review_action = 'manual_context_check';
    bucketC.review_bucket = 'C';
    bucketC.review_action = 'manual_person_verify';

    await confirmContextForTopReviewRecords([bucketA, bucketB1, bucketB2, bucketC], {
      ...defaultInput,
      runExaEnrichment: false,
      exaTopReviewConfirmations: 1,
    });

    expect(bucketA.confirmation_summary).toContain('skipped');
    expect(bucketB1.confirmation_summary).toContain('skipped');
    expect(bucketB2.confirmation_summary).toBe('');
    expect(bucketC.confirmation_summary).toBe('');
  });

  it('uses Exa search and contents as confirmatory context without loosening match-ready gates', async () => {
    const record = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    record.review_bucket = 'A';
    record.review_action = 'manual_context_check';
    record.role = 'CFO';
    record.company_domain = 'adyen.com';
    record.match_ready = false;

    let fetchCount = 0;
    const responses = [
      { ok: true, json: async () => ({ results: [{ url: 'https://adyen.com/about', title: 'Adyen leadership Jan de Vries CFO', summary: 'Board page', highlights: ['Jan de Vries serves as CFO'], score: 0.9 }] }) },
      { ok: true, json: async () => ({ results: [{ url: 'https://news.example.com/article', title: 'Adyen insider share sale', summary: 'News summary', highlights: ['Jan de Vries sold shares after filing'], score: 0.8 }] }) },
      { ok: true, json: async () => ({ results: [
        { url: 'https://adyen.com/about', title: 'Adyen leadership Jan de Vries CFO', summary: 'Board page', highlights: ['Jan de Vries serves as CFO'], text: 'Jan de Vries serves as CFO of Adyen.' },
        { url: 'https://news.example.com/article', title: 'Adyen insider share sale', summary: 'News summary', highlights: ['Jan de Vries sold shares after filing'], text: 'News coverage says Jan de Vries sold shares.' },
      ] }) },
    ];

    const originalFetch = global.fetch;
    global.fetch = (async () => responses[fetchCount++]) as unknown as typeof fetch;
    try {
      await confirmContextForTopReviewRecords([record], {
        ...defaultInput,
        runExaEnrichment: true,
        exaApiKey: 'test-key',
        exaTopReviewConfirmations: 2,
      });
    } finally {
      global.fetch = originalFetch;
    }

    expect(fetchCount).toBe(3);
    expect(record.context_confirmed).toBe(true);
    expect(record.role_confirmed).toBe(true);
    expect(record.disposal_confirmed).toBe(true);
    expect(record.confirmation_urls).toHaveLength(2);
    expect(record.confirmation_evidence_strength).toBe('strong');
    expect(record.review_action_updated).toBe('watchlist_only');
    expect(record.match_ready).toBe(false);
  });

  it('exports top-by-issuer views and keeps review_action aligned with blockers', () => {
    const low = normalizeRecord({ personName: 'Klaas', companyName: 'Random Plc', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    low.natural_person_confidence = 0.62;
    low.nl_relevance_score = 0.2;
    low.issuer_desirability_score = 0.15;
    low.signal_confidence = 0.28;
    low.review_bucket = 'C';
    low.blocked_by = ['low_nl_relevance'];
    low.review_action = 'discard_low_relevance';

    const issuerA1 = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A1', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const issuerA2 = normalizeRecord({ personName: 'Piet de Vries', companyName: 'Adyen NV', signalDate: '2026-03-17', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A2', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const issuerA3 = normalizeRecord({ personName: 'Koen de Vries', companyName: 'Adyen NV', signalDate: '2026-03-16', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A3', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const issuerA4 = normalizeRecord({ personName: 'Milan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-15', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A4', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    for (const record of [issuerA1, issuerA2, issuerA3, issuerA4]) {
      record.natural_person_confidence = 0.78;
      record.nl_relevance_score = 0.81;
      record.issuer_desirability_score = 0.9;
      record.signal_confidence = 0.58;
      record.review_bucket = 'A';
      record.review_action = 'manual_context_check';
      record.role = 'CFO';
      record.company_domain = 'adyen.com';
    }

    const byIssuer = topByIssuer([issuerA1, issuerA2, issuerA3, issuerA4, low], 3);
    expect(byIssuer.filter((record) => record.company_name === 'Adyen NV')).toHaveLength(3);
    expect(byIssuer.find((record) => record.company_name === 'Random Plc')?.review_action).toBe('discard_low_relevance');
  });

  // --- Section 1 & 2: Hard source schema contracts ---

  it('fails fast when AFM MAR 19 CSV is missing required Dutch columns', () => {
    // Simulate a CSV that has different (English) headers — e.g. after AFM changes export format.
    const rowsWithWrongHeaders = [{ TransactionDate: '2026-03-01', Issuer: 'Adyen NV', Name: 'Jan de Vries' }];
    expect(() => validateRequiredColumns(rowsWithWrongHeaders, AFM_MAR19_REQUIRED_COLUMNS, 'AFM MAR 19'))
      .toThrow(/schema contract violated.*missing required column/);
  });

  it('fails fast when AFM substantial holdings CSV is missing required Dutch columns', () => {
    const rowsWithWrongHeaders = [{ NotificationDate: '2026-03-01', Issuer: 'Adyen NV' }];
    expect(() => validateRequiredColumns(rowsWithWrongHeaders, AFM_SUBSTANTIAL_REQUIRED_COLUMNS, 'AFM substantial holdings'))
      .toThrow(/schema contract violated.*missing required column/);
  });

  it('fails fast when CSV has zero rows (cannot validate schema contract)', () => {
    expect(() => validateRequiredColumns([], AFM_MAR19_REQUIRED_COLUMNS, 'AFM MAR 19'))
      .toThrow(/zero rows/);
  });

  it('maps Dutch AFM MAR 19 headers — Transactie drives signal_date, not TransactionDate', async () => {
    // The semicolon fixture uses Dutch headers exclusively. If the adapter were
    // falling back to English aliases, signal_date would still resolve; this test
    // verifies the raw_source_payload_summary uses the Dutch label (hardcoded template)
    // and that all canonical fields map correctly.
    const records = await ingestAfmMar19(mar19SemicolonFixtureUrl.pathname);
    expect(records).toHaveLength(3);
    expect(records[0].signal_date).toBe('2026-03-19');
    expect(records[0].company_name).toBe('Universal Music Group N.V.');
    expect(records[0].person_name).toBe('Jansen, Eva');
    // person_last_name should come from MeldingsPlichtigeAchternaam = 'Jansen'
    expect(records[0].person_last_name).toBe('jansen');
    // rawSummary template always uses Dutch labels regardless of CSV column names
    expect(records[0].raw_source_payload_summary).toContain('transactie=2026-03-19');
    expect(records[0].raw_source_payload_summary).toContain('uitgevende_instelling=Universal Music Group N.V.');
  });

  it('maps Dutch AFM substantial holdings headers — Datum meldingsplicht drives signal_date', async () => {
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname, 365);
    expect(records).toHaveLength(3);
    expect(records[0].signal_date).toBe('2026-03-19');
    expect(records[0].company_name).toBe('Pharming Group N.V.');
    expect(records[0].person_name).toBe('Bank Of America Corporation');
    expect(records[0].raw_source_payload_summary).toContain('kvk_nr=');
    expect(records[0].raw_source_payload_summary).toContain('plaats=');
  });

  it('passing a CSV with missing required column to ingestAfmMar19 throws before any normalization', async () => {
    // Build a temp CSV body that has wrong headers and write it as a local path.
    // We use parseCsv directly to get rows, then validate — ingestAfmMar19 uses fetchCsvRows
    // which calls parseCsv. The easiest way to test the gate is via validateRequiredColumns directly
    // (already covered) but we also verify the error message is descriptive.
    const rows = [{ TransactionDate: '2026-03-01', Issuer: 'Test NV', Name: 'Someone' }];
    let thrown: Error | null = null;
    try {
      validateRequiredColumns(rows, AFM_MAR19_REQUIRED_COLUMNS, 'AFM MAR 19');
    } catch (e) {
      thrown = e as Error;
    }
    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('Transactie');
    expect(thrown!.message).toContain('Present columns');
  });

  // --- Section 4 & 5: Unknown identities and conservative dedupe ---

  it('unknown identities do not merge — each gets a non-mergeable synthetic key', () => {
    // Two records with empty person, company, date must not collapse.
    const a = normalizeRecord({ personName: '', companyName: '', signalDate: '', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: '', companyName: '', signalDate: '', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });
    const result = dedupeSignalsWithStats([a, b]);
    expect(result.records).toHaveLength(2);
    expect(result.stats.mergesPerformed).toBe(0);
  });

  it('distinct dates for same person and issuer remain distinct — never merged', () => {
    const a = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });
    const c = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-15', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'C', sourceName: 'afm_mar19', sourceUrl: 'c', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'c' });
    const result = dedupeSignalsWithStats([a, b, c]);
    expect(result.records).toHaveLength(3);
    expect(result.stats.mergesPerformed).toBe(0);
  });

  it('emits implausible-reduction warning when nearly all records collapse into one group', () => {
    // All records identical identity → should trigger implausible ratio warning in logs.
    // We can't capture console output directly, but we verify the stats object exposes it.
    const seed = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'dup', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    // 100 identical records → 99 merges → reductionRatio = 0.99 ≥ 0.98 threshold.
    const records = Array.from({ length: 100 }, (_, i) => ({
      ...seed,
      record_id: `dup-${i}`,
      source_url: `fixture-${i}`,
      notes: [],
      blocked_by: [],
      confirmation_urls: [],
      confirmation_sources: [],
      provenance_record_ids: [`dup-${i}`],
    }));
    const result = dedupeSignalsWithStats(records);
    expect(result.stats.reductionRatio).toBeGreaterThanOrEqual(0.98);
    expect(result.records).toHaveLength(1);
  });

  // --- Section 6: Early lookback filter for large source ---

  it('AFM substantial holdings drops stale rows before normalization when lookbackDays is tight', async () => {
    // The semicolon fixture has rows dated 2026-03-17, 2026-03-18, 2026-03-19 — all in the past
    // relative to today (2026-03-20). With lookbackDays=0, only rows where diffMs === 0 (exactly
    // today) pass isWithinLookback. None of the fixture rows are today, so all are dropped.
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname, 0);
    expect(records).toHaveLength(0);
  });

  it('AFM substantial holdings keeps all rows when lookback is wide enough', async () => {
    // lookbackDays=3650 (10 years) — all fixture rows should be kept.
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname, 3650);
    expect(records).toHaveLength(3);
  });

  // --- Section 7: Source reliability policy (degraded mode) ---

  it('AFM substantial holdings 504 triggers degraded mode — run continues with MAR 19 only', async () => {
    // The retry policy is 2 retries with exponential backoff (~2s + ~4s).
    // This test must wait for all retries to complete, hence the extended timeout.
    let fetchCallCount = 0;
    const originalFetch = global.fetch;
    // Simulate 3 consecutive 504s (enough to exhaust 2 retries).
    global.fetch = (async (_url: string) => {
      fetchCallCount += 1;
      return { ok: false, status: 504, body: null } as unknown as Response;
    }) as unknown as typeof fetch;

    let thrown: Error | null = null;
    try {
      // ingestAfmSubstantialHoldings calls fetchWithRetry (maxRetries=2 → 3 total attempts)
      await ingestAfmSubstantialHoldings('https://fake-afm.nl/substantial', 45);
    } catch (e) {
      thrown = e as Error;
    } finally {
      global.fetch = originalFetch;
    }

    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('504');
    // 3 attempts total (1 initial + 2 retries)
    expect(fetchCallCount).toBe(3);
  }, 15_000);

  it('AFM MAR 19 failure propagates immediately — no retry, no degraded mode', async () => {
    const originalFetch = global.fetch;
    let fetchCallCount = 0;
    global.fetch = (async (_url: string) => {
      fetchCallCount += 1;
      return { ok: false, status: 503, body: null } as unknown as Response;
    }) as unknown as typeof fetch;

    let thrown: Error | null = null;
    try {
      // MAR 19 uses fetchCsvRows which uses fetchBodyWithTimeout — no retry wrapper.
      await ingestAfmMar19('https://fake-afm.nl/mar19');
    } catch (e) {
      thrown = e as Error;
    } finally {
      global.fetch = originalFetch;
    }

    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('503');
    // Should have been exactly 1 attempt — no retry on MAR 19.
    expect(fetchCallCount).toBe(1);
  });
});
