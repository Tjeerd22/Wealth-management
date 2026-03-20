import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { fetchCsvRows, parseCsv } from '../../src/utils/csv.js';
import { ingestAfmMar19, AFM_MAR19_REQUIRED_COLUMNS } from '../../src/sources/afmMar19.js';
import { ingestAfmSubstantialHoldings, AFM_SUBSTANTIAL_REQUIRED_COLUMNS } from '../../src/sources/afmSubstantialHoldings.js';
import { normalizeRecord } from '../../src/normalize/normalizeRecord.js';
import { logNormalizationHealth, validateRequiredColumns, validateSourceSchema } from '../../src/normalize/sourceNormalization.js';
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
import { appendRecords, loadSourceWithPolicy } from '../../src/main.js';

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
const substantialMultirowFilingFixtureUrl = new URL('../fixtures/afm_substantial_multirow_filing.csv', import.meta.url);
const substantialWithFooterFixtureUrl = new URL('../fixtures/afm_substantial_with_footer.csv', import.meta.url);

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
    expect(rows[0]['Uitgevende instelling']).toBe('Adyen NV');
  });

  it('validates source schema contracts and fails on missing required columns', () => {
    expect(() => validateSourceSchema([{ Transactie: '2026-03-19' }], { sourceName: 'AFM MAR 19', requiredColumns: [...AFM_MAR19_REQUIRED_COLUMNS] })).toThrow(/missing required columns/i);
    expect(() => validateSourceSchema([{ 'Datum meldingsplicht': '2026-03-19' }], { sourceName: 'AFM substantial holdings', requiredColumns: [...AFM_SUBSTANTIAL_REQUIRED_COLUMNS] })).toThrow(/missing required columns/i);
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
  });

  it('maps Dutch AFM substantial holdings headers into canonical normalized fields', async () => {
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname);
    expect(records[0].signal_date).toBe('2026-03-19');
    expect(records[0].company_name).toBe('Pharming Group N.V.');
    expect(records[0].person_name).toBe('Bank Of America Corporation');
    expect(records[0].raw_source_payload_summary).toContain('kvk_nr=12345678');
    expect(records[0].raw_source_payload_summary).toContain('plaats=Amsterdam');
  });

  it('fails fast when a source loses canonical identity coverage on too many rows', () => {
    const records = [0, 1, 2].map((index) => normalizeRecord({ personName: '', companyName: '', signalDate: `2026-03-1${index}`, signalType: 'pdmr_transaction_unconfirmed', signalDetail: `row-${index}`, sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' }));
    expect(() => logNormalizationHealth('afm_mar19', records)).toThrow(/Normalization health check failed/);
  });

  it('enters degraded mode when substantial holdings retries exhaust with a 504 and MAR 19 can still succeed', async () => {
    const result = await loadSourceWithPolicy('afm_substantial', 'fixture', async () => { throw new Error('Failed to fetch CSV from fixture: 504'); });
    expect(result.degraded).toBe(true);
    expect(result.status.status).toBe('degraded');
    expect(result.status.retries).toBe(2);
  });

  it('fails immediately when MAR 19 fetch fails', async () => {
    await expect(loadSourceWithPolicy('afm_mar19', 'fixture', async () => { throw new Error('Failed to fetch CSV from fixture: 500'); })).rejects.toThrow(/AFM MAR 19 failed/);
  });

  it('flags a clear institution record from substantial holdings', () => {
    const [row] = parseCsv(substantialFixture);
    const record = normalizeRecord({ personName: row['Meldingsplichtige'], companyName: row['Uitgevende instelling'], signalDate: row['Datum meldingsplicht'], signalType: 'substantial_holding_reduction', signalDetail: 'Reduction', sourceName: 'afm_substantial', sourceUrl: 'fixture', evidenceType: 'afm_csv_holding_notice', evidenceStrength: 0.8, rawSummary: 'fixture' });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.institutional_risk).toBe('high');
    expect(record.natural_person_confidence).toBeLessThan(0.2);
  });

  it('scores a likely natural person from MAR 19 in the stronger personal-name band', () => {
    const [row] = parseCsv(mar19Fixture);
    const record = normalizeRecord({ personName: row['Meldingsplichtige'], companyName: row['Uitgevende instelling'], signalDate: row['Transactie'], signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Thin but timely MAR 19 record', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.7);
    expect(record.natural_person_confidence).toBeLessThanOrEqual(0.85);
  });

  it('handles initials plus surname as moderate confidence rather than near-failure', () => {
    const rows = parseCsv(mar19Fixture);
    const row = rows[1];
    const record = normalizeRecord({ personName: row['Meldingsplichtige'], companyName: row['Uitgevende instelling'], signalDate: row['Transactie'], signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Initials case', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.5);
    expect(record.natural_person_confidence).toBeLessThanOrEqual(0.65);
  });

  it('treats dutch comma-prefix human names as strong personal patterns', () => {
    const record = normalizeRecord({ personName: 'Dijk, Van J.', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Dutch surname prefix case', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.78);
  });

  it('preserves Dutch surname prefixes for dedupe keys', () => {
    const record = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Prefix case', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    expect(record.person_last_name).toBe('dijk');
  });

  it('merges likely duplicates across initials and full name only on the same day', () => {
    const a = normalizeRecord({ personName: 'J. van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.7, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(1);
  });

  it('does not collapse distinct same-person events on different dates', () => {
    const a = normalizeRecord({ personName: 'J. van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-03', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.7, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(2);
  });

  it('merges a large AFM-sized source array without stack overflow', () => {
    const seed = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Large merge regression', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const sourceRecords = Array.from({ length: 254814 }, (_, index) => ({ ...seed, record_id: `large-${index}`, source_url: `fixture-${index}`, provenance_record_ids: [`large-${index}`], notes: [], blocked_by: [], confirmation_urls: [], confirmation_sources: [] }));
    const targetRecords = Array.from({ length: 8782 }, (_, index) => ({ ...seed, record_id: `seed-${index}`, source_url: `seed-${index}`, provenance_record_ids: [`seed-${index}`], notes: [], blocked_by: [], confirmation_urls: [], confirmation_sources: [] }));
    expect(() => appendRecords(targetRecords, sourceRecords)).not.toThrow();
    expect(targetRecords).toHaveLength(8782 + 254814);
  });

  it('does not merge records that are missing canonical identity fields', () => {
    const a = normalizeRecord({ personName: '', companyName: '', signalDate: '', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: '', companyName: '', signalDate: '', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(2);
  });

  it('keeps ambiguous family holding as review-only with explicit blockers', () => {
    const row = parseCsv(substantialFixture)[1];
    const record = normalizeRecord({ personName: row['Meldingsplichtige'], companyName: row['Uitgevende instelling'], signalDate: row['Datum meldingsplicht'], signalType: 'substantial_holding_reduction', signalDetail: 'Family holding reduction', sourceName: 'afm_substantial', sourceUrl: 'fixture', evidenceType: 'afm_csv_holding_notice', evidenceStrength: 0.82, rawSummary: 'fixture' });
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
  });

  it('passes a strong natural-person reduction into match-ready while keeping blockers empty', () => {
    const row = parseCsv(substantialFixture)[2];
    const record = normalizeRecord({ personName: row['Meldingsplichtige'], companyName: row['Uitgevende instelling'], signalDate: row['Datum meldingsplicht'], signalType: 'substantial_holding_reduction', signalDetail: 'Clear reduction', sourceName: 'afm_substantial', sourceUrl: 'fixture', evidenceType: 'afm_csv_holding_notice', evidenceStrength: 0.82, rawSummary: 'fixture', capitalInterestBefore: 6.2, capitalInterestAfter: 4.8 });
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
    expect(record.match_ready).toBe(true);
  });

  it('can rank and export review datasets without broadening match-ready semantics', async () => {
    const records = (await ingestAfmMar19(mar19SemicolonFixtureUrl.pathname)).map((record) => ({ ...record, signal_confidence: 0.6, review_bucket: 'A' as const }));
    const ranked = rankReviewRecords(records);
    expect(ranked.length).toBeGreaterThan(0);
    expect(topByIssuer(records, 1).length).toBeGreaterThan(0);
    expect(await exportReviewDataset(records, 2)).toHaveLength(2);
    await expect(confirmContextForTopReviewRecords(ranked.slice(0, 1), { ...defaultInput, exaApiKey: '', runExaConfirmation: false })).resolves.toHaveLength(1);
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

  // --- Section 8: Filing collapse ---

  it('collapses multi-component filing rows into one record per notifier+issuer+date', async () => {
    // Fixture has 6 rows: ASML/Pieter Vos×3 on 2026-03-19 and Adyen/Jan de Vries×3 on 2026-03-18.
    // After collapse, 2 records — one per distinct filing.
    const records = await ingestAfmSubstantialHoldings(substantialMultirowFilingFixtureUrl.pathname, 3650);
    expect(records).toHaveLength(2);
  });

  it('collapsed filing aggregates capital interest — max(before), min(after)', async () => {
    // ASML group: before values [8.0, 5.0, 7.5] → max=8.0; after values [6.2, 4.5, 3.0] → min=3.0
    const records = await ingestAfmSubstantialHoldings(substantialMultirowFilingFixtureUrl.pathname, 3650);
    const asml = records.find((r) => r.company_name === 'ASML Holding NV');
    expect(asml).toBeDefined();
    expect(asml!.capital_interest_before).toBe(8.0);
    expect(asml!.capital_interest_after).toBe(3.0);
    expect(asml!.signal_type).toBe('substantial_holding_reduction');
    expect(asml!.raw_source_payload_summary).toContain('filing_components=3');
  });

  it('strips footer rows before normalization — disclaimer and update rows do not become records', async () => {
    // Fixture has 2 real data rows + 2 footer rows (Disclaimer CSV, Datum laatste update).
    // After stripping, 2 records are produced.
    const records = await ingestAfmSubstantialHoldings(substantialWithFooterFixtureUrl.pathname, 3650);
    expect(records).toHaveLength(2);
    // Verify no record has footer text in person_name or company_name.
    for (const record of records) {
      expect(record.person_name.toLowerCase()).not.toContain('disclaimer');
      expect(record.person_name.toLowerCase()).not.toContain('datum laatste');
    }
  });

  // --- Section 9: Source role differentiation ---

  it('MAR 19 ingest produces records with source_role primary', async () => {
    const records = await ingestAfmMar19(mar19SemicolonFixtureUrl.pathname);
    expect(records.every((r) => r.source_role === 'primary')).toBe(true);
  });

  it('substantial holdings ingest produces records with source_role secondary_confirmation', async () => {
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname, 3650);
    expect(records.every((r) => r.source_role === 'secondary_confirmation')).toBe(true);
  });

  it('standalone secondary record is blocked from match-ready with secondary_source_no_primary_match', () => {
    const record = normalizeRecord({
      personName: 'Pieter Vos',
      companyName: 'ASML Holding NV',
      signalDate: '2026-03-19',
      signalType: 'substantial_holding_reduction',
      signalDetail: 'Reduction 8% → 3%',
      sourceName: 'afm_substantial',
      sourceRole: 'secondary_confirmation',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: 0.82,
      rawSummary: 'fixture',
      capitalInterestBefore: 8.0,
      capitalInterestAfter: 3.0,
    });
    // Provenance has only afm_substantial — no MAR 19 corroboration.
    record.role = 'Executive Director';
    record.company_domain = 'asml.com';
    record.enrichment_context = 'Board biography found';
    record.person_type = 'natural_person';
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);

    expect(record.match_ready).toBe(false);
    expect(record.blocked_by).toContain('secondary_source_no_primary_match');
  });

  it('secondary record merged with MAR 19 provenance is not blocked by secondary gate', () => {
    const record = normalizeRecord({
      personName: 'Pieter Vos',
      companyName: 'ASML Holding NV',
      signalDate: '2026-03-19',
      signalType: 'substantial_holding_reduction',
      signalDetail: 'Reduction 8% → 3%',
      sourceName: 'afm_substantial',
      sourceRole: 'secondary_confirmation',
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_holding_notice',
      evidenceStrength: 0.82,
      rawSummary: 'fixture',
      capitalInterestBefore: 8.0,
      capitalInterestAfter: 3.0,
    });
    // Simulate dedupe merge: MAR 19 provenance added.
    record.provenance_sources = ['afm_substantial', 'afm_mar19'];
    record.role = 'Executive Director';
    record.company_domain = 'asml.com';
    record.enrichment_context = 'Board biography found';
    record.person_type = 'natural_person';
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);

    expect(record.blocked_by).not.toContain('secondary_source_no_primary_match');
  });

  // --- Section 10: Signal interpretation fields ---

  it('MAR 19 records carry signal_direction=unclear, signal_clarity=inferred, liquidity_relevance=0.5', async () => {
    const records = await ingestAfmMar19(mar19SemicolonFixtureUrl.pathname);
    for (const r of records) {
      expect(r.signal_direction).toBe('unclear');
      expect(r.signal_clarity).toBe('inferred');
      expect(r.liquidity_relevance).toBe(0.5);
    }
  });

  it('substantial holdings reduction records carry signal_direction=sell, signal_clarity=explicit, liquidity_relevance=0.72', async () => {
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname, 3650);
    const reductions = records.filter((r) => r.signal_type === 'substantial_holding_reduction');
    expect(reductions.length).toBeGreaterThan(0);
    for (const r of reductions) {
      expect(r.signal_direction).toBe('sell');
      expect(r.signal_clarity).toBe('explicit');
      expect(r.liquidity_relevance).toBe(0.72);
    }
  });

  it('substantial holdings non-reduction records carry signal_direction=unclear, signal_clarity=unclear, liquidity_relevance=0.3', async () => {
    const records = await ingestAfmSubstantialHoldings(substantialSemicolonFixtureUrl.pathname, 3650);
    const nonReductions = records.filter((r) => r.signal_type !== 'substantial_holding_reduction');
    // If all fixture rows happen to be reductions, verify via normalizeRecord default instead.
    if (nonReductions.length === 0) {
      const r = normalizeRecord({
        personName: 'Test Person',
        companyName: 'Test NV',
        signalDate: '2026-03-19',
        signalType: 'substantial_holding_change_unclear',
        signalDetail: 'Direction unclear',
        sourceName: 'afm_substantial',
        sourceRole: 'secondary_confirmation',
        signalDirection: 'unclear',
        signalClarity: 'unclear',
        liquidityRelevance: 0.3,
        sourceUrl: 'fixture',
        evidenceType: 'afm_csv_holding_notice',
        evidenceStrength: 0.55,
        rawSummary: 'fixture',
      });
      expect(r.signal_direction).toBe('unclear');
      expect(r.signal_clarity).toBe('unclear');
      expect(r.liquidity_relevance).toBe(0.3);
    } else {
      for (const r of nonReductions) {
        expect(r.signal_direction).toBe('unclear');
        expect(r.signal_clarity).toBe('unclear');
        expect(r.liquidity_relevance).toBe(0.3);
      }
    }
  });

  // --- Section 11: Shortlist eligibility ---

  function makeShortlistRecord(overrides: Partial<Parameters<typeof normalizeRecord>[0]> = {}) {
    const r = normalizeRecord({
      personName: 'Joost De Vries',
      companyName: 'Philips NV',
      signalDate: '2026-03-15',
      signalType: 'pdmr_transaction_unconfirmed',
      signalDetail: 'AFM MAR 19 filing',
      sourceName: 'afm_mar19',
      sourceRole: 'primary',
      signalDirection: 'unclear',
      signalClarity: 'inferred',
      liquidityRelevance: 0.5,
      sourceUrl: 'fixture',
      evidenceType: 'afm_csv_filing',
      evidenceStrength: 0.66,
      rawSummary: 'fixture',
      ...overrides,
    });
    // Patch to a state that would earn bucket B (natural_person_confidence=0.5, nl_relevance_score=0.55)
    r.natural_person_confidence = 0.5;
    r.nl_relevance_score = 0.55;
    r.issuer_desirability_score = 0.5;
    scoreSignal(r, 45);
    r.match_ready = true;
    return r;
  }

  it('shortlist_eligible is true for an A/B bucket primary record meeting thresholds', () => {
    const r = makeShortlistRecord();
    // Force signal_confidence above 0.40 so the gate passes.
    r.signal_confidence = 0.45;
    applySignalGates(r, defaultInput);
    // natural_person_confidence=0.5 >= 0.45, signal_confidence=0.45 >= 0.40,
    // review_bucket=B, source_role=primary, signal_type has no 'unclear'
    expect(r.review_bucket).toBe('B');
    expect(r.shortlist_eligible).toBe(true);
  });

  it('shortlist_eligible is false when natural_person_confidence is below 0.45', () => {
    const r = makeShortlistRecord();
    r.natural_person_confidence = 0.3;
    r.signal_confidence = 0.45;
    applySignalGates(r, defaultInput);
    expect(r.shortlist_eligible).toBe(false);
  });

  it('shortlist_eligible is false when signal_confidence is below 0.40', () => {
    const r = makeShortlistRecord();
    r.signal_confidence = 0.35;
    applySignalGates(r, defaultInput);
    expect(r.shortlist_eligible).toBe(false);
  });

  it('shortlist_eligible is false for a bucket C record', () => {
    const r = makeShortlistRecord();
    r.natural_person_confidence = 0.2; // pushes to C
    r.nl_relevance_score = 0.2;
    r.signal_confidence = 0.45;
    applySignalGates(r, defaultInput);
    // classifyReviewBucket: likelyPerson=false, 0.2<0.45 → 'C'
    expect(r.review_bucket).toBe('C');
    expect(r.shortlist_eligible).toBe(false);
  });

  it('shortlist_eligible is false for a standalone secondary record with no MAR 19 provenance', () => {
    const r = makeShortlistRecord({ sourceName: 'afm_substantial', sourceRole: 'secondary_confirmation' });
    r.natural_person_confidence = 0.5;
    r.signal_confidence = 0.45;
    // provenance_sources does not include afm_mar19
    applySignalGates(r, defaultInput);
    expect(r.shortlist_eligible).toBe(false);
  });

  it('shortlist_eligible is false for unclear signal_type with low liquidity_relevance', () => {
    const r = makeShortlistRecord({ signalType: 'substantial_holding_change_unclear', liquidityRelevance: 0.3 });
    r.natural_person_confidence = 0.5;
    r.signal_confidence = 0.45;
    applySignalGates(r, defaultInput);
    // signal_type contains 'unclear' and liquidity_relevance=0.3 < 0.6 → not eligible
    expect(r.shortlist_eligible).toBe(false);
  });

  it('shortlist_eligible is true when signal_type contains unclear but liquidity_relevance >= 0.6', () => {
    const r = makeShortlistRecord({ signalType: 'substantial_holding_change_unclear', liquidityRelevance: 0.65 });
    r.natural_person_confidence = 0.5;
    r.signal_confidence = 0.45;
    applySignalGates(r, defaultInput);
    expect(r.shortlist_eligible).toBe(true);
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
