import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { fetchCsvRows, parseCsv } from '../../src/utils/csv.js';
import { ingestAfmMar19, AFM_MAR19_REQUIRED_COLUMNS } from '../../src/sources/afmMar19.js';
import { ingestAfmSubstantialHoldings, AFM_SUBSTANTIAL_REQUIRED_COLUMNS } from '../../src/sources/afmSubstantialHoldings.js';
import { normalizeRecord } from '../../src/normalize/normalizeRecord.js';
import { logNormalizationHealth, validateSourceSchema } from '../../src/normalize/sourceNormalization.js';
import { applyInstitutionalFilter } from '../../src/filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from '../../src/filters/personConfidence.js';
import { dedupeSignals } from '../../src/dedupe/dedupeSignals.js';
import { scoreSignal } from '../../src/scoring/scoreSignal.js';
import { applySignalGates } from '../../src/filters/signalGates.js';
import { defaultInput } from '../../src/config.js';
import { scoreNlRelevance } from '../../src/scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from '../../src/scoring/scoreIssuerDesirability.js';
import { exportReviewDataset, rankReviewRecords, topByIssuer } from '../../src/export/exportReviewDataset.js';
import { confirmContextForTopReviewRecords } from '../../src/enrich/confirmContextForTopReviewRecords.js';
import { appendRecords, loadSourceWithPolicy } from '../../src/main.js';

const mar19Fixture = readFileSync(new URL('../fixtures/afm_mar19_sample.csv', import.meta.url), 'utf8');
const substantialFixture = readFileSync(new URL('../fixtures/afm_substantial_sample.csv', import.meta.url), 'utf8');
const mar19SemicolonFixtureUrl = new URL('../fixtures/afm_mar19_semicolon_sample.csv', import.meta.url);
const mar19SemicolonFixture = readFileSync(mar19SemicolonFixtureUrl, 'utf8');
const substantialSemicolonFixtureUrl = new URL('../fixtures/afm_substantial_semicolon_sample.csv', import.meta.url);
const substantialSemicolonFixture = readFileSync(substantialSemicolonFixtureUrl, 'utf8');

describe('connector os dutch liquidity pipeline', () => {
  it('parses AFM-style semicolon CSV with BOM, whitespace, quotes, and empty lines', () => {
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
});
