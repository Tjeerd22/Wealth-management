import { describe, expect, it, vi } from 'vitest';
import { normalizeRecord } from '../../src/normalize/normalizeRecord.js';
import { logNormalizationHealth, validateRequiredColumns, validateSourceSchema } from '../../src/normalize/sourceNormalization.js';
import { applyInstitutionalFilter } from '../../src/filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from '../../src/filters/personConfidence.js';
import { scoreSignal } from '../../src/scoring/scoreSignal.js';
import { applySignalGates } from '../../src/filters/signalGates.js';
import { defaultInput } from '../../src/config.js';
import { scoreNlRelevance } from '../../src/scoring/scoreNlRelevance.js';
import { scoreIssuerDesirability } from '../../src/scoring/scoreIssuerDesirability.js';
import { exportReviewDataset, rankReviewRecords, topByIssuer } from '../../src/export/exportReviewDataset.js';
import { confirmContextForTopReviewRecords } from '../../src/enrich/confirmContextForTopReviewRecords.js';

vi.mock('apify', () => ({
  Actor: { setValue: vi.fn(async () => undefined), init: vi.fn(), exit: vi.fn(), getInput: vi.fn(async () => null) },
  Dataset: { pushData: vi.fn(async () => undefined), open: vi.fn(async () => ({ pushData: vi.fn(async () => undefined) })) },
}));

describe('connector os dutch liquidity pipeline', () => {
  it('fails fast when a source loses canonical identity coverage on too many rows', () => {
    const records = [0, 1, 2].map((index) => normalizeRecord({ personName: '', companyName: '', signalDate: `2026-03-1${index}`, signalType: 'pdmr_transaction_unconfirmed', signalDetail: `row-${index}`, sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' }));
    expect(() => logNormalizationHealth('afm_mar19_html', records)).toThrow(/Normalization health check failed/);
  });

  it('scores a likely natural person in the stronger personal-name band', () => {
    const record = normalizeRecord({ personName: 'Vries, Jan de', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Thin but timely MAR 19 record', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.7);
    expect(record.natural_person_confidence).toBeLessThanOrEqual(0.85);
  });

  it('handles initials plus surname as moderate confidence rather than near-failure', () => {
    const record = normalizeRecord({ personName: 'J. de Vries', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Initials case', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.5);
    expect(record.natural_person_confidence).toBeLessThanOrEqual(0.65);
  });

  it('treats dutch comma-prefix human names as strong personal patterns', () => {
    const record = normalizeRecord({ personName: 'Dijk, Van J.', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Dutch surname prefix case', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.78);
  });

  it('preserves Dutch surname prefixes for dedupe keys', () => {
    const record = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Prefix case', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    expect(record.person_last_name).toBe('dijk');
  });

  it('can rank and export review datasets', async () => {
    const records = [
      normalizeRecord({ personName: 'Jansen, Eva', companyName: 'Universal Music Group N.V.', signalDate: '2026-03-19', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'test', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' }),
      normalizeRecord({ personName: 'Vries, Jan de', companyName: 'Adyen NV', signalDate: '2026-03-18', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'test', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' }),
    ].map((record) => ({ ...record, signal_confidence: 0.6, review_bucket: 'A' as const }));
    const ranked = rankReviewRecords(records);
    expect(ranked.length).toBeGreaterThan(0);
    expect(topByIssuer(records, 1).length).toBeGreaterThan(0);
    expect(await exportReviewDataset(records, 2)).toHaveLength(2);
    await expect(confirmContextForTopReviewRecords(ranked.slice(0, 1), { ...defaultInput, exaApiKey: '', runExaConfirmation: false })).resolves.toHaveLength(1);
  });

  it('fails fast when CSV has zero rows (cannot validate schema contract)', () => {
    expect(() => validateRequiredColumns([], ['Transactie', 'Uitgevende instelling'], 'AFM MAR 19'))
      .toThrow(/zero rows/);
  });

  it('scores signal with afm_mar19_html source name using the correct source quality', () => {
    const record = normalizeRecord({ personName: 'Jansen, Eva', companyName: 'Universal Music Group N.V.', signalDate: '2026-03-19', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'test', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, 45);
    // signal_confidence should be capped at 0.58 for unconfirmed types
    expect(record.signal_confidence).toBeLessThanOrEqual(0.58);
    expect(record.signal_confidence).toBeGreaterThan(0);
  });

  it('shortlist eligibility works with primary source records', () => {
    const record = normalizeRecord({ personName: 'Jansen, Eva', companyName: 'Universal Music Group N.V.', signalDate: '2026-03-19', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'test', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.nl_relevance_score = scoreNlRelevance(record);
    record.issuer_desirability_score = scoreIssuerDesirability(record);
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    // Natural person with decent score should be shortlist eligible if in bucket A/B
    if (record.review_bucket === 'A' || record.review_bucket === 'B') {
      expect(record.shortlist_eligible).toBe(record.signal_confidence >= 0.40 && record.natural_person_confidence >= 0.45);
    }
  });

  it('flags institution records correctly', () => {
    const record = normalizeRecord({ personName: 'Bank Of America Corporation', companyName: 'Pharming Group N.V.', signalDate: '2026-03-19', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'Institution test', sourceName: 'afm_mar19_html', sourceUrl: 'fixture', evidenceType: 'afm_html_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    expect(record.institutional_risk).toBe('high');
    expect(record.natural_person_confidence).toBeLessThan(0.2);
  });
});
