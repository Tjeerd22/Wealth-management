import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { parseCsv } from '../../src/utils/csv.js';
import { normalizeRecord } from '../../src/normalize/normalizeRecord.js';
import { applyInstitutionalFilter } from '../../src/filters/institutionalFilter.js';
import { scoreNaturalPersonConfidence } from '../../src/filters/personConfidence.js';
import { dedupeSignals } from '../../src/dedupe/dedupeSignals.js';
import { scoreSignal } from '../../src/scoring/scoreSignal.js';
import { applySignalGates } from '../../src/filters/signalGates.js';
import { defaultInput } from '../../src/config.js';
import { scoreNlRelevance } from '../../src/scoring/scoreNlRelevance.js';
import { exportReviewDataset } from '../../src/export/exportReviewDataset.js';

const mar19Fixture = readFileSync(new URL('../fixtures/afm_mar19_sample.csv', import.meta.url), 'utf8');
const substantialFixture = readFileSync(new URL('../fixtures/afm_substantial_sample.csv', import.meta.url), 'utf8');

describe('connector os dutch liquidity pipeline', () => {
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


  it('removes exact duplicate rows without dropping distinct dated events', () => {
    const a = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(1);
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
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.match_ready).toBe(false);
    expect(record.blocked_by).toContain('low_natural_person_confidence');
    expect(record.review_bucket).toBe('C');
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
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.signal_confidence).toBeGreaterThanOrEqual(defaultInput.minSignalConfidence);
    expect(record.match_ready).toBe(true);
    expect(record.blocked_by).toHaveLength(0);
    expect(record.review_bucket).toBe('A');
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

  it('sorts review exports by bucket first and then confidence', async () => {
    const a = normalizeRecord({ personName: 'Jan de Vries', companyName: 'Adyen NV', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    const b = normalizeRecord({ personName: 'Klaas', companyName: 'Random Plc', signalDate: '2026-03-01', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'fixture', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'fixture' });
    a.natural_person_confidence = 0.78; a.nl_relevance_score = 0.8; a.signal_confidence = 0.52; a.review_bucket = 'A';
    b.natural_person_confidence = 0.3; b.nl_relevance_score = 0.35; b.signal_confidence = 0.7; b.review_bucket = 'C';
    const review = await exportReviewDataset([b, a], 10);
    expect(review[0].review_bucket).toBe('A');
    expect(review[1].review_bucket).toBe('C');
  });
});
