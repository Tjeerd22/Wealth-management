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
    expect(record.natural_person_confidence).toBeLessThan(0.3);
  });

  it('scores a likely natural person from MAR 19', () => {
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
    expect(record.natural_person_confidence).toBeGreaterThan(0.55);
  });

  it('handles initials plus surname as moderate confidence', () => {
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
    expect(record.natural_person_confidence).toBeGreaterThanOrEqual(0.4);
    expect(record.natural_person_confidence).toBeLessThan(0.8);
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

  it('merges likely duplicates across initials and full name', () => {
    const a = normalizeRecord({ personName: 'J. van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-02', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'A', sourceName: 'afm_mar19', sourceUrl: 'a', evidenceType: 'afm_csv_filing', evidenceStrength: 0.66, rawSummary: 'a' });
    const b = normalizeRecord({ personName: 'Jan van Dijk', companyName: 'ASML Holding NV', signalDate: '2026-03-03', signalType: 'pdmr_transaction_unconfirmed', signalDetail: 'B', sourceName: 'afm_mar19', sourceUrl: 'b', evidenceType: 'afm_csv_filing', evidenceStrength: 0.7, rawSummary: 'b' });
    const deduped = dedupeSignals([a, b]);
    expect(deduped).toHaveLength(1);
    expect(deduped[0].notes.join(' ')).toContain('Likely duplicate');
  });

  it('keeps ambiguous family holding as review-only', () => {
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
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.match_ready).toBe(false);
  });

  it('passes a strong natural-person reduction into match-ready', () => {
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
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.signal_confidence).toBeGreaterThanOrEqual(defaultInput.minSignalConfidence);
    expect(record.match_ready).toBe(true);
  });

  it('caps MAR 19 record due to insufficient evidence', () => {
    const record = normalizeRecord({
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
    applyInstitutionalFilter(record);
    record.natural_person_confidence = scoreNaturalPersonConfidence(record);
    record.role = 'CFO';
    record.company_domain = 'adyen.com';
    scoreSignal(record, 45);
    record.match_ready = true;
    applySignalGates(record, defaultInput);
    expect(record.signal_confidence).toBeLessThanOrEqual(0.72);
  });
});
