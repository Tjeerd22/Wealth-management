import { ActorInput } from './types.js';

export const DEFAULT_AFM_MAR19_CSV_URL = 'https://www.afm.nl/~/profmedia/files/registers/mar19-transactions.csv';
export const DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL = 'https://www.afm.nl/~/profmedia/files/registers/substantial-holdings.csv';

export const defaultInput: ActorInput = {
  runAfmMar19: true,
  runAfmSubstantialHoldings: true,
  runExaEnrichment: false,
  afmMar19CsvUrl: DEFAULT_AFM_MAR19_CSV_URL,
  afmSubstantialHoldingsCsvUrl: DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL,
  lookbackDays: 45,
  minSignalConfidence: 0.6,
  minNaturalPersonConfidence: 0.6,
  excludeInstitutions: true,
  maxReviewRecords: 100,
  maxMatchReadyRecords: 30,
  exaApiKey: '',
  debug: false,
};
