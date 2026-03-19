import { ActorInput } from './types.js';

export const DEFAULT_AFM_MAR19_CSV_URL = 'https://www.afm.nl/export.aspx?format=csv&type=0ee836dc-5520-459d-bcf4-a4a689de6614';
export const DEFAULT_AFM_SUBSTANTIAL_HOLDINGS_CSV_URL = 'https://www.afm.nl/export.aspx?format=csv&type=1331d46f-3fb6-4a36-b903-9584972675af';

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
