import { ActorInput } from './types.js';

export const AFM_MAR19_HTML_BASE_URL = 'https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-';

export const defaultInput: ActorInput = {
  dateFrom: '',
  maxPages: 20,
  runExaConfirmation: false,
  minSignalConfidence: 0.6,
  minNaturalPersonConfidence: 0.6,
  minReviewPriorityScore: 0.4,
  excludeInstitutions: true,
  maxReviewRecords: 100,
  maxShortlistRecords: 60,
  topBucketBForExa: 5,
  exaApiKey: '',
  exaFreshnessMaxAgeHours: 72,
  debug: false,
};
