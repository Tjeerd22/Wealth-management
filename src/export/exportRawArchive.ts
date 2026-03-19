import { Dataset } from 'apify';
import { NormalizedSignalRecord } from '../types.js';

export async function exportRawArchive(records: NormalizedSignalRecord[]): Promise<void> {
  await Dataset.pushData(records);
}
