export function parseDate(input: string): Date | null {
  if (!input) return null;
  const date = new Date(input);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function toIsoDate(input: string | Date): string {
  const date = typeof input === 'string' ? parseDate(input) : input;
  if (!date) return '';
  return date.toISOString().slice(0, 10);
}

export function isWithinLookback(signalDate: string, lookbackDays: number, referenceDate = new Date()): boolean {
  const date = parseDate(signalDate);
  if (!date) return false;
  const diffMs = referenceDate.getTime() - date.getTime();
  return diffMs >= 0 && diffMs <= lookbackDays * 24 * 60 * 60 * 1000;
}

/**
 * Convert a Date or ISO string to dd-mm-yyyy format for AFM URL parameters.
 */
export function toAfmDateParam(input: string | Date): string {
  const date = typeof input === 'string' ? parseDate(input) : input;
  if (!date) return '';
  const dd = String(date.getUTCDate()).padStart(2, '0');
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const yyyy = date.getUTCFullYear();
  return `${dd}-${mm}-${yyyy}`;
}

export function dayDistance(a: string, b: string): number {
  const dateA = parseDate(a);
  const dateB = parseDate(b);
  if (!dateA || !dateB) return Number.MAX_SAFE_INTEGER;
  return Math.abs(dateA.getTime() - dateB.getTime()) / (24 * 60 * 60 * 1000);
}
