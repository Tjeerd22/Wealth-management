import { createHash } from 'node:crypto';

const DUTCH_PREFIXES = ['van', 'de', 'der', 'den', 'ter', 'ten', 'te', 'van de', 'van der'];

export function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

export function normalizeName(value: string): string {
  return normalizeWhitespace(value)
    .toLowerCase()
    .replace(/[.,]/g, '')
    .replace(/\s+/g, ' ');
}

export function normalizeCompanyName(value: string): string {
  return normalizeName(value).replace(/\b(bv|b\.v\.|nv|n\.v\.|ltd|plc|sa|sarl|llc|inc|gmbh)\b/g, '').replace(/\s+/g, ' ').trim();
}

export function surnameKey(value: string): string {
  const normalized = normalizeName(value);
  for (const prefix of DUTCH_PREFIXES) {
    if (normalized.startsWith(`${prefix} `)) {
      return normalized;
    }
  }
  return normalized;
}

export function splitName(fullName: string): { firstName: string; lastName: string } {
  const normalized = normalizeWhitespace(fullName);
  // Handle "Lastname, Firstname" format used by AFM substantial holdings exports.
  const commaIdx = normalized.indexOf(',');
  if (commaIdx > 0) {
    const beforeComma = normalized.slice(0, commaIdx).trim();
    const afterComma = normalized.slice(commaIdx + 1).trim();
    if (afterComma) return { firstName: afterComma, lastName: beforeComma };
  }
  const parts = normalized.split(' ');
  if (parts.length <= 1) return { firstName: parts[0] ?? '', lastName: parts[0] ?? '' };
  return { firstName: parts.slice(0, -1).join(' '), lastName: parts.at(-1) ?? '' };
}

export function deterministicId(...parts: string[]): string {
  const key = parts.map((part) => normalizeWhitespace(part || '')).join('|');
  return createHash('sha1').update(key).digest('hex').slice(0, 16);
}

export function clamp01(value: number): number {
  return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}

export function titleCase(value: string): string {
  return value
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}
