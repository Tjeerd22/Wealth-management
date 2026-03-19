declare module 'apify' {
  export const Actor: {
    init(): Promise<void>;
    exit(): Promise<void>;
    getInput<T>(): Promise<T | null>;
    setValue(key: string, value: unknown): Promise<void>;
  };
  export const Dataset: {
    pushData(data: unknown): Promise<void>;
    open(name?: string): Promise<{ pushData(data: unknown): Promise<void> }>;
  };
}

declare module 'csv-parse/sync' {
  export function parse(input: string, options: Record<string, unknown>): Record<string, string>[];
}

declare module 'node:crypto' {
  export function createHash(algorithm: string): { update(data: string): { digest(encoding: string): string } };
}

declare module 'node:fs' {
  export function readFileSync(path: string | URL, encoding: string): string;
}

declare module 'vitest' {
  export function describe(name: string, fn: () => void): void;
  export function it(name: string, fn: () => void): void;
  export function expect(value: unknown): {
    toBe(expected: unknown): void;
    toBeLessThan(expected: number): void;
    toBeGreaterThan(expected: number): void;
    toBeGreaterThanOrEqual(expected: number): void;
    toBeLessThanOrEqual(expected: number): void;
    toHaveLength(expected: number): void;
    toContain(expected: string): void;
  };
}
