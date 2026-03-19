export function logInfo(message: string, data?: unknown): void {
  if (data === undefined) {
    console.log(`[INFO] ${message}`);
    return;
  }
  console.log(`[INFO] ${message}`, data);
}

export function logWarn(message: string, data?: unknown): void {
  if (data === undefined) {
    console.warn(`[WARN] ${message}`);
    return;
  }
  console.warn(`[WARN] ${message}`, data);
}
