export interface RateLimitResult {
  allowed: boolean;
  retryAfterMs?: number;
}

export class RateLimiter {
  private timestamps = new Map<string, number[]>();
  private readonly maxRequests: number;
  private readonly windowMs: number;

  constructor(maxRequests = 5, windowMs = 10000) {
    this.maxRequests = maxRequests;
    this.windowMs = windowMs;
  }

  check(handle: string): RateLimitResult {
    const now = Date.now();
    const windowStart = now - this.windowMs;
    const list = this.timestamps.get(handle) ?? [];
    const valid = list.filter((t) => t > windowStart);

    if (valid.length >= this.maxRequests) {
      const oldest = valid[0] ?? now;
      const retryAfterMs = Math.max(0, oldest + this.windowMs - now);
      return { allowed: false, retryAfterMs };
    }

    valid.push(now);
    this.timestamps.set(handle, valid);
    return { allowed: true };
  }

  reset(): void {
    this.timestamps.clear();
  }
}
