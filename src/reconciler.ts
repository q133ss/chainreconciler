import { lookupWithTimeout } from './lookup.js';
import { describeRef, normalizeRef, sameRef } from './ref.js';
import {
  systemClock,
  type ChainReceipt,
  type Clock,
  type Observation,
  type ObservationStatus,
  type ObservationUpdate,
  type Receipt,
  type ReconcilerOptions,
  type RunSummary,
  type Store,
  type TxRef,
} from './types.js';

const HOUR_MS = 60 * 60 * 1000;

export const DEFAULT_WATCH_FOR_MS = 24 * HOUR_MS;
export const DEFAULT_INITIAL_RETRY_MS = 15 * 1000;
export const DEFAULT_MAX_RETRY_MS = 5 * 60 * 1000;
export const DEFAULT_RPC_TIMEOUT_MS = 10 * 1000;
export const DEFAULT_BATCH_SIZE = 100;

interface Settings {
  readonly watchForMs: number;
  readonly initialRetryMs: number;
  readonly maxRetryMs: number;
  readonly retryFactor: number;
  readonly rpcTimeoutMs: number;
  readonly batchSize: number;
}

type CheckOutcome = 'confirmed' | 'failed' | 'unknown' | 'pending' | 'skipped';

/**
 * Recovers the outcome of transactions that were already signed and sent by
 * the caller. The reconciler never signs, sends, or retries a transaction.
 *
 * Typical loop:
 * 1. `track(ref)` before sending, so the observation survives a crash.
 * 2. Send the transaction with the application's own client.
 * 3. Call `runDue()` on startup and on an application timer.
 * 4. Read `get(ref)` from any process sharing the store.
 */
export class Reconciler {
  private readonly store: Store;
  private readonly reader: ReconcilerOptions['reader'];
  private readonly clock: Clock;
  private readonly settings: Settings;

  constructor(options: ReconcilerOptions) {
    if (options === null || typeof options !== 'object') {
      throw new TypeError('reconciler options are required');
    }
    if (options.store === undefined || options.reader === undefined) {
      throw new TypeError('reconciler requires store and reader');
    }

    this.store = options.store;
    this.reader = options.reader;
    this.clock = options.clock ?? systemClock;
    this.settings = resolveSettings(options);
  }

  /**
   * Persists an observation before the transaction is sent.
   *
   * Idempotent: a second call with the same `(chainId, hash)` returns the
   * existing observation and leaves its deadline, status, attempts, and
   * receipt untouched.
   */
  async track(ref: TxRef): Promise<Observation> {
    const normalized = normalizeRef(ref);
    const now = this.clock.now();
    return this.store.createIfAbsent({
      ref: normalized,
      deadline: new Date(now.getTime() + this.settings.watchForMs).toISOString(),
      nextAttemptAt: now.toISOString(),
      createdAt: now.toISOString(),
    });
  }

  /** Returns the current observation, or `null` if `track` was never called. */
  async get(ref: TxRef): Promise<Observation | null> {
    return this.store.get(normalizeRef(ref));
  }

  /**
   * Processes at most `batchSize` observations whose retry time has come.
   *
   * Safe to call from several processes against the same store: every update
   * is conditional on the version that was read, so a slow response cannot
   * overwrite a result written by another worker.
   */
  async runDue(): Promise<RunSummary> {
    const due = await this.store.due(
      this.clock.now().toISOString(),
      this.settings.batchSize,
    );
    const counters: Record<CheckOutcome, number> = {
      confirmed: 0,
      failed: 0,
      unknown: 0,
      pending: 0,
      skipped: 0,
    };

    for (const observation of due) {
      if (observation.status !== 'pending') {
        continue;
      }
      counters[await this.check(observation)] += 1;
    }

    return {
      scanned: due.length,
      confirmed: counters.confirmed,
      failed: counters.failed,
      unknown: counters.unknown,
      pending: counters.pending,
      skipped: counters.skipped,
    };
  }

  /** Releases the underlying store when it owns resources. */
  async close(): Promise<void> {
    await this.store.close?.();
  }

  private async check(observation: Observation): Promise<CheckOutcome> {
    const now = this.clock.now();
    const ref = normalizeRef(observation.ref);
    let diagnostic: string | undefined;
    let receipt: Receipt | undefined;
    let terminal: 'confirmed' | 'failed' | undefined;

    try {
      const result = await lookupWithTimeout(
        this.reader,
        ref,
        this.settings.rpcTimeoutMs,
      );

      switch (result.kind) {
        case 'final': {
          const normalized = normalizeReceipt(result.receipt);
          if (normalized === null) {
            diagnostic = 'chain reader returned a malformed final receipt';
          } else if (!sameRef(normalized, ref)) {
            diagnostic =
              `final receipt ref mismatch: expected ${describeRef(ref)}, ` +
              `got ${describeRef(normalized)}`;
          } else {
            receipt = normalized;
            terminal = normalized.success ? 'confirmed' : 'failed';
          }
          break;
        }
        case 'notFound':
          diagnostic = result.reason ?? 'transaction not found';
          break;
        case 'pending':
          diagnostic = result.reason ?? 'transaction is not final yet';
          break;
        default:
          diagnostic = 'chain reader returned an unknown lookup result';
      }
    } catch (error) {
      diagnostic = `RPC lookup failed: ${errorMessage(error)}`;
    }

    const attempts = observation.attempts + 1;
    const updatedAt = now.toISOString();

    if (terminal !== undefined && receipt !== undefined) {
      const status: ObservationStatus = terminal;
      const reason =
        terminal === 'failed'
          ? receipt.reason?.trim() || 'execution failed'
          : undefined;
      return this.commit(observation, {
        status,
        attempts,
        nextAttemptAt: updatedAt,
        reason,
        receipt,
        updatedAt,
      }, terminal);
    }

    const deadline = Date.parse(observation.deadline);
    const hasDeadline = Number.isFinite(deadline);
    if (!hasDeadline || now.getTime() >= deadline) {
      const stopReason =
        `watch deadline expired after ${attempts} attempt(s)` +
        (diagnostic === undefined ? '' : `: ${diagnostic}`);
      return this.commit(observation, {
        status: 'unknown',
        attempts,
        nextAttemptAt: updatedAt,
        reason: stopReason,
        updatedAt,
      }, 'unknown');
    }

    const delay = Math.min(
      this.settings.initialRetryMs * this.settings.retryFactor ** (attempts - 1),
      this.settings.maxRetryMs,
    );
    const next = Math.min(now.getTime() + delay, deadline);

    return this.commit(observation, {
      status: 'pending',
      attempts,
      nextAttemptAt: new Date(next).toISOString(),
      reason: diagnostic,
      updatedAt,
    }, 'pending');
  }

  private async commit(
    observation: Observation,
    update: ObservationUpdate,
    outcome: CheckOutcome,
  ): Promise<CheckOutcome> {
    const applied = await this.store.compareAndSet(
      observation.ref,
      observation.version,
      update,
    );
    return applied ? outcome : 'skipped';
  }
}

function resolveSettings(options: ReconcilerOptions): Settings {
  const watchForMs = positiveNumber(
    options.watchForMs,
    DEFAULT_WATCH_FOR_MS,
    'watchForMs',
  );
  const initialRetryMs = positiveNumber(
    options.initialRetryMs,
    DEFAULT_INITIAL_RETRY_MS,
    'initialRetryMs',
  );
  const maxRetryMs = positiveNumber(
    options.maxRetryMs,
    DEFAULT_MAX_RETRY_MS,
    'maxRetryMs',
  );
  const rpcTimeoutMs = positiveNumber(
    options.rpcTimeoutMs,
    DEFAULT_RPC_TIMEOUT_MS,
    'rpcTimeoutMs',
  );
  const retryFactor = positiveNumber(options.retryFactor, 2, 'retryFactor');
  const batchSize = positiveNumber(
    options.batchSize,
    DEFAULT_BATCH_SIZE,
    'batchSize',
  );

  if (retryFactor < 1) {
    throw new RangeError('retryFactor must be at least 1');
  }
  if (maxRetryMs < initialRetryMs) {
    throw new RangeError('maxRetryMs must be greater than or equal to initialRetryMs');
  }
  if (!Number.isInteger(batchSize)) {
    throw new RangeError('batchSize must be an integer');
  }

  return {
    watchForMs,
    initialRetryMs,
    maxRetryMs,
    retryFactor,
    rpcTimeoutMs,
    batchSize,
  };
}

function positiveNumber(
  value: number | undefined,
  fallback: number,
  name: string,
): number {
  const resolved = value ?? fallback;
  if (typeof resolved !== 'number' || !Number.isFinite(resolved) || resolved <= 0) {
    throw new RangeError(`${name} must be a positive finite number`);
  }
  return resolved;
}

function normalizeReceipt(input: ChainReceipt): Receipt | null {
  if (input === null || typeof input !== 'object') {
    return null;
  }
  if (typeof input.chainId !== 'string' || typeof input.hash !== 'string') {
    return null;
  }
  if (typeof input.success !== 'boolean') {
    return null;
  }

  const blockNumber =
    typeof input.blockNumber === 'bigint' ||
    typeof input.blockNumber === 'number' ||
    typeof input.blockNumber === 'string'
      ? input.blockNumber.toString()
      : '';

  if (!/^\d+$/.test(blockNumber)) {
    return null;
  }

  return {
    chainId: input.chainId.trim(),
    hash: input.hash.trim().toLowerCase(),
    blockNumber,
    success: input.success,
    ...(typeof input.reason === 'string' ? { reason: input.reason } : {}),
    ...(input.result !== undefined ? { result: input.result } : {}),
  };
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
