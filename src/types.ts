/**
 * A transaction identity: the network it was sent to and its hash.
 *
 * Consumers sign and send transactions themselves. The reconciler only ever
 * reads the chain, so this pair is the whole input required to recover an
 * outcome later.
 */
export interface TxRef {
  /** Network identifier, for example an EVM chain id such as `"1"`. */
  readonly chainId: string;
  /** Transaction hash. Compared case-insensitively and stored lowercased. */
  readonly hash: string;
}

export const TERMINAL_STATUSES = ['confirmed', 'failed', 'unknown'] as const;

export type TerminalStatus = (typeof TERMINAL_STATUSES)[number];

/**
 * `pending` is the only status the worker keeps polling.
 * The other three are final for this observation.
 */
export type ObservationStatus = 'pending' | TerminalStatus;

/** Receipt as returned by a network adapter. */
export interface ChainReceipt {
  readonly chainId: string;
  readonly hash: string;
  /** Decimal block height. `bigint` is accepted and stored as a decimal string. */
  readonly blockNumber: string | number | bigint;
  /** `false` means the transaction was mined but execution reverted. */
  readonly success: boolean;
  /** Human-readable failure reason, normally present when `success` is false. */
  readonly reason?: string;
  /** JSON-serializable execution result, if the adapter can expose one. */
  readonly result?: unknown;
}

/** Receipt in the normalized form that is persisted and returned to callers. */
export interface Receipt {
  readonly chainId: string;
  readonly hash: string;
  readonly blockNumber: string;
  readonly success: boolean;
  readonly reason?: string;
  readonly result?: unknown;
}

/**
 * What a chain adapter knows at the moment of a lookup.
 *
 * Returning `final` is the adapter's decision: it has applied whatever
 * confirmation count or finality rule the network needs. The reconciler never
 * guesses finality from block depth.
 */
export type LookupResult =
  | { readonly kind: 'notFound'; readonly reason?: string }
  | { readonly kind: 'pending'; readonly reason?: string }
  | { readonly kind: 'final'; readonly receipt: ChainReceipt };

export interface LookupOptions {
  /** Aborted when the reconciler's RPC timeout expires. Adapters should pass it to the RPC client. */
  readonly signal: AbortSignal;
  /** Same value as the abort deadline, exposed for adapters that cannot use `signal`. */
  readonly timeoutMs: number;
}

/** Read-only view of one chain. Identical hashes on different chains are unrelated. */
export interface ChainReader {
  lookup(ref: TxRef, options: LookupOptions): Promise<LookupResult>;
}

/** Injectable time source. Tests replace it to make retries and deadlines deterministic. */
export interface Clock {
  now(): Date;
}

export const systemClock: Clock = {
  now: () => new Date(),
};

/**
 * Persisted observation.
 *
 * Times are ISO-8601 strings in UTC so they sort lexicographically in SQLite.
 * `version` increases on every successful update and is used for optimistic
 * concurrency between workers.
 */
export interface Observation {
  readonly ref: TxRef;
  readonly status: ObservationStatus;
  /** The moment after which a still-unresolved transaction becomes `unknown`. */
  readonly deadline: string;
  /** Number of completed lookup attempts (including failed RPC calls). */
  readonly attempts: number;
  readonly nextAttemptAt: string;
  readonly version: number;
  /** Last diagnostic cause: RPC error, missing transaction, receipt mismatch, stop reason. */
  readonly reason?: string;
  readonly receipt?: Receipt;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface NewObservation {
  readonly ref: TxRef;
  readonly deadline: string;
  readonly nextAttemptAt: string;
  readonly createdAt: string;
}

/**
 * A conditional transition of a pending observation.
 * The store must apply it only if the stored status is `pending` and the
 * version still equals the one the worker read.
 */
export interface ObservationUpdate {
  readonly status: ObservationStatus;
  readonly attempts: number;
  readonly nextAttemptAt: string;
  readonly reason?: string;
  readonly receipt?: Receipt;
  readonly updatedAt: string;
}

/**
 * Storage contract.
 *
 * `createIfAbsent` is idempotent on `(chainId, hash)` and must never move the
 * deadline of an existing observation. `compareAndSet` is the only writer used
 * by workers; it returns `false` when another worker won the race.
 */
export interface Store {
  createIfAbsent(input: NewObservation): Promise<Observation>;
  get(ref: TxRef): Promise<Observation | null>;
  /** Pending observations whose `nextAttemptAt` is at or before `now`, oldest first. */
  due(now: string, limit: number): Promise<Observation[]>;
  compareAndSet(
    ref: TxRef,
    expectedVersion: number,
    update: ObservationUpdate,
  ): Promise<boolean>;
  close?(): Promise<void> | void;
}

export interface ReconcilerOptions {
  readonly store: Store;
  readonly reader: ChainReader;
  readonly clock?: Clock;
  /** How long an observation may stay unresolved. Default: 24 hours. */
  readonly watchForMs?: number;
  /** Delay before the second attempt. Default: 15 seconds. */
  readonly initialRetryMs?: number;
  /** Upper bound for the exponential retry delay. Default: 5 minutes. */
  readonly maxRetryMs?: number;
  /** Exponential backoff multiplier. Default: 2. */
  readonly retryFactor?: number;
  /** Per-lookup RPC deadline. Default: 10 seconds. */
  readonly rpcTimeoutMs?: number;
  /** Maximum observations handled by one `runDue` call. Default: 100. */
  readonly batchSize?: number;
}

/** Counters for one `runDue` batch. Only `pending` may be picked up again. */
export interface RunSummary {
  /** Observations read from the store and attempted in this batch. */
  readonly scanned: number;
  readonly confirmed: number;
  readonly failed: number;
  readonly unknown: number;
  /** Lookups that ended without a final receipt but still have time left. */
  readonly pending: number;
  /** Observations another worker updated first; this worker's result was discarded. */
  readonly skipped: number;
}
