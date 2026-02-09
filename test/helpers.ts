import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import type {
  ChainReader,
  Clock,
  LookupOptions,
  LookupResult,
  TxRef,
} from '../src/index.js';

/** Deterministic clock used by every test that exercises retries or deadlines. */
export class TestClock implements Clock {
  private current: Date;

  constructor(start = '2026-02-03T09:00:00.000Z') {
    this.current = new Date(start);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export type LookupHandler = (
  ref: TxRef,
  options: LookupOptions,
  call: number,
) => LookupResult | Promise<LookupResult>;

/** Chain reader driven by a handler, with a call log for assertions. */
export class FakeReader implements ChainReader {
  readonly calls: TxRef[] = [];

  constructor(private readonly handler: LookupHandler) {}

  lookup(ref: TxRef, options: LookupOptions): Promise<LookupResult> {
    this.calls.push(ref);
    return Promise.resolve(this.handler(ref, options, this.calls.length));
  }
}

export interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (reason?: unknown) => void;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export async function createTempDb(): Promise<{
  readonly path: string;
  readonly cleanup: () => Promise<void>;
}> {
  const directory = await mkdtemp(join(tmpdir(), 'chainreconciler-'));
  return {
    path: join(directory, 'observations.db'),
    cleanup: () => rm(directory, { recursive: true, force: true }),
  };
}

export function successReceipt(
  chainId: string,
  hash: string,
  blockNumber: number | bigint | string = 100,
  result: unknown = { ok: true },
) {
  return {
    chainId,
    hash,
    blockNumber,
    success: true,
    result,
  } as const;
}

export function failedReceipt(
  chainId: string,
  hash: string,
  reason: string,
  blockNumber: number | bigint | string = 100,
) {
  return {
    chainId,
    hash,
    blockNumber,
    success: false,
    reason,
  } as const;
}
