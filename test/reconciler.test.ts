import { afterEach, describe, expect, it } from 'vitest';

import {
  Reconciler,
  SqliteStore,
  type ChainReader,
  type LookupResult,
  type ReconcilerOptions,
} from '../src/index.js';
import {
  FakeReader,
  TestClock,
  failedReceipt,
  successReceipt,
} from './helpers.js';

const T0 = '2026-02-03T09:00:00.000Z';

const stores: SqliteStore[] = [];

function setup(
  reader: ChainReader,
  options: Partial<Omit<ReconcilerOptions, 'store' | 'reader'>> = {},
) {
  const store = new SqliteStore(':memory:');
  const clock = new TestClock(T0);
  const reconciler = new Reconciler({
    store,
    reader,
    clock,
    watchForMs: 3_600_000,
    initialRetryMs: 1_000,
    maxRetryMs: 3_000,
    retryFactor: 2,
    rpcTimeoutMs: 100,
    ...options,
  });
  stores.push(store);
  return { store, clock, reconciler };
}

afterEach(() => {
  for (const store of stores.splice(0)) {
    store.close();
  }
});

describe('Reconciler.track', () => {
  it('stores one observation and keeps its deadline on repeated calls', async () => {
    const reader = new FakeReader(() => ({ kind: 'notFound' }));
    const { store, clock, reconciler } = setup(reader);

    const first = await reconciler.track({ chainId: '1', hash: '0xAABB' });
    expect(first.ref).toEqual({ chainId: '1', hash: '0xaabb' });
    expect(first.status).toBe('pending');
    expect(first.deadline).toBe('2026-02-03T10:00:00.000Z');
    expect(first.version).toBe(0);

    clock.advance(60_000);
    const repeated = await reconciler.track({ chainId: ' 1 ', hash: '0xAABB' });
    expect(repeated).toEqual(first);
    expect(await store.get({ chainId: '1', hash: '0xaabb' })).toEqual(first);
  });

  it('rejects an empty chain id or hash', async () => {
    const reader = new FakeReader(() => ({ kind: 'notFound' }));
    const { reconciler } = setup(reader);

    await expect(
      reconciler.track({ chainId: '', hash: '0xaa' }),
    ).rejects.toThrow(TypeError);
    await expect(
      reconciler.track({ chainId: '1', hash: '   ' }),
    ).rejects.toThrow(TypeError);
  });
});

describe('Reconciler.runDue', () => {
  it('stores a final successful receipt and stops polling', async () => {
    const reader = new FakeReader(() => ({
      kind: 'final',
      receipt: successReceipt('1', '0xaa', 123n, { transfer: 'ok' }),
    }));
    const { clock, reconciler } = setup(reader);
    const ref = { chainId: '1', hash: '0xaa' };
    await reconciler.track(ref);

    const summary = await reconciler.runDue();
    expect(summary).toEqual({
      scanned: 1,
      confirmed: 1,
      failed: 0,
      unknown: 0,
      pending: 0,
      skipped: 0,
    });

    const observation = await reconciler.get(ref);
    expect(observation?.status).toBe('confirmed');
    expect(observation?.attempts).toBe(1);
    expect(observation?.version).toBe(1);
    expect(observation?.receipt).toEqual({
      chainId: '1',
      hash: '0xaa',
      blockNumber: '123',
      success: true,
      result: { transfer: 'ok' },
    });

    clock.advance(86_400_000);
    expect((await reconciler.runDue()).scanned).toBe(0);
    expect(reader.calls).toHaveLength(1);
  });

  it('stores a final execution failure with its reason and stops polling', async () => {
    const reader = new FakeReader(() => ({
      kind: 'final',
      receipt: failedReceipt('1', '0xbb', 'execution reverted: insufficient balance'),
    }));
    const { clock, reconciler } = setup(reader);
    const ref = { chainId: '1', hash: '0xbb' };
    await reconciler.track(ref);

    await reconciler.runDue();

    const observation = await reconciler.get(ref);
    expect(observation?.status).toBe('failed');
    expect(observation?.reason).toBe('execution reverted: insufficient balance');
    expect(observation?.receipt?.success).toBe(false);

    clock.advance(86_400_000);
    expect((await reconciler.runDue()).scanned).toBe(0);
    expect(reader.calls).toHaveLength(1);
  });

  it('keeps polling with increasing capped intervals while the transaction is not final', async () => {
    const reader = new FakeReader(() => ({
      kind: 'pending',
      reason: '3/12 confirmations',
    }));
    const { clock, reconciler } = setup(reader);
    const ref = { chainId: '1', hash: '0xcc' };
    await reconciler.track(ref);

    await reconciler.runDue();
    let observation = await reconciler.get(ref);
    expect(observation?.status).toBe('pending');
    expect(observation?.attempts).toBe(1);
    expect(observation?.nextAttemptAt).toBe('2026-02-03T09:00:01.000Z');
    expect(observation?.reason).toBe('3/12 confirmations');

    clock.advance(1_000);
    await reconciler.runDue();
    observation = await reconciler.get(ref);
    expect(observation?.attempts).toBe(2);
    expect(observation?.nextAttemptAt).toBe('2026-02-03T09:00:03.000Z');

    clock.advance(2_000);
    await reconciler.runDue();
    observation = await reconciler.get(ref);
    expect(observation?.attempts).toBe(3);
    expect(observation?.nextAttemptAt).toBe('2026-02-03T09:00:06.000Z');

    clock.advance(3_000);
    await reconciler.runDue();
    observation = await reconciler.get(ref);
    expect(observation?.attempts).toBe(4);
    expect(observation?.nextAttemptAt).toBe('2026-02-03T09:00:09.000Z');
  });

  it('marks an unresolved observation unknown when the watch deadline expires', async () => {
    const reader = new FakeReader(() => {
      throw new Error('connection reset');
    });
    const { clock, reconciler } = setup(reader, {
      watchForMs: 3_000,
      initialRetryMs: 1_000,
      maxRetryMs: 3_000,
    });
    const ref = { chainId: '1', hash: '0xdd' };
    await reconciler.track(ref);

    await reconciler.runDue();
    clock.advance(1_000);
    await reconciler.runDue();

    let observation = await reconciler.get(ref);
    expect(observation?.status).toBe('pending');
    expect(observation?.attempts).toBe(2);
    expect(observation?.nextAttemptAt).toBe('2026-02-03T09:00:03.000Z');

    clock.advance(2_000);
    await reconciler.runDue();

    observation = await reconciler.get(ref);
    expect(observation?.status).toBe('unknown');
    expect(observation?.attempts).toBe(3);
    expect(observation?.reason).toContain('watch deadline expired');
    expect(observation?.reason).toContain('connection reset');

    clock.advance(86_400_000);
    expect((await reconciler.runDue()).scanned).toBe(0);
  });

  it('treats an RPC timeout as a transient error', async () => {
    const reader = new FakeReader(() => new Promise<LookupResult>(() => {}));
    const { clock, reconciler } = setup(reader, { rpcTimeoutMs: 20 });
    const ref = { chainId: '1', hash: '0xee' };
    await reconciler.track(ref);

    await reconciler.runDue();

    const observation = await reconciler.get(ref);
    expect(observation?.status).toBe('pending');
    expect(observation?.reason).toContain('timed out after 20 ms');

    clock.advance(1_000);
    expect((await reconciler.runDue()).scanned).toBe(1);
  });

  it('rejects final receipts for another hash or chain without accepting them', async () => {
    const reader = new FakeReader((ref) => {
      if (ref.hash === '0xaa') {
        return { kind: 'final', receipt: successReceipt('1', '0xdead') };
      }
      if (ref.hash === '0xbb') {
        return { kind: 'final', receipt: successReceipt('137', '0xbb') };
      }
      return { kind: 'final', receipt: successReceipt('1', ref.hash) };
    });
    const { reconciler } = setup(reader);
    const wrongHash = { chainId: '1', hash: '0xaa' };
    const wrongChain = { chainId: '1', hash: '0xbb' };
    await reconciler.track(wrongHash);
    await reconciler.track(wrongChain);

    const summary = await reconciler.runDue();
    expect(summary.pending).toBe(2);
    expect(summary.confirmed).toBe(0);

    const first = await reconciler.get(wrongHash);
    expect(first?.status).toBe('pending');
    expect(first?.receipt).toBeUndefined();
    expect(first?.reason).toContain('ref mismatch');
    expect(first?.reason).toContain('0xdead');

    const second = await reconciler.get(wrongChain);
    expect(second?.status).toBe('pending');
    expect(second?.receipt).toBeUndefined();
    expect(second?.reason).toContain('chainId=137');
  });

  it('handles at most batchSize observations per call', async () => {
    const reader = new FakeReader(() => ({ kind: 'notFound' }));
    const { reconciler } = setup(reader, { batchSize: 2 });
    for (const hash of ['0x01', '0x02', '0x03']) {
      await reconciler.track({ chainId: '1', hash });
    }

    const first = await reconciler.runDue();
    expect(first.scanned).toBe(2);

    const second = await reconciler.runDue();
    expect(second.scanned).toBe(1);
  });
});

describe('Reconciler settings', () => {
  it('rejects invalid settings before any work starts', () => {
    const store = new SqliteStore(':memory:');
    const reader = new FakeReader(() => ({ kind: 'notFound' }));
    stores.push(store);

    expect(
      () => new Reconciler({ store, reader, batchSize: 0 }),
    ).toThrow(RangeError);
    expect(
      () => new Reconciler({ store, reader, maxRetryMs: 10, initialRetryMs: 20 }),
    ).toThrow(RangeError);
    expect(
      () => new Reconciler({ store, reader, retryFactor: 0.5 }),
    ).toThrow(RangeError);
  });
});
