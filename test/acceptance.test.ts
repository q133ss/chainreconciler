import { afterEach, describe, expect, it } from 'vitest';

import {
  Reconciler,
  SqliteStore,
  type ChainReader,
  type LookupResult,
  type Receipt,
} from '../src/index.js';
import {
  FakeReader,
  TestClock,
  createTempDb,
  deferred,
  successReceipt,
} from './helpers.js';

const T0 = '2026-02-03T09:00:00.000Z';

const cleanupTasks: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const cleanup of cleanupTasks.splice(0).reverse()) {
    await cleanup();
  }
});

describe('acceptance: recovering an already-sent transaction', () => {
  it('recovers a send timeout without ever sending the transaction again', async () => {
    const clock = new TestClock(T0);
    const reader = new FakeReader((_ref, _options, call) => {
      if (call === 1) {
        return { kind: 'notFound' };
      }
      return {
        kind: 'final',
        receipt: successReceipt('1', '0xtimeout', 88, { status: 'ok' }),
      };
    });
    const store = new SqliteStore(':memory:');
    const reconciler = new Reconciler({
      store,
      reader,
      clock,
      watchForMs: 600_000,
      initialRetryMs: 1_000,
      maxRetryMs: 30_000,
    });
    cleanupTasks.push(() => reconciler.close());

    const ref = { chainId: '1', hash: '0xtimeout' };
    await reconciler.track(ref);

    let sends = 0;
    const send = async () => {
      sends += 1;
      throw new Error('rpc client timed out');
    };
    await expect(send()).rejects.toThrow('rpc client timed out');

    await reconciler.runDue();
    expect((await reconciler.get(ref))?.status).toBe('pending');

    clock.advance(1_000);
    await reconciler.runDue();

    const recovered = await reconciler.get(ref);
    expect(recovered?.status).toBe('confirmed');
    expect(recovered?.receipt?.blockNumber).toBe('88');
    expect(recovered?.receipt?.result).toEqual({ status: 'ok' });
    expect(sends).toBe(1);
    expect(reader.calls).toHaveLength(2);
  });

  it('restores the result after a crash using only the hash and the same SQLite file', async () => {
    const temp = await createTempDb();
    cleanupTasks.push(temp.cleanup);

    const clock = new TestClock(T0);
    const reader = new FakeReader(() => ({
      kind: 'final',
      receipt: successReceipt('1', '0xrestart', 9001),
    }));
    const ref = { chainId: '1', hash: '0xrestart' };

    let sends = 0;
    const firstStore = new SqliteStore(temp.path);
    try {
      const first = new Reconciler({
        store: firstStore,
        reader,
        clock,
        watchForMs: 600_000,
      });
      const tracked = await first.track(ref);
      expect(tracked.status).toBe('pending');

      // The transaction reaches the network, then the process dies before it
      // can persist anything else.
      sends += 1;
    } finally {
      firstStore.close();
    }

    clock.advance(5_000);

    const secondStore = new SqliteStore(temp.path);
    try {
      const second = new Reconciler({
        store: secondStore,
        reader,
        clock,
        watchForMs: 600_000,
      });

      // No second `track` call: the observation already exists on disk.
      const restored = await second.get(ref);
      expect(restored?.status).toBe('pending');
      expect(restored?.deadline).toBe('2026-02-03T09:10:00.000Z');

      await second.runDue();

      const recovered = await second.get(ref);
      expect(recovered?.status).toBe('confirmed');
      expect(recovered?.receipt?.blockNumber).toBe('9001');
      expect(sends).toBe(1);
      expect(reader.calls).toHaveLength(1);
    } finally {
      secondStore.close();
    }
  });

  it('waits for adapter finality before moving a mined transaction out of pending', async () => {
    const clock = new TestClock(T0);
    const reader = new FakeReader((_ref, _options, call) => {
      if (call < 3) {
        return { kind: 'pending', reason: '1/12 confirmations' };
      }
      return {
        kind: 'final',
        receipt: successReceipt('1', '0xfinality', 700),
      };
    });
    const store = new SqliteStore(':memory:');
    const reconciler = new Reconciler({
      store,
      reader,
      clock,
      initialRetryMs: 1_000,
      maxRetryMs: 10_000,
    });
    cleanupTasks.push(() => reconciler.close());

    const ref = { chainId: '1', hash: '0xfinality' };
    await reconciler.track(ref);

    await reconciler.runDue();
    clock.advance(1_000);
    await reconciler.runDue();

    const notFinal = await reconciler.get(ref);
    expect(notFinal?.status).toBe('pending');
    expect(notFinal?.reason).toBe('1/12 confirmations');
    expect(notFinal?.receipt).toBeUndefined();

    clock.advance(2_000);
    await reconciler.runDue();

    const final = await reconciler.get(ref);
    expect(final?.status).toBe('confirmed');
    expect(final?.receipt?.blockNumber).toBe('700');
  });

  it('records unknown when the watch deadline passes without a final answer', async () => {
    const clock = new TestClock(T0);
    const reader = new FakeReader(() => ({ kind: 'notFound' }));
    const store = new SqliteStore(':memory:');
    const reconciler = new Reconciler({
      store,
      reader,
      clock,
      watchForMs: 3_000,
      initialRetryMs: 1_000,
      maxRetryMs: 2_000,
    });
    cleanupTasks.push(() => reconciler.close());

    const ref = { chainId: '1', hash: '0xmissing' };
    await reconciler.track(ref);

    await reconciler.runDue();
    clock.advance(1_000);
    await reconciler.runDue();

    let observation = await reconciler.get(ref);
    expect(observation?.attempts).toBe(2);
    expect(observation?.nextAttemptAt).toBe('2026-02-03T09:00:03.000Z');

    clock.advance(2_000);
    await reconciler.runDue();

    observation = await reconciler.get(ref);
    expect(observation?.status).toBe('unknown');
    expect(observation?.reason).toContain('watch deadline expired');
    expect(observation?.reason).toContain('transaction not found');
    expect(observation?.receipt).toBeUndefined();
  });

  it('keeps the first final result when two workers resolve the same observation', async () => {
    const temp = await createTempDb();
    cleanupTasks.push(temp.cleanup);
    const clock = new TestClock(T0);
    const ref = { chainId: '1', hash: '0xrace' };

    const entered = deferred<void>();
    const gate = deferred<LookupResult>();
    const slowReader = new FakeReader((_ref, _options, call) => {
      if (call === 1) {
        entered.resolve();
        return gate.promise;
      }
      return { kind: 'notFound' };
    });
    const finalReader = new FakeReader(() => ({
      kind: 'final',
      receipt: successReceipt('1', '0xrace', 501, { winner: 'worker-b' }),
    }));

    const storeA = new SqliteStore(temp.path);
    const storeB = new SqliteStore(temp.path);
    cleanupTasks.push(() => {
      storeA.close();
      storeB.close();
    });

    const workerA = new Reconciler({
      store: storeA,
      reader: slowReader,
      clock,
      initialRetryMs: 1_000,
    });
    const workerB = new Reconciler({
      store: storeB,
      reader: finalReader,
      clock,
      initialRetryMs: 1_000,
    });

    await workerA.track(ref);

    const runA = workerA.runDue();
    await entered.promise;

    const summaryB = await workerB.runDue();
    expect(summaryB.confirmed).toBe(1);

    gate.resolve({ kind: 'notFound' });
    const summaryA = await runA;
    expect(summaryA.skipped).toBe(1);

    const settled = await workerB.get(ref);
    expect(settled?.status).toBe('confirmed');
    expect(settled?.version).toBe(1);
    expect(settled?.receipt?.result).toEqual({ winner: 'worker-b' });

    const repeated = await workerA.track(ref);
    expect(repeated).toEqual(settled);
    expect(repeated.receipt?.blockNumber).toBe('501');
  });

  it('observes the same hash independently on different chains', async () => {
    const clock = new TestClock(T0);
    const reader = new FakeReader((ref) => {
      if (ref.chainId === '1') {
        return { kind: 'final', receipt: successReceipt('1', ref.hash, 10) };
      }
      return { kind: 'notFound' };
    });
    const store = new SqliteStore(':memory:');
    const reconciler = new Reconciler({
      store,
      reader,
      clock,
      initialRetryMs: 1_000,
    });
    cleanupTasks.push(() => reconciler.close());

    const onMainnet = { chainId: '1', hash: '0xsame' };
    const onPolygon = { chainId: '137', hash: '0xsame' };
    await reconciler.track(onMainnet);
    await reconciler.track(onPolygon);

    const summary = await reconciler.runDue();
    expect(summary.scanned).toBe(2);
    expect(summary.confirmed).toBe(1);
    expect(summary.pending).toBe(1);

    const mainnet = await reconciler.get(onMainnet);
    const polygon = await reconciler.get(onPolygon);
    expect(mainnet?.status).toBe('confirmed');
    expect(polygon?.status).toBe('pending');

    clock.advance(1_000);
    await reconciler.runDue();

    const stillPending = await reconciler.get(onPolygon);
    expect(stillPending?.status).toBe('pending');
    expect(stillPending?.receipt).toBeUndefined();
  });
});

/**
 * Compile-time check that the receipt shape exported by the library is usable
 * by adapter implementations.
 */
const _receiptTypeCheck: Receipt = {
  chainId: '1',
  hash: '0xaa',
  blockNumber: '1',
  success: true,
};
void _receiptTypeCheck;

const _readerTypeCheck: ChainReader = {
  async lookup() {
    return { kind: 'notFound' };
  },
};
void _readerTypeCheck;
