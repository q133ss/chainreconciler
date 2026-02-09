import { afterEach, describe, expect, it } from 'vitest';

import { SqliteStore } from '../src/index.js';
import type { NewObservation, ObservationUpdate } from '../src/index.js';

const T0 = '2026-02-03T09:00:00.000Z';

const stores: SqliteStore[] = [];

function memoryStore(): SqliteStore {
  const store = new SqliteStore(':memory:');
  stores.push(store);
  return store;
}

function newObservation(
  chainId: string,
  hash: string,
  nextAttemptAt = T0,
  deadline = '2026-02-04T09:00:00.000Z',
): NewObservation {
  return {
    ref: { chainId, hash },
    deadline,
    nextAttemptAt,
    createdAt: T0,
  };
}

function pendingUpdate(overrides: Partial<ObservationUpdate> = {}): ObservationUpdate {
  return {
    status: 'pending',
    attempts: 1,
    nextAttemptAt: '2026-02-03T09:00:30.000Z',
    reason: 'not found',
    updatedAt: '2026-02-03T09:00:00.000Z',
    ...overrides,
  };
}

afterEach(() => {
  for (const store of stores.splice(0)) {
    store.close();
  }
});

describe('SqliteStore', () => {
  it('creates one observation per chain and hash and preserves its first deadline', async () => {
    const store = memoryStore();

    const first = await store.createIfAbsent(
      newObservation('1', '0xAABB', T0, '2026-02-04T09:00:00.000Z'),
    );
    const repeated = await store.createIfAbsent(
      newObservation('1', '0xaabb', T0, '2026-03-01T00:00:00.000Z'),
    );

    expect(first.version).toBe(0);
    expect(first.status).toBe('pending');
    expect(repeated).toEqual(first);

    const byNormalizedRef = await store.get({ chainId: '1', hash: '0xAABB' });
    expect(byNormalizedRef).toEqual(first);

    const otherChain = await store.createIfAbsent(
      newObservation('137', '0xaabb'),
    );
    expect(otherChain.ref).toEqual({ chainId: '137', hash: '0xaabb' });
    expect(otherChain.version).toBe(0);
  });

  it('returns due pending observations in retry order and respects the limit', async () => {
    const store = memoryStore();

    await store.createIfAbsent(newObservation('1', '0x01', T0));
    await store.createIfAbsent(
      newObservation('1', '0x02', '2026-02-03T09:01:00.000Z'),
    );
    await store.createIfAbsent(newObservation('1', '0x03', T0));

    const due = await store.due(T0, 10);
    expect(due.map((item) => item.ref.hash)).toEqual(['0x01', '0x03']);

    const limited = await store.due(T0, 1);
    expect(limited).toHaveLength(1);
    expect(limited[0]?.ref.hash).toBe('0x01');

    for (const hash of ['0x01', '0x03']) {
      const observation = await store.get({ chainId: '1', hash });
      await store.compareAndSet(
        { chainId: '1', hash },
        observation!.version,
        pendingUpdate({ nextAttemptAt: '2026-02-03T09:02:00.000Z' }),
      );
    }

    const later = await store.due('2026-02-03T09:01:00.000Z', 10);
    expect(later.map((item) => item.ref.hash)).toEqual(['0x02']);
  });

  it('applies a transition only once for the version that was read', async () => {
    const store = memoryStore();
    const ref = { chainId: '1', hash: '0xaa' };
    await store.createIfAbsent(newObservation(ref.chainId, ref.hash));

    const read = await store.get(ref);
    expect(read?.version).toBe(0);

    const won = await store.compareAndSet(ref, 0, pendingUpdate({ attempts: 1 }));
    expect(won).toBe(true);

    const lost = await store.compareAndSet(
      ref,
      0,
      pendingUpdate({ status: 'confirmed', attempts: 2 }),
    );
    expect(lost).toBe(false);

    const after = await store.get(ref);
    expect(after?.version).toBe(1);
    expect(after?.status).toBe('pending');
    expect(after?.attempts).toBe(1);
  });

  it('does not move a terminal observation back to pending', async () => {
    const store = memoryStore();
    const ref = { chainId: '1', hash: '0xbb' };
    await store.createIfAbsent(newObservation(ref.chainId, ref.hash));
    const read = await store.get(ref);
    expect(read).not.toBeNull();

    const receipt = {
      chainId: '1',
      hash: '0xbb',
      blockNumber: '42',
      success: true,
    };
    const confirmed = await store.compareAndSet(
      ref,
      read!.version,
      pendingUpdate({
        status: 'confirmed',
        reason: undefined,
        receipt,
        attempts: 1,
      }),
    );
    expect(confirmed).toBe(true);

    const stale = await store.compareAndSet(
      ref,
      read!.version,
      pendingUpdate({ attempts: 9 }),
    );
    expect(stale).toBe(false);

    const after = await store.get(ref);
    expect(after?.status).toBe('confirmed');
    expect(after?.receipt).toEqual(receipt);
    expect(after?.attempts).toBe(1);
    expect((await store.due('2030-01-01T00:00:00.000Z', 10))).toEqual([]);
  });
});
