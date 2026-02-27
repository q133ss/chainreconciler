import { rm } from 'node:fs/promises';

import {
  Reconciler,
  SqliteStore,
  type ChainReader,
  type LookupResult,
} from '../src/index.js';

const dbPath = process.argv[2] ?? 'recovery-demo.db';
const ref = {
  chainId: '8453',
  hash: `0x${'ab'.repeat(32)}`,
};

let sends = 0;

async function sendTransaction(): Promise<void> {
  sends += 1;
  // The application's send path: the node accepted the bytes, but the client
  // gave up waiting for the response.
  throw new Error('send timed out after 30s');
}

const firstProcessReader: ChainReader = {
  async lookup(): Promise<LookupResult> {
    return { kind: 'pending', reason: 'transaction is in the mempool' };
  },
};

const restartedProcessReader: ChainReader = {
  async lookup(lookedUp): Promise<LookupResult> {
    return {
      kind: 'final',
      receipt: {
        chainId: lookedUp.chainId,
        hash: lookedUp.hash,
        blockNumber: 12_345_678,
        success: true,
        result: { status: 'success' },
      },
    };
  },
};

async function main(): Promise<void> {
  await Promise.all([
    rm(dbPath, { force: true }),
    rm(`${dbPath}-wal`, { force: true }),
    rm(`${dbPath}-shm`, { force: true }),
  ]);

  // Process #1: record the observation, send, then "crash" (close) before the
  // result is known.
  const firstStore = new SqliteStore(dbPath);
  const first = new Reconciler({
    store: firstStore,
    reader: firstProcessReader,
    watchForMs: 10 * 60_000,
    initialRetryMs: 250,
    maxRetryMs: 5_000,
  });

  const tracked = await first.track(ref);
  console.log('tracked before send:', {
    status: tracked.status,
    deadline: tracked.deadline,
  });

  try {
    await sendTransaction();
  } catch (error) {
    console.log('send failed:', (error as Error).message);
  }

  console.log('process 1 check:', await first.runDue());
  await first.close();
  console.log('process 1 exited');

  await new Promise((resolve) => setTimeout(resolve, 300));

  // Process #2: same SQLite file, no second `track` call, different RPC client.
  const restartedStore = new SqliteStore(dbPath);
  const restarted = new Reconciler({
    store: restartedStore,
    reader: restartedProcessReader,
    watchForMs: 10 * 60_000,
  });

  const beforeRestart = await restarted.get(ref);
  console.log('reopened state:', beforeRestart?.status);

  console.log('process 2 check:', await restarted.runDue());

  const recovered = await restarted.get(ref);
  console.log('recovered:', {
    status: recovered?.status,
    blockNumber: recovered?.receipt?.blockNumber,
    result: recovered?.receipt?.result,
  });
  console.log('transaction sends:', sends);

  await restarted.close();
}

await main();
