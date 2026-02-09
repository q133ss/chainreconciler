import Database from 'better-sqlite3';

import { normalizeRef } from '../ref.js';
import type {
  NewObservation,
  Observation,
  ObservationStatus,
  ObservationUpdate,
  Receipt,
  Store,
  TxRef,
} from '../types.js';

export interface SqliteStoreOptions {
  /** How long SQLite waits for a competing writer. Default: 5000 ms. */
  readonly busyTimeoutMs?: number;
}

interface ObservationRow {
  readonly chain_id: string;
  readonly hash: string;
  readonly status: string;
  readonly deadline: string;
  readonly attempts: number;
  readonly next_attempt_at: string;
  readonly reason: string | null;
  readonly receipt: string | null;
  readonly version: number;
  readonly created_at: string;
  readonly updated_at: string;
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS observations (
    chain_id        TEXT    NOT NULL,
    hash            TEXT    NOT NULL,
    status          TEXT    NOT NULL
                    CHECK (status IN ('pending', 'confirmed', 'failed', 'unknown')),
    deadline        TEXT    NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT    NOT NULL,
    reason          TEXT,
    receipt         TEXT,
    version         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    PRIMARY KEY (chain_id, hash)
  );

  CREATE INDEX IF NOT EXISTS observations_due
    ON observations (status, next_attempt_at);
`;

/**
 * SQLite-backed observation store. One file can be shared by several worker
 * processes; SQLite serializes writers and `compareAndSet` resolves races.
 *
 * Pass `:memory:` for tests that do not need to survive a restart.
 */
export class SqliteStore implements Store {
  private readonly db: Database.Database;
  private readonly selectOne;
  private readonly selectDue;
  private readonly insertOne;
  private readonly updateOne;

  constructor(filename: string, options: SqliteStoreOptions = {}) {
    if (typeof filename !== 'string' || filename.trim().length === 0) {
      throw new TypeError('sqlite filename must be a non-empty string');
    }

    const busyTimeoutMs = options.busyTimeoutMs ?? 5000;
    if (!Number.isInteger(busyTimeoutMs) || busyTimeoutMs < 0) {
      throw new RangeError('busyTimeoutMs must be a non-negative integer');
    }

    this.db = new Database(filename);
    this.db.pragma(`busy_timeout = ${busyTimeoutMs}`);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(SCHEMA);

    this.selectOne = this.db.prepare(
      `SELECT chain_id, hash, status, deadline, attempts, next_attempt_at,
              reason, receipt, version, created_at, updated_at
         FROM observations
        WHERE chain_id = ? AND hash = ?`,
    );
    this.selectDue = this.db.prepare(
      `SELECT chain_id, hash, status, deadline, attempts, next_attempt_at,
              reason, receipt, version, created_at, updated_at
         FROM observations
        WHERE status = 'pending' AND next_attempt_at <= ?
        ORDER BY next_attempt_at ASC, created_at ASC
        LIMIT ?`,
    );
    this.insertOne = this.db.prepare(
      `INSERT OR IGNORE INTO observations
         (chain_id, hash, status, deadline, attempts, next_attempt_at,
          reason, receipt, version, created_at, updated_at)
       VALUES (?, ?, 'pending', ?, 0, ?, NULL, NULL, 0, ?, ?)`,
    );
    this.updateOne = this.db.prepare(
      `UPDATE observations
          SET status = ?,
              attempts = ?,
              next_attempt_at = ?,
              reason = ?,
              receipt = ?,
              version = version + 1,
              updated_at = ?
        WHERE chain_id = ? AND hash = ? AND status = 'pending' AND version = ?`,
    );
  }

  createIfAbsent(input: NewObservation): Promise<Observation> {
    const ref = normalizeRef(input.ref);
    this.insertOne.run(
      ref.chainId,
      ref.hash,
      input.deadline,
      input.nextAttemptAt,
      input.createdAt,
      input.createdAt,
    );

    const existing = this.select(ref);
    if (existing === null) {
      throw new Error(`failed to create observation for ${ref.chainId}/${ref.hash}`);
    }
    return Promise.resolve(existing);
  }

  get(ref: TxRef): Promise<Observation | null> {
    return Promise.resolve(this.select(normalizeRef(ref)));
  }

  due(now: string, limit: number): Promise<Observation[]> {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError('due limit must be a positive integer');
    }
    const rows = this.selectDue.all(now, limit) as ObservationRow[];
    return Promise.resolve(rows.map(toObservation));
  }

  compareAndSet(
    ref: TxRef,
    expectedVersion: number,
    update: ObservationUpdate,
  ): Promise<boolean> {
    const normalized = normalizeRef(ref);
    const receipt = update.receipt === undefined ? null : JSON.stringify(update.receipt);

    const result = this.updateOne.run(
      update.status,
      update.attempts,
      update.nextAttemptAt,
      update.reason ?? null,
      receipt,
      update.updatedAt,
      normalized.chainId,
      normalized.hash,
      expectedVersion,
    );

    return Promise.resolve(result.changes === 1);
  }

  close(): void {
    this.db.close();
  }

  private select(ref: TxRef): Observation | null {
    const row = this.selectOne.get(ref.chainId, ref.hash) as
      | ObservationRow
      | undefined;
    return row === undefined ? null : toObservation(row);
  }
}

function toObservation(row: ObservationRow): Observation {
  const status = row.status as ObservationStatus;
  return {
    ref: { chainId: row.chain_id, hash: row.hash },
    status,
    deadline: row.deadline,
    attempts: row.attempts,
    nextAttemptAt: row.next_attempt_at,
    version: row.version,
    ...(row.reason === null ? {} : { reason: row.reason }),
    ...(row.receipt === null
      ? {}
      : { receipt: JSON.parse(row.receipt) as Receipt }),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
