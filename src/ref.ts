import type { TxRef } from './types.js';

/**
 * Canonical form used for storage keys and reader calls.
 *
 * Hashes are trimmed and lowercased because hex hashes are case-insensitive;
 * keeping both spellings would create two observations for one transaction.
 * Chain ids are trimmed but otherwise preserved.
 */
export function normalizeRef(ref: TxRef): TxRef {
  if (ref === null || typeof ref !== 'object') {
    throw new TypeError('tx ref must be an object with chainId and hash');
  }
  if (typeof ref.chainId !== 'string' || typeof ref.hash !== 'string') {
    throw new TypeError('tx ref chainId and hash must be strings');
  }

  const chainId = ref.chainId.trim();
  const hash = ref.hash.trim().toLowerCase();

  if (chainId.length === 0) {
    throw new TypeError('tx ref chainId must not be empty');
  }
  if (hash.length === 0) {
    throw new TypeError('tx ref hash must not be empty');
  }

  return { chainId, hash };
}

export function sameRef(left: TxRef, right: TxRef): boolean {
  return left.chainId === right.chainId && left.hash === right.hash;
}

export function describeRef(ref: TxRef): string {
  return `chainId=${ref.chainId} hash=${ref.hash}`;
}
