import type { ChainReader, LookupResult, TxRef } from './types.js';

/**
 * Runs one lookup under the configured RPC deadline.
 *
 * The adapter receives an AbortSignal so a real client can cancel the request.
 * The race guarantees `runDue` is not held hostage by a reader that ignores it.
 */
export async function lookupWithTimeout(
  reader: ChainReader,
  ref: TxRef,
  timeoutMs: number,
): Promise<LookupResult> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;

  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      const error = new Error(`RPC lookup timed out after ${timeoutMs} ms`);
      controller.abort(error);
      reject(error);
    }, timeoutMs);
  });

  try {
    return await Promise.race([
      reader.lookup(ref, { signal: controller.signal, timeoutMs }),
      deadline,
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}
