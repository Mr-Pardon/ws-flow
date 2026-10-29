/** Stop waiting without leaving an abort listener on each completed operation. */
export function abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason)
    if (signal.aborted) abort()
    else signal.addEventListener('abort', abort, { once: true })
    operation
      .then(resolve, reject)
      .finally(() => signal.removeEventListener('abort', abort))
  })
}
