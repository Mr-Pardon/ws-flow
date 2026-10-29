import type { PeerLike } from '../types/peer.js'
import type { RuntimeMessage } from '../protocol.js'

export interface ReceivedMessage<Peer extends PeerLike = PeerLike> {
  /** Live peer identity; payload snapshots do not clone the connection. */
  readonly peer: Peer
  readonly raw: unknown
  readonly data: unknown
  readonly event?: string
}

export type SubscriptionFilter<Peer extends PeerLike = PeerLike> = (
  raw: unknown,
  message: ReceivedMessage<Peer>
) => boolean

export interface SubscriptionOptions<Peer extends PeerLike = PeerLike> {
  /** Positive integer; overflowing fails this subscription instead of dropping data. */
  capacity?: number
  signal?: AbortSignal
  /** Scope to this peer object, not merely a reusable connection ID. */
  peer?: Peer
}

export class SubscriptionOverflowError extends Error {
  constructor(capacity: number) {
    super(`Subscription buffer exceeded capacity ${capacity}`)
    this.name = 'SubscriptionOverflowError'
  }
}

/** One consumer queue. Fan-out requires independent subscriptions. */
export class MessageSubscription<Peer extends PeerLike = PeerLike>
  implements AsyncIterableIterator<ReceivedMessage<Peer>>
{
  private _queue: ReceivedMessage<Peer>[] = []
  private _waiters: {
    resolve: (result: IteratorResult<ReceivedMessage<Peer>>) => void
    reject: (error: unknown) => void
  }[] = []
  private _state: 'active' | 'closed' | 'failed' = 'active'
  private _error: unknown
  private _onClose: () => void

  constructor(
    private _capacity: number,
    onClose: () => void
  ) {
    if (!Number.isInteger(_capacity) || _capacity < 1) {
      throw new RangeError('Subscription capacity must be a positive integer')
    }
    this._onClose = onClose
  }

  get closed() {
    return this._state !== 'active'
  }

  /** @internal */
  push(message: ReceivedMessage<Peer>) {
    if (this.closed) return
    const waiter = this._waiters.shift()
    if (waiter) waiter.resolve({ value: message, done: false })
    else if (this._queue.length >= this._capacity)
      this.fail(new SubscriptionOverflowError(this._capacity))
    else this._queue.push(message)
  }

  private _close(state: 'closed' | 'failed', error?: unknown) {
    if (this.closed) return
    this._state = state
    this._error = error
    this._queue = []
    const cleanup = this._onClose
    this._onClose = () => {}
    cleanup()
    for (const waiter of this._waiters.splice(0)) {
      if (state === 'failed') waiter.reject(error)
      else waiter.resolve({ value: undefined, done: true })
    }
  }

  /** @internal */
  fail(error: unknown) {
    this._close('failed', error)
  }

  /** Ends local observation only; does not send a remote task cancellation. */
  cancel() {
    this._close('closed')
  }

  next(): Promise<IteratorResult<ReceivedMessage<Peer>>> {
    if (this._state === 'failed') return Promise.reject(this._error)
    if (this._state === 'closed') return Promise.resolve({ value: undefined, done: true })
    if (this._queue.length)
      return Promise.resolve({ value: this._queue.shift()!, done: false })
    return new Promise((resolve, reject) => this._waiters.push({ resolve, reject }))
  }

  return(): Promise<IteratorResult<ReceivedMessage<Peer>>> {
    this.cancel()
    return Promise.resolve({ value: undefined, done: true })
  }

  [Symbol.asyncIterator]() {
    return this
  }
}

type RecordEntry<Peer extends PeerLike> = {
  filter: SubscriptionFilter<Peer>
  peer?: Peer
  subscription: MessageSubscription<Peer>
}

export class SubscriptionRegistry<Peer extends PeerLike = PeerLike> {
  private _records = new Set<RecordEntry<Peer>>()

  subscribe(filter: SubscriptionFilter<Peer>, options: SubscriptionOptions<Peer> = {}) {
    if (typeof filter !== 'function')
      throw new TypeError('Subscription requires a predicate')
    const { signal } = options
    const record = { filter, peer: options.peer } as RecordEntry<Peer>
    const onAbort = () => subscription.cancel()
    const subscription = new MessageSubscription<Peer>(options.capacity ?? 128, () => {
      this._records.delete(record)
      signal?.removeEventListener('abort', onAbort)
    })
    record.subscription = subscription
    if (signal?.aborted) subscription.cancel()
    else {
      this._records.add(record)
      signal?.addEventListener('abort', onAbort, { once: true })
    }
    return subscription
  }

  publish(raw: unknown, message: RuntimeMessage, peer: Peer) {
    // Snapshot membership and isolate payloads for predicates and consumers.
    for (const record of [...this._records]) {
      if (record.subscription.closed || (record.peer && record.peer !== peer)) continue
      try {
        const snapshot: ReceivedMessage<Peer> = {
          ...structuredClone({ raw, data: message.data, event: message.event }),
          peer
        }
        const matched: unknown = record.filter(snapshot.raw, snapshot)
        if (typeof matched !== 'boolean') {
          // JS callers can accidentally pass async predicates. Consume any
          // rejected promise before reporting the unsupported contract.
          void Promise.resolve(matched).catch(() => {})
          throw new TypeError(
            'Subscription predicate must return a boolean synchronously'
          )
        }
        if (matched) record.subscription.push(snapshot)
      } catch (error) {
        record.subscription.fail(error)
      }
    }
  }

  clear(peer?: Peer) {
    for (const record of [...this._records]) {
      if (peer === undefined || record.peer === peer) record.subscription.cancel()
    }
  }
}
