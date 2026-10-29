import type { 
  AnyRecord, 
  QueueStrategyFn, 
  WSHandler
} from '../types/ws.js'

interface WSStreamTaskOptions<Receive> {
  maxQueueSize?: number
  strategy?: QueueStrategyFn<Receive> | keyof typeof builtInStrategies
}

const builtInStrategies = {
  'unbounded': <Receive>({ queue, incoming }: any) => {
    return [...queue, incoming]
  },

  'drop-head': <Receive>({ queue, incoming, maxSize = 10 }: any) => {
    const next = [...queue, incoming]
    return next.slice(-maxSize)
  },

  'drop-tail': <Receive>({ queue, incoming, maxSize = 10 }: any) => {
    if (queue.length >= maxSize) return queue
    return [...queue, incoming]
  }
}

export class StreamTask<Receive = any, Events extends AnyRecord = AnyRecord> {
  private _subscribe: (handler: WSHandler<Receive, Events>) => () => void
  private _unsubscribe?: () => void
  private _handlers: WSHandler<Receive, Events>[] = []
  private _stopHandlers: (() => void)[] = []

  private _queue: Receive[] = []
  private _resolvers: ((value: IteratorResult<Receive>) => void)[] = []
  private _maxQueueSize?: number
  private _strategy: QueueStrategyFn<Receive>
  private _ended = false

  /**
   * Create a StreamTask for consuming a stream of messages.
   *
   * @param subscribe - Subscription function that registers a handler and
   * returns an unsubscribe function.
   * @param options - Queue and strategy configuration for buffering.
   *
   * @example
   * ```ts
   * const task = new StreamTask<MyMessage>((handler) => {
   *   const off = client.on('my:event', handler)
   *   return () => off()
   * })
   * task.start()
   * ```
   */
  constructor(
    subscribe: (handler: WSHandler<Receive, Events>) => () => void,
    options?: WSStreamTaskOptions<Receive>
  ) {
    this._subscribe = subscribe
    this._maxQueueSize = options?.maxQueueSize
    const strategy = options?.strategy || 'unbounded'

    this._strategy = strategy instanceof Function
      ? strategy
      : builtInStrategies[strategy]
  }

  private _push(data: Receive) {
    if (this._resolvers.length) {
      const resolve = this._resolvers.shift()!
      resolve({ value: data, done: false })
      return
    }

    this._queue = this._strategy({
      queue: this._queue,
      incoming: data,
      maxSize: this._maxQueueSize
    })
  }

  push(data: Receive) {
    this._push(data)
  }

  /**
   * Create an async iterator over the streamed messages.
   *
   * Each `next()` resolves when a message is available or when the task ends.
   *
   * @example
   * ```ts
   * for await (const item of task) {
   *   console.log(item)
   * }
   * ```
   */
  [Symbol.asyncIterator]() {
    return {
      next: () => {
        if (this._queue.length) {
          const value = this._queue.shift()!
          return Promise.resolve({ value, done: false })
        }

        if (this._ended) {
          return Promise.resolve({ done: true })
        }

        return new Promise(resolve => {
          this._resolvers.push(resolve)
        })
      }
    }
  }

  /**
   * Start the stream subscription.
   *
   * No-op if already started.
   *
   * @example
   * ```ts
   * task.start()
   * ```
   */
  start() {
    if (this._unsubscribe) return

    this._unsubscribe = this._subscribe((data, ctx) => {
      this._handlers.forEach(h => h(data, ctx))
      this._push(data)
    })
  }

  /**
   * Stop the stream subscription and end the iterator.
   *
   * Pending `next()` promises are resolved as done, the queue is cleared,
   * and stop handlers are invoked.
   *
   * @example
   * ```ts
   * task.stop()
   * ```
   */
  stop() {
    if (this._unsubscribe) {
      this._unsubscribe?.()
      this._unsubscribe = undefined
    }

    if (this._ended) return
    this._ended = true

    this._resolvers.forEach(r => r({ done: true, value: undefined }))
    this._resolvers = []
    this._queue = []

    this._stopHandlers.forEach(h => h())
  }

  /**
   * Restart the stream subscription.
   *
   * Clears buffered items and resets the ended state.
   *
   * @example
   * ```ts
   * task.restart()
   * ```
   */
  restart() {
    if (this._ended) {
      this._ended = false
    }
    this._queue = []
    this._resolvers = []

    this._unsubscribe?.()
    this._unsubscribe = undefined
    this.start()
  }

  /**
   * Register a handler invoked for each received message.
   *
   * @param handler - Callback invoked with data and context.
   * @returns An unsubscribe function for this handler.
   *
   * @example
   * ```ts
   * const off = task.onData((data) => console.log(data))
   * // later...
   * off()
   * ```
   */
  onData(handler: WSHandler<Receive, Events>) {
    this._handlers.push(handler)
    return () => {
      this._handlers = this._handlers.filter(h => h !== handler)
    }
  }

  /**
   * Register a handler invoked when the task stops.
   *
   * @param handler - Callback invoked on stop.
   * @returns An unsubscribe function for this handler.
   *
   * @example
   * ```ts
   * const off = task.onStop(() => console.log('stopped'))
   * ```
   */
  onStop(handler: () => void) {
    this._stopHandlers.push(handler)
    return () => {
      this._stopHandlers = this._stopHandlers.filter(h => h !== handler)
    }
  }

  /**
   * Await the next item from the stream.
   *
   * @returns A promise that resolves with the next item or rejects if the task ended.
   *
   * @example
   * ```ts
   * const item = await task.next()
   * ```
   */
  next(): Promise<Receive> {
    if (this._queue.length) {
      return Promise.resolve(this._queue.shift()!)
    }

    if (this._ended) {
      return Promise.reject(new Error('StreamTask ended'))
    }

    return new Promise<Receive>((resolve, reject) => {
      this._resolvers.push((result) => {
        if (result.done) {
          reject(new Error('StreamTask ended'))
          return
        }
        resolve(result.value as Receive)
      })
    })
  }
}
