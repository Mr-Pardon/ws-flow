export type StreamState = 'start' | 'chunk' | 'end' | 'error'

export type StreamEvent<T = any> = { state: StreamState; data?: T; error?: unknown }

export class StreamConsumer<T = any> implements AsyncIterableIterator<T> {
  private _queue: T[] = []
  private _waiters: {
    resolve: (value: IteratorResult<T>) => void
    reject: (reason: unknown) => void
  }[] = []
  private _stateHandlers: ((event: StreamEvent<T>) => void)[] = []
  private _dataHandlers: ((data: T) => void)[] = []
  private _mode: 'unset' | 'iterator' | 'callback' = 'unset'
  private _ended = false
  private _failed = false
  private _error: unknown
  private _capacity: number

  constructor(private _options: { capacity?: number; onClose?: () => void } = {}) {
    this._capacity = _options.capacity ?? 128
    if (!Number.isInteger(this._capacity) || this._capacity < 1) {
      throw new RangeError('Stream capacity must be a positive integer')
    }
  }

  private _notify<Value>(handlers: ((value: Value) => void)[], value: Value) {
    for (const handler of [...handlers]) {
      try {
        const result = handler(value)
        void Promise.resolve(result).catch(error => {
          if (!this._ended) {
            try {
              this.stop('error', error)
            } catch {
              /* terminal state is already settled */
            }
          }
        })
      } catch (error) {
        if (!this._ended) {
          try {
            this.stop('error', error)
          } catch {
            /* preserve the original callback error */
          }
        }
        throw error
      }
    }
  }

  private _finish(event: StreamEvent<T>) {
    this._ended = true
    this._failed = event.state === 'error'
    this._error = event.error ?? new Error('Remote stream failed')
    if (this._failed) this._queue = []
    for (const waiter of this._waiters.splice(0)) {
      if (this._failed) waiter.reject(this._error)
      else waiter.resolve({ value: undefined, done: true })
    }
    const handlers = this._stateHandlers.splice(0)
    this._dataHandlers = []
    const onClose = this._options.onClose
    this._options = {}
    onClose?.()
    this._notify(handlers, event)
  }

  emit(event: StreamEvent<T>) {
    if (this._ended) return
    if (event.state === 'end' || event.state === 'error') {
      this._finish(event)
      return
    }
    this._notify(this._stateHandlers, event)
    if (event.state !== 'chunk' || this._ended) return
    if (this._mode === 'callback') {
      this._notify(this._dataHandlers, event.data as T)
    } else {
      const waiter = this._waiters.shift()
      if (waiter) waiter.resolve({ value: event.data as T, done: false })
      else if (this._queue.length >= this._capacity) {
        const error = new Error('Stream buffer exceeded capacity ' + this._capacity)
        this.stop('error', error)
        throw error
      } else this._queue.push(event.data as T)
    }
  }

  stop(state: 'end' | 'error', reason?: unknown) {
    if (!this._ended) this.emit({ state, error: reason })
  }

  onState(handler: (event: StreamEvent<T>) => void) {
    if (!this._ended) this._stateHandlers.push(handler)
    return () => {
      this._stateHandlers = this._stateHandlers.filter(fn => fn !== handler)
    }
  }

  onData(handler: (data: T) => void) {
    if (this._mode === 'iterator')
      throw new Error('Cannot mix stream callback and iterator consumption')
    this._mode = 'callback'
    if (!this._ended) this._dataHandlers.push(handler)
    for (const value of this._queue.splice(0)) this._notify([handler], value)
    return () => {
      this._dataHandlers = this._dataHandlers.filter(fn => fn !== handler)
    }
  }

  async next(): Promise<IteratorResult<T>> {
    if (this._mode === 'callback')
      throw new Error('Cannot mix stream callback and iterator consumption')
    this._mode = 'iterator'
    if (this._failed) throw this._error
    if (this._queue.length) return { value: this._queue.shift()!, done: false }
    if (this._ended) return { value: undefined, done: true }
    return new Promise((resolve, reject) => this._waiters.push({ resolve, reject }))
  }

  async return(): Promise<IteratorResult<T>> {
    this._queue = []
    this.stop('end')
    return { value: undefined, done: true }
  }

  [Symbol.asyncIterator]() {
    return this
  }

  isEnded() {
    return this._ended
  }

  get bufferedSize() {
    return this._queue.length
  }
}
