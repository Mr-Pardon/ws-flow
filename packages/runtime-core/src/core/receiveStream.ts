import { StreamConsumer, StreamEvent } from '@ws-flow/consumer'
import { composeMiddleware } from '@ws-flow/pipeline'
import { IdleDeadline, TaskTimeoutError } from '../taskResources.js'
import { abortable } from '../utils/abortable.js'
import type { CoreStreamContext, CoreStreamMiddleware, PeerLike } from '../types/index.js'

type StreamControlType = 'none' | 'stop' | 'end' | 'fail' | 'reject'

export class ReceiveStream<Peer extends PeerLike = PeerLike> {
  private _state: 'init' | 'running' | 'ended' | 'error' = 'init'
  private _consumer: StreamConsumer
  private _closeHandlers = new Set<() => void>()
  private _queue = Promise.resolve()
  private _localMiddlewares: CoreStreamMiddleware<Peer>[] = []
  private _controller = new AbortController()
  private _idle: IdleDeadline
  private _failed = false

  constructor(
    private _peer: Peer,
    private _receiveStream: CoreStreamMiddleware<Peer>[],
    options?: {
      capacity?: number
      idleTimeoutMs?: number | null
      onIdleTimeout?: (error: TaskTimeoutError) => void
    }
  ) {
    this._idle = new IdleDeadline(options?.idleTimeoutMs, () => {
      const error = new TaskTimeoutError('STREAM_IDLE_TIMEOUT', options!.idleTimeoutMs!)
      try {
        this.stop('error', error)
      } catch (failure) {
        console.error('[ReceiveStream] idle timeout cleanup failed', failure)
      }
      try {
        options?.onIdleTimeout?.(error)
      } catch (failure) {
        console.error('[ReceiveStream] idle timeout observer failed', failure)
      }
    })
    this._consumer = new StreamConsumer({
      capacity: options?.capacity,
      onClose: () => {
        this._idle.clear()
        const handlers = [...this._closeHandlers]
        this._closeHandlers.clear()
        this._localMiddlewares = []
        for (const handler of handlers) {
          try {
            handler()
          } catch (error) {
            console.error('[ReceiveStream] close handler failed', error)
          }
        }
      }
    })
    this._consumer.onState(event => {
      if (event.state !== 'end' && event.state !== 'error') return
      this._state = event.state === 'end' ? 'ended' : 'error'
      this._failed = event.state === 'error'
      this._controller.abort(event.error ?? new Error('Stream is closed'))
    })
  }

  private _createPipeline() {
    return composeMiddleware<CoreStreamContext<Peer, any>>([
      ...this._receiveStream,
      ...this._localMiddlewares
    ])
  }

  private _createContext(
    data: any,
    state: 'start' | 'chunk' | 'end' | 'error',
    controls: {
      stop: () => void
      end: () => void
      fail: (reason?: unknown) => void
      reject: (reason?: unknown) => void
    },
    err?: unknown
  ): CoreStreamContext<Peer, any> {
    return {
      peer: this._peer,
      signal: this._controller.signal,
      data,
      state,
      error: err,
      stop: controls.stop,
      end: controls.end,
      fail: controls.fail,
      reject: controls.reject
    }
  }

  private async _handlePush(
    data: any,
    state: 'start' | 'chunk' | 'end' | 'error',
    error?: unknown
  ) {
    if (this.isClosed()) return
    this._idle.check()
    if (this.isClosed()) return
    const progress =
      (state === 'start' && this._state === 'init') ||
      (state === 'chunk' && this._state === 'running')
    if (state === 'start' && this._state === 'init') {
      this._state = 'running'
      // Include stalled start middleware in the first idle period.
      this._idle.touch()
    }
    if (this.isClosed()) return

    const control: {
      type: StreamControlType
      reason?: unknown
    } = { type: 'none' }
    const setControl = (type: Exclude<StreamControlType, 'none'>, reason?: unknown) => {
      if (control.type !== 'none') return
      control.type = type
      control.reason = reason
    }

    const ctx = this._createContext(
      data,
      state,
      {
        stop: () => setControl('stop'),
        end: () => setControl('end'),
        fail: (reason?: unknown) => setControl('fail', reason),
        reject: (reason?: unknown) => setControl('reject', reason)
      },
      error
    )
    const pipeline = this._createPipeline()

    await abortable(pipeline(ctx), this._controller.signal)
    this._idle.check()
    if (this.isClosed()) return
    if (ctx.peer !== this._peer) throw new Error('Stream peer cannot be changed')
    if (ctx.state !== state)
      throw new Error('Stream state cannot be changed; use stream controls')

    // TODO: 区分stop和reject(reject进入observers)
    if (control.type === 'stop' || control.type === 'reject') {
      if (state === 'end') {
        this._state = 'ended'
        this._consumer.stop('end')
      }

      if (state === 'error') {
        this._state = 'error'
        this._consumer.stop('error', ctx.error)
      }
      return
    }

    if (control.type === 'end') {
      this._state = 'ended'
      this._consumer.stop('end')
      return
    }

    if (control.type === 'fail') {
      this._state = 'error'
      this._consumer.stop('error', control.reason)
      return
    }

    if (progress) this._idle.touch()
    if (this.isClosed()) return
    this._consumer.emit({
      state,
      data: ctx.data,
      error: ctx.error
    })
  }

  async push(data: any, state: 'start' | 'chunk' | 'end' | 'error', error?: unknown) {
    if (this.isClosed()) return

    const operation = this._queue.then(() => this._handlePush(data, state, error))
    this._queue = operation.catch(() => {})
    try {
      await operation
    } catch (failure) {
      if (this.isClosed() && !this._failed) return
      this.stop('error', failure)
      throw failure
    }
  }

  use(middlewares: CoreStreamMiddleware<Peer> | CoreStreamMiddleware<Peer>[]) {
    const arr = Array.isArray(middlewares) ? middlewares : [middlewares]
    this._localMiddlewares.push(...arr)
    return this
  }

  [Symbol.asyncIterator]() {
    return this
  }

  next() {
    return this._consumer.next()
  }

  return() {
    return this._consumer.return()
  }

  onClose(handler: () => void) {
    if (this.isClosed()) handler()
    else this._closeHandlers.add(handler)
    return () => {
      this._closeHandlers.delete(handler)
    }
  }

  stop(state: 'end' | 'error', reason?: unknown) {
    if (this.isClosed()) return
    this._state = state === 'end' ? 'ended' : 'error'
    this._consumer.stop(state, reason)
  }

  isClosed() {
    return this._consumer.isEnded()
  }

  onState(handler: (event: StreamEvent<any>) => void) {
    this._consumer.onState(handler)
  }

  onData(handler: (data: any) => void) {
    this._consumer.onData(handler)
  }
}
