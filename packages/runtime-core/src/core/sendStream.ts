import { composeMiddleware } from '@ws-flow/pipeline'
import type { WSEnvelope } from '@ws-flow/envelope'
import { abortable } from '../utils/abortable.js'
import { IdleDeadline, TaskTimeoutError } from '../taskResources.js'
import type {
  PeerLike,
  SendStreamDeps,
  SendStreamOptions,
  CoreStreamContext
} from '../types/index.js'

type StreamControlType = 'none' | 'stop' | 'end' | 'fail' | 'reject'

export class SendStream<Peer extends PeerLike = PeerLike> {
  private _state: 'init' | 'running' | 'ended' | 'error' = 'init'
  private _queue = Promise.resolve()

  private _streamId: string | undefined
  private _pipeline
  private _closed = false
  private _failed = false
  private _reason: unknown
  private _controller = new AbortController()
  private _closeHandlers = new Set<() => void>()
  private _generator?: AsyncGenerator<any>
  private _generatorReturned = false
  private _generatorStarted = false
  private _removeAbort?: () => void
  private _idle: IdleDeadline

  constructor(
    private _deps: SendStreamDeps<Peer>,
    private _peer: Peer,
    private _event?: string,
    private _options?: SendStreamOptions<Peer>
  ) {
    this._idle = new IdleDeadline(this._options?.idleTimeoutMs, () => {
      const error = new TaskTimeoutError(
        'STREAM_IDLE_TIMEOUT',
        this._options!.idleTimeoutMs!
      )
      this.cancel(error)
      try {
        this._deps.onIdleTimeout?.(error)
      } catch (failure) {
        console.error('[SendStream] idle observer failed', failure)
      }
    })
    this._pipeline = composeMiddleware<CoreStreamContext<Peer>>([
      ...this._deps.sendStreamMiddlewares,
      ...(this._options?.streamMiddlewares || [])
    ])
    const signal = this._options?.signal
    if (signal) {
      const abort = () => this.cancel(signal.reason)
      if (signal.aborted) abort()
      else {
        signal.addEventListener('abort', abort, { once: true })
        this._removeAbort = () => signal.removeEventListener('abort', abort)
      }
    }
    if (!this._closed) this._idle.touch()
  }

  isClosed() {
    return this._closed
  }

  get failed() {
    return this._failed
  }

  get error() {
    return this._reason
  }

  onClose(handler: () => void) {
    if (this._closed) handler()
    else this._closeHandlers.add(handler)
    return () => {
      this._closeHandlers.delete(handler)
    }
  }

  private _returnGenerator() {
    if (!this._generator || this._generatorReturned) return
    this._generatorReturned = true
    // Async generators cannot interrupt an arbitrary await. Request cleanup,
    // observe its rejection, but do not hold task settlement hostage to it.
    try {
      void Promise.resolve(this._generator.return(undefined)).catch(() => {})
    } catch {
      /* cleanup must not replace the task's original outcome */
    }
  }

  private _finish(failed = false, reason?: unknown) {
    if (this._closed) return
    this._closed = true
    this._failed = failed
    this._reason = reason
    this._state = failed ? 'error' : 'ended'
    this._idle.clear()
    this._removeAbort?.()
    this._removeAbort = undefined
    this._controller.abort(reason ?? new Error('Stream is closed'))
    this._returnGenerator()
    const handlers = [...this._closeHandlers]
    this._closeHandlers.clear()
    for (const handler of handlers) {
      try {
        handler()
      } catch (error) {
        console.error('[SendStream] cleanup observer failed', error)
      }
    }
  }

  cancel(reason: unknown = new Error('Stream cancelled')) {
    this._finish(true, reason)
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
  ): CoreStreamContext<Peer> {
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

  private _buildEnvelope(
    state: 'start' | 'chunk' | 'end' | 'error',
    ctx: CoreStreamContext<Peer>
  ): WSEnvelope {
    const builder = this._deps.envelopeBuilder

    switch (state) {
      case 'start': {
        const env =
          this._event === undefined
            ? builder.createStreamStart({
                priority: this._options?.priority,
                meta: this._options?.meta
              })
            : builder.createStreamStart({
                event: this._event as string,
                priority: this._options?.priority,
                meta: this._options?.meta
              })

        this._streamId = env.streamId
        if (!this._streamId) throw new Error('Stream builder must supply a streamId')
        this._deps.onStart?.(this._streamId)
        return env
      }
      case 'chunk':
        return builder.createStreamChunk({
          streamId: this._streamId!,
          data: ctx.data
        })
      case 'end':
        return builder.createStreamEnd({
          streamId: this._streamId!
        })
      case 'error':
        return builder.createStreamError({
          streamId: this._streamId!,
          error: ctx.error
        })
    }
  }

  private async _dispatchEnvelope(envelope: WSEnvelope) {
    const check = () => {
      this._idle.check()
      this._controller.signal.throwIfAborted()
    }
    check()
    await this._deps.sendDispatcher.dispatch(
      this._peer,
      envelope,
      {
        priority: this._options?.priority,
        meta: this._options?.meta,
        sendMiddlewares: this._options?.sendMiddlewares,
        signal: this._controller.signal
      },
      check
    )
    if (
      !this._closed &&
      (envelope.streamState === 'start' || envelope.streamState === 'chunk')
    )
      this._idle.touch()
    this._controller.signal.throwIfAborted()
  }

  private async _handleEmit(
    state: 'start' | 'chunk' | 'end' | 'error',
    data?: any,
    err?: unknown
  ) {
    if (this._state === 'ended' || this._state === 'error' || this._closed) return
    this._idle.check()
    this._controller.signal.throwIfAborted()

    if (state === 'start' && this._state !== 'init')
      throw new Error('Stream already started')
    if (state !== 'start' && this._state !== 'running')
      throw new Error('Stream must start before sending frames')
    if (state === 'start') this._state = 'running'

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
      err
    )

    await this._pipeline(ctx)
    this._idle.check()
    this._controller.signal.throwIfAborted()
    if (ctx.peer !== this._peer) throw new Error('Stream peer cannot be changed')
    if (ctx.state !== state)
      throw new Error('Stream state cannot be changed; use stream controls')

    if (control.type === 'stop' || control.type === 'reject') {
      this._finish(
        control.type === 'reject',
        control.reason ?? new Error('Stream rejected')
      )
      return
    }

    let nextState = state
    let nextError = ctx.error

    if (control.type === 'end') {
      nextState = 'end'
    }

    if (control.type === 'fail') {
      nextState = 'error'
      nextError = control.reason
    }

    if (state === 'start' && (nextState === 'end' || nextState === 'error')) {
      const startEnvelope = this._buildEnvelope('start', ctx)
      await this._dispatchEnvelope(startEnvelope)
    }

    const envelope = this._buildEnvelope(nextState, {
      ...ctx,
      error: nextError
    })

    await this._dispatchEnvelope(envelope)

    if (nextState === 'end') {
      this._finish()
    }

    if (nextState === 'error') {
      this._finish(true, nextError ?? new Error('Stream failed'))
    }
  }

  async emit(state: 'start' | 'chunk' | 'end' | 'error', data?: any, err?: unknown) {
    if (this._closed) {
      if (this._failed) throw this._reason
      return
    }
    const operation = this._queue.then(() => this._handleEmit(state, data, err))
    this._queue = operation.catch(() => {})
    try {
      await abortable(operation, this._controller.signal)
    } catch (error) {
      if (this._closed && !this._failed) return
      this.cancel(error)
      throw this._reason
    }
  }

  async runGenerator(gen: AsyncGenerator<any>) {
    if (this._generatorStarted) throw new Error('Stream generator already started')
    this._generatorStarted = true
    this._generator = gen
    try {
      await this.emit('start')
      while (!this._closed) {
        const result = await abortable(gen.next(), this._controller.signal)
        if (result.done) {
          this._generatorReturned = true
          await this.emit('end')
          return
        }
        await this.emit('chunk', result.value)
      }
    } catch (err) {
      if (this._closed && !this._failed) return
      if (!this._closed) {
        try {
          await this.emit('error', undefined, err)
        } catch {
          /* retain source failure */
        }
      }
      throw err
    } finally {
      this._returnGenerator()
      this._generator = undefined
    }
  }
}
