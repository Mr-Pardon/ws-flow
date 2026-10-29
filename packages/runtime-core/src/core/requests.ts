import { ProtocolCapabilityError } from '../protocol.js'
import {
  ActiveTaskBudget,
  TaskCapacityError,
  TaskTimeoutError,
  validateTaskTimeouts
} from '../taskResources.js'
import type { ActiveTaskKind, RuntimeTaskTimeouts } from '../taskResources.js'
import type { ProtocolAdapter, RuntimeMessage } from '../protocol.js'
import type { PeerLike, SendOptions } from '../types/index.js'
import type { StreamTask } from './streamTask.js'

export class RequestError extends Error {
  constructor(
    public readonly code: string,
    message: string
  ) {
    super(message)
    this.name = 'RequestError'
  }
}

export interface RequestOptions<Peer extends PeerLike = PeerLike>
  extends SendOptions<Peer> {
  /** Includes queue and send time. Default 30000; null explicitly disables it. */
  timeoutMs?: number | null
}

export interface RequestStreamOptions<Peer extends PeerLike = PeerLike>
  extends RequestOptions<Peer> {
  capacity?: number
  /** Starts after the stream start frame; null disables the runtime idle deadline. */
  idleTimeoutMs?: number | null
}

export type StreamRequestHandler<
  Peer extends PeerLike = PeerLike,
  Input = unknown,
  Chunk = unknown
> = RequestHandler<Peer, Input, AsyncGenerator<Chunk>>

export interface RequestContext<Peer extends PeerLike = PeerLike, Input = unknown> {
  readonly peer: Peer
  readonly event: string
  readonly requestId: string
  readonly signal: AbortSignal
  data: Input
}

export type RequestHandler<
  Peer extends PeerLike = PeerLike,
  Input = unknown,
  Result = unknown
> = (ctx: RequestContext<Peer, Input>) => Result | Promise<Result>

type Pending = {
  finish(error: unknown, data?: unknown, failed?: boolean, notifyRemote?: boolean): void
  stream?: StreamTask<any>
}

type Handler<Peer extends PeerLike> = {
  run: RequestHandler<Peer, any, any>
  mode: 'single' | 'stream'
}

type Incoming = {
  finish(reason?: unknown): boolean
}

let runtimeSequence = 0

/** Owns correlation and lifetime, independent of wire encoding and sockets. */
export class RequestManager<Peer extends PeerLike> {
  private _pending = new Map<Peer, Map<string, Pending>>()
  private _incoming = new Map<Peer, Map<string, Incoming>>()
  private _handlers = new Map<string, Handler<Peer>>()
  private _localHandlers = new WeakMap<Peer, Map<string, Handler<Peer>>>()
  private _sequence = 0
  private _prefix = `request-${++runtimeSequence}`
  private _budget: ActiveTaskBudget<Peer>
  private _timeouts: Required<RuntimeTaskTimeouts>
  private _closedPeers = new WeakSet<Peer>()
  private _disposed = false

  constructor(
    private _deps: {
      protocol: ProtocolAdapter
      budget?: ActiveTaskBudget<Peer>
      timeouts?: RuntimeTaskTimeouts
      onTaskError?(
        peer: Peer,
        error: TaskCapacityError | TaskTimeoutError,
        kind: ActiveTaskKind
      ): void
      send(
        peer: Peer,
        message: RuntimeMessage,
        options?: SendOptions<Peer>,
        onTransportStart?: () => void
      ): Promise<void>
      report(error: unknown, peer: Peer): void
      sendStream(
        peer: Peer,
        requestId: string,
        source: AsyncGenerator<any>,
        signal: AbortSignal,
        onStart?: (streamId: string) => void,
        onTerminal?: () => void,
        onTaskTimeout?: (error: TaskTimeoutError) => void
      ): Promise<void>
    }
  ) {
    this._budget = _deps.budget ?? new ActiveTaskBudget<Peer>()
    this._timeouts = validateTaskTimeouts(_deps.timeouts)
  }

  register<Input, Result>(
    event: string,
    handler: RequestHandler<Peer, Input, Result>,
    peer?: Peer,
    mode: 'single' | 'stream' = 'single'
  ) {
    if (!event || typeof event !== 'string' || typeof handler !== 'function')
      throw new TypeError('A request event and handler are required')
    let handlers = peer ? this._localHandlers.get(peer) : this._handlers
    if (!handlers) this._localHandlers.set(peer!, (handlers = new Map()))
    if (handlers.has(event))
      throw new Error(`Request handler already registered: ${event}`)
    const record = { run: handler, mode }
    handlers.set(event, record)
    const current = handlers
    return () => {
      if (current.get(event) === record) current.delete(event)
    }
  }

  async request<Result>(
    peer: Peer,
    event: string,
    data: unknown,
    options: RequestOptions<Peer> = {},
    stream?: StreamTask<any>
  ): Promise<Result> {
    if (!this._deps.protocol.request)
      throw new ProtocolCapabilityError(this._deps.protocol.name, 'requests')
    if (stream && !this._deps.protocol.request.stream)
      throw new ProtocolCapabilityError(this._deps.protocol.name, 'stream requests')
    if (typeof event !== 'string' || !event)
      throw new TypeError('Request event must be nonempty')
    options.signal?.throwIfAborted()
    const timeout = options.timeoutMs === undefined ? 30_000 : options.timeoutMs
    if (
      timeout !== null &&
      (!Number.isFinite(timeout) || timeout <= 0 || timeout > 2_147_483_647)
    ) {
      throw new TypeError('timeoutMs must be positive and at most 2147483647, or null')
    }
    const release = this._reserve(peer, 'outgoingRequests')
    let tasks = this._pending.get(peer)
    if (!tasks) this._pending.set(peer, (tasks = new Map()))
    const current = tasks
    const requestId = `${this._prefix}:${++this._sequence}`
    const controller = new AbortController()
    return new Promise<Result>((resolve, reject) => {
      let settled = false
      let transportStarted = false
      let timer: ReturnType<typeof setTimeout> | undefined
      const abort = () => finish(options.signal!.reason, undefined, true, true)
      const finish = (
        error: unknown,
        value?: unknown,
        failed = false,
        notifyRemote = false
      ) => {
        if (settled) return
        settled = true
        current.delete(requestId)
        if (!current.size) this._pending.delete(peer)
        release()
        if (timer !== undefined) clearTimeout(timer)
        options.signal?.removeEventListener('abort', abort)
        stream?.finish(error, failed)
        controller.abort(error ?? new Error('Request settled'))
        if (failed) reject(error)
        else resolve(value as Result)
        if (notifyRemote && transportStarted && this._deps.protocol.request?.cancel) {
          // Best effort; local settlement never waits for the remote or send queue.
          void this._deps
            .send(peer, { kind: 'request_cancel', requestId })
            .catch(failure => this._reportSendFailure(failure, peer))
        }
      }
      current.set(requestId, { finish, stream })
      stream?.bind((error, failed, notify) => finish(error, undefined, failed, notify))
      options.signal?.addEventListener('abort', abort, { once: true })
      if (timeout !== null)
        timer = setTimeout(
          () =>
            finish(
              new RequestError('TIMEOUT', 'Request timed out'),
              undefined,
              true,
              true
            ),
          timeout
        )
      // Registration precedes send: loopback responses may arrive synchronously.
      void this._deps
        .send(
          peer,
          {
            kind: 'request',
            requestId,
            event,
            data,
            ...(stream ? { resultMode: 'stream' as const } : {})
          },
          {
            ...options,
            signal: controller.signal
          },
          () => {
            transportStarted = true
          }
        )
        .catch(error => finish(error, undefined, true))
    })
  }

  async accept(peer: Peer, message: RuntimeMessage): Promise<boolean> {
    if (!message.kind) return false
    if (message.kind === 'request_cancel') {
      const tasks = this._incoming.get(peer)
      const task = tasks?.get(message.requestId!)
      task?.finish(new RequestError('CANCELLED', 'Remote cancelled request'))
      return true
    }
    if (message.kind !== 'request') {
      const task = this._pending.get(peer)?.get(message.requestId!)
      if (message.kind === 'request_stream') {
        if (task?.stream) {
          const error = message.error as { code: string; message: string }
          await task.stream.accept({
            ...message,
            error:
              message.streamState === 'error'
                ? new RequestError(error.code, error.message)
                : undefined
          })
        } else
          task?.finish(
            new RequestError('MODE_MISMATCH', 'Expected single response'),
            undefined,
            true,
            true
          )
        return true
      }
      if (message.kind === 'response' && task?.stream) {
        task.finish(
          new RequestError('MODE_MISMATCH', 'Expected stream response'),
          undefined,
          true,
          true
        )
        return true
      }
      const error = message.error as { code: string; message: string }
      this._pending
        .get(peer)
        ?.get(message.requestId!)
        ?.finish(
          message.kind === 'request_error'
            ? new RequestError(error.code, error.message)
            : undefined,
          message.data,
          message.kind === 'request_error'
        )
      return true // Late/unknown responses must not become ordinary notifications.
    }
    let tasks = this._incoming.get(peer)
    if (tasks?.has(message.requestId!)) return true // Preserve the original task.
    let release: () => void
    try {
      release = this._reserve(peer, 'incomingRequests')
    } catch (error) {
      this.reject(peer, message, error)
      return true
    }
    if (!tasks) this._incoming.set(peer, (tasks = new Map()))
    const controller = new AbortController(),
      current = tasks
    const timeout = this._timeouts.incomingRequestMs
    const deadline = timeout === null ? undefined : performance.now() + timeout
    let active = true,
      terminalStarted = false,
      streamId: string | undefined,
      timer: ReturnType<typeof setTimeout> | undefined
    const task: Incoming = {
      finish: (reason = new Error('Request settled')) => {
        if (!active) return false
        active = false
        if (current.get(message.requestId!) === task) current.delete(message.requestId!)
        if (!current.size && this._incoming.get(peer) === current)
          this._incoming.delete(peer)
        if (timer !== undefined) clearTimeout(timer)
        release()
        controller.abort(reason)
        return true
      }
    }
    current.set(message.requestId!, task)
    const terminate = (error: TaskTimeoutError) => {
      if (!task.finish(error)) return
      this._deps.onTaskError?.(peer, error, 'incomingRequests')
      // A terminal already handed to transport cannot be recalled or sent twice.
      if (terminalStarted) return
      void this._deps
        .send(peer, this._errorFrame(message, error, streamId))
        .catch(failure => this._reportSendFailure(failure, peer))
    }
    const expire = () => terminate(new TaskTimeoutError('EXECUTION_TIMEOUT', timeout!))
    const isActive = () => {
      if (active && deadline !== undefined && performance.now() >= deadline) expire()
      return active
    }
    const onTerminal = () => {
      if (!isActive()) throw controller.signal.reason
      terminalStarted = true
    }
    if (timeout !== null) timer = setTimeout(expire, timeout)
    const handler =
      this._localHandlers.get(peer)?.get(message.event!) ??
      this._handlers.get(message.event!)
    const execute = async () => {
      try {
        let response: RuntimeMessage
        try {
          if (!handler)
            throw new RequestError(
              'METHOD_NOT_FOUND',
              `No request handler: ${message.event}`
            )
          if (handler.mode !== (message.resultMode ?? 'single'))
            throw new RequestError(
              'MODE_MISMATCH',
              'Request result mode does not match handler'
            )
          const data = await handler.run({
            peer,
            data: message.data,
            event: message.event!,
            requestId: message.requestId!,
            signal: controller.signal
          })
          if (handler.mode === 'stream') {
            if (
              !data ||
              typeof data.next !== 'function' ||
              typeof data.return !== 'function' ||
              typeof data[Symbol.asyncIterator] !== 'function'
            ) {
              throw new RequestError(
                'INVALID_STREAM',
                'Stream handler must return an AsyncGenerator'
              )
            }
            if (!isActive()) {
              // An uncooperative handler may produce its generator after cancellation.
              void Promise.resolve()
                .then(() => data.return(undefined))
                .catch(error => this._deps.report(error, peer))
              return
            }
            await this._deps.sendStream(
              peer,
              message.requestId!,
              data,
              controller.signal,
              id => {
                if (!isActive()) throw controller.signal.reason
                streamId = id
              },
              onTerminal,
              terminate
            )
            isActive()
            return
          }
          response = { kind: 'response', requestId: message.requestId, data }
        } catch (error) {
          if (!isActive()) return
          if (terminalStarted) {
            this._deps.report(error, peer)
            return
          }
          response = this._errorFrame(message, error, streamId)
        }
        try {
          if (isActive())
            await this._deps.send(
              peer,
              response,
              { signal: controller.signal },
              onTerminal
            )
          isActive()
        } catch (error) {
          if (isActive()) this._deps.report(error, peer)
        }
      } finally {
        task.finish()
      }
    }
    // Never occupy the receive scheduler while waiting for a handler or reverse request.
    void execute().catch(error => this._deps.report(error, peer))
    return true
  }

  private _reserve(peer: Peer, kind: ActiveTaskKind) {
    try {
      return this._budget.reserve(peer, kind)
    } catch (error) {
      if (error instanceof TaskCapacityError) this._deps.onTaskError?.(peer, error, kind)
      throw error
    }
  }

  private _reportSendFailure(error: unknown, peer: Peer) {
    if (!this._disposed && !this._closedPeers.has(peer)) this._deps.report(error, peer)
  }

  private _errorFrame(
    message: RuntimeMessage,
    error: unknown,
    streamId?: string
  ): RuntimeMessage {
    return {
      ...(streamId
        ? { kind: 'request_stream' as const, streamId, streamState: 'error' as const }
        : { kind: 'request_error' as const }),
      requestId: message.requestId,
      error: {
        code:
          error instanceof RequestError ||
          error instanceof TaskCapacityError ||
          error instanceof TaskTimeoutError
            ? error.code
            : 'HANDLER_ERROR',
        message: error instanceof Error ? error.message : 'Request processing failed'
      }
    }
  }

  reject(peer: Peer, message: RuntimeMessage, error: unknown) {
    if (message.kind === 'request') {
      void this._deps
        .send(peer, this._errorFrame(message, error))
        .catch(failure => this._reportSendFailure(failure, peer))
    } else if (
      message.kind === 'response' ||
      message.kind === 'request_error' ||
      message.kind === 'request_stream'
    ) {
      this._pending
        .get(peer)
        ?.get(message.requestId!)
        ?.finish(error, undefined, true, true)
    }
  }

  closePeer(peer: Peer, reason: unknown) {
    this._closedPeers.add(peer)
    for (const task of this._pending.get(peer)?.values() ?? [])
      task.finish(reason, undefined, true)
    const incoming = this._incoming.get(peer)
    this._incoming.delete(peer)
    for (const task of incoming?.values() ?? []) task.finish(reason)
    this._localHandlers.delete(peer)
  }

  dispose() {
    this._disposed = true
    for (const peer of new Set([...this._pending.keys(), ...this._incoming.keys()]))
      this.closePeer(peer, new Error('Runtime is closed'))
    this._handlers.clear()
    this._localHandlers = new WeakMap()
  }
}
