import {
  RuntimeCore,
  genericAdapter,
  wsFlowAdapter,
  ByteBudget,
  TransportLimitError,
  TaskCapacityError,
  TaskTimeoutError,
  TransportSendQueue,
  wireByteLength,
  waitForWritable,
  validateSendLimits
} from '@ws-flow/runtime-core'
import type {
  ProtocolAdapter,
  PeerLike,
  RequestOptions,
  RequestStreamOptions,
  RequestHandler,
  StreamRequestHandler,
  SubscriptionFilter,
  SubscriptionOptions,
  ReceivedMessage,
  SendOptions,
  SendStreamOptions,
  RuntimePipelines,
  CoreReceiveContext,
  ByteLimits,
  SendTransportLimits,
  PreparedTransportSend,
  RuntimeTaskLimits,
  RuntimeTaskTimeouts
} from '@ws-flow/runtime-core'
import {
  ImmediateScheduler,
  QueueCapacityError,
  SessionScheduler,
  type QueueLimits
} from '@ws-flow/scheduler'

export interface ClientSocket {
  readonly readyState: number
  readonly bufferedAmount?: number
  binaryType: string
  send(data: any): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, listener: (event: any) => void): void
  removeEventListener(type: string, listener: (event: any) => void): void
}

export interface ClientConnection {
  socket: ClientSocket
  send(data: any): void | Promise<void>
}

export type SocketFactory = (
  url: string,
  protocols?: string | string[]
) => ClientConnection | Promise<ClientConnection>

export interface ClientPeer extends PeerLike {
  readonly socket: ClientSocket
}

export type ClientState =
  | 'idle'
  | 'connecting'
  | 'open'
  | 'reconnecting'
  | 'closed'
  | 'disposed'

export interface ClientOptions {
  protocol?: 'generic' | 'ws-flow' | ProtocolAdapter
  socketFactory?: SocketFactory
  codec?: {
    encode(message: unknown): unknown
    decode(data: unknown): unknown | Promise<unknown>
  }
  connectTimeoutMs?: number
  reconnect?: false | { delayMs?: number; maxAttempts?: number }
  queueLimits?: { receive?: QueueLimits; send?: QueueLimits }
  transportLimits?: { receive?: ByteLimits; send?: SendTransportLimits }
  taskLimits?: RuntimeTaskLimits
  taskTimeouts?: RuntimeTaskTimeouts
}

const jsonCodec = {
  encode: (message: unknown) => JSON.stringify(message),
  decode: (data: unknown) => (typeof data === 'string' ? JSON.parse(data) : data)
}
let nextPeerId = 0

/** Connection host only; shared RuntimeCore owns all messaging tasks. */
export class Client<Events extends Record<string, any> = Record<string, any>> {
  private _runtime: RuntimeCore<ClientPeer>
  private _receiveBytes: ByteBudget<ClientPeer>
  private _sendBytes: ByteBudget<ClientPeer>
  private _sendLimits: SendTransportLimits
  private _sendQueue = new TransportSendQueue<ClientPeer>()
  private _receiveScheduler: SessionScheduler
  private _receiveController?: AbortController
  private _peer?: ClientPeer
  private _connection?: ClientConnection
  private _state: ClientState = 'idle'
  private _generation = 0
  private _connecting?: Promise<void>
  private _cancelConnect?: (error: unknown) => void
  private _unbind?: () => void
  private _retry?: ReturnType<typeof setTimeout>
  private _attempts = 0
  private _url?: string
  private _protocols?: string | string[]
  private _manual = false
  private _issues = new Set<(error: unknown) => void | Promise<void>>()
  private _resourceErrors = new WeakSet<
    QueueCapacityError | TransportLimitError | TaskCapacityError | TaskTimeoutError
  >()
  private _states = new Set<(state: ClientState) => void | Promise<void>>()
  private _streamHandlers = new Map<
    string,
    (ctx: CoreReceiveContext<ClientPeer>) => void | Promise<void>
  >()

  constructor(
    private _options: ClientOptions,
    private _factory: SocketFactory
  ) {
    const timeout = _options.connectTimeoutMs ?? 10_000
    if (!Number.isFinite(timeout) || timeout <= 0 || timeout > 2_147_483_647)
      throw new TypeError('Invalid connectTimeoutMs')
    if (_options.reconnect) {
      const { delayMs = 1000, maxAttempts = 5 } = _options.reconnect
      if (
        !Number.isFinite(delayMs) ||
        delayMs < 0 ||
        delayMs > 2_147_483_647 ||
        !Number.isInteger(maxAttempts) ||
        maxAttempts < 0
      )
        throw new TypeError('Invalid reconnect policy')
    }
    const protocol =
      _options.protocol === 'ws-flow'
        ? wsFlowAdapter()
        : !_options.protocol || _options.protocol === 'generic'
          ? genericAdapter()
          : _options.protocol
    this._receiveScheduler = new SessionScheduler(_options.queueLimits?.receive)
    this._receiveBytes = new ByteBudget(_options.transportLimits?.receive)
    this._sendLimits = validateSendLimits(_options.transportLimits?.send)
    this._sendBytes = new ByteBudget(this._sendLimits)
    this._runtime = new RuntimeCore<ClientPeer>({
      taskLimits: _options.taskLimits,
      taskTimeouts: _options.taskTimeouts,
      base: {
        protocol,
        transport: {
          prepare: (peer, message, signal) => this._prepareSend(peer, message, signal),
          send: async (peer, message) => {
            const prepared = await this._prepareSend(peer, message)
            try {
              await prepared.send()
            } finally {
              prepared.release()
            }
          }
        }
      },
      schedulers: {
        in: new ImmediateScheduler(),
        out: new SessionScheduler(_options.queueLimits?.send)
      }
    })
    this._runtime.useQueueErrorObserver(info =>
      this._handleResourceFailure(info.peer, info.error, info.fatal)
    )
    this._runtime.useTransportErrorObserver(info =>
      this._handleResourceFailure(info.peer, info.error, info.fatal)
    )
    this._runtime.useTaskErrorObserver(info => {
      this._resourceErrors.add(info.error)
      this._report(info.error)
    })
    this._runtime.useStreamErrorObserver(error => this._reportTaskError(error))
    this._runtime.useRequestErrorObserver(error => this._reportTaskError(error))
    this._runtime.useInvalidStreamObserver(info => this._report(new Error(info.message)))
    this._runtime.pipelines.useReceiveEvent({
      matcher: () => true,
      middlewares: ctx => {
        if (!ctx.stream) return
        const handler = this._streamHandlers.get(ctx.envelope.event ?? '')
        if (!handler) {
          ctx.stream.stop('error', new Error('No stream handler'))
          return
        }
        return handler(ctx)
      }
    })
  }

  get state() {
    return this._state
  }

  private async _prepareSend(
    peer: ClientPeer,
    message: unknown,
    signal?: AbortSignal
  ): Promise<PreparedTransportSend> {
    const connection = this._connection
    if (peer !== this._peer || peer.state !== 'open' || !connection)
      throw new Error('Connection is closed')
    signal?.throwIfAborted()
    const raw = (this._options.codec ?? jsonCodec).encode(message)
    signal?.throwIfAborted()
    const size =
      this._sendBytes.enabled || this._sendLimits.highWaterMark !== undefined
        ? wireByteLength(raw)
        : 0
    const release = this._sendBytes.enabled
      ? this._sendBytes.reserve(peer, size)
      : () => {}
    try {
      const prepare = async () => {
        await waitForWritable(connection.socket, size, this._sendLimits, signal)
        return {
          release,
          send: () => {
            signal?.throwIfAborted()
            if (
              peer !== this._peer ||
              connection !== this._connection ||
              peer.state !== 'open' ||
              connection.socket.readyState !== 1
            )
              throw new Error('Connection is closed')
            return connection.send(raw)
          }
        }
      }
      return this._sendLimits.highWaterMark === undefined
        ? await prepare()
        : await this._sendQueue.prepare(peer, signal, prepare)
    } catch (error) {
      release()
      throw error
    }
  }

  get peer() {
    return this._peer
  }

  getWsInstance() {
    return this._connection?.socket ?? null
  }

  private _report(error: unknown) {
    if (!this._issues.size) console.error('[WSClient]', error)
    for (const handler of [...this._issues])
      void Promise.resolve()
        .then(() => handler(error))
        .catch(() => {})
  }

  private _handleResourceFailure(
    peer: ClientPeer,
    error: QueueCapacityError | TransportLimitError,
    fatal: boolean
  ) {
    this._resourceErrors.add(error)
    this._report(error)
    if (!fatal || this._peer !== peer) return
    this._generation++
    this._drop(error)
    this._closeSocket(
      peer.socket,
      error instanceof QueueCapacityError ? 4008 : 4009,
      'Message resource limit exceeded'
    )
    this._setState('closed')
    this._scheduleReconnect()
  }

  private _reportTaskError(error: unknown) {
    if (
      (error instanceof QueueCapacityError ||
        error instanceof TransportLimitError ||
        error instanceof TaskCapacityError ||
        error instanceof TaskTimeoutError) &&
      this._resourceErrors.has(error)
    )
      return
    this._report(error)
  }

  private _setState(state: ClientState) {
    this._state = state
    for (const handler of [...this._states])
      void Promise.resolve()
        .then(() => handler(state))
        .catch(error => this._report(error))
  }

  onIssue(handler: (error: unknown) => void | Promise<void>) {
    this._issues.add(handler)
    return () => {
      this._issues.delete(handler)
    }
  }

  onState(handler: (state: ClientState) => void | Promise<void>) {
    this._states.add(handler)
    return () => {
      this._states.delete(handler)
    }
  }

  private _closeSocket(socket: ClientSocket, code?: number, reason?: string) {
    if (socket.readyState === 3) return
    const ignore = () => {}
    const cleanup = () => {
      socket.removeEventListener('error', ignore)
      socket.removeEventListener('close', cleanup)
    }
    // ws can emit error when a connecting socket is closed.
    socket.addEventListener('error', ignore)
    socket.addEventListener('close', cleanup)
    try {
      socket.close(code, reason)
    } catch (error) {
      cleanup()
      this._report(error)
    }
  }

  connect(url: string, protocols?: string | string[]): Promise<void> {
    if (this._state === 'disposed') return Promise.reject(new Error('Client is disposed'))
    if (this._connecting || this._peer?.state === 'open')
      return Promise.reject(new Error('Client is already connected or connecting'))
    if (this._retry) clearTimeout(this._retry)
    this._retry = undefined
    this._manual = false
    this._attempts = 0
    this._url = url
    this._protocols = protocols
    return this._open(url, protocols)
  }

  private _open(url: string, protocols?: string | string[]): Promise<void> {
    const generation = ++this._generation
    this._setState(this._attempts ? 'reconnecting' : 'connecting')
    const promise = new Promise<void>((resolve, reject) => {
      let settled = false
      const timer = setTimeout(
        () => fail(new Error('Connection timed out')),
        this._options.connectTimeoutMs ?? 10_000
      )
      const finish = (error?: unknown) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        this._cancelConnect = undefined
        error === undefined ? resolve() : reject(error)
      }
      const fail = (error: unknown) => {
        if (settled) return
        finish(error)
        if (generation !== this._generation) return
        this._generation++
        const socket = this._connection?.socket
        this._drop(error)
        if (socket) this._closeSocket(socket)
        this._setState('closed')
        this._scheduleReconnect()
      }
      this._cancelConnect = fail
      void Promise.resolve()
        .then(() => (this._options.socketFactory ?? this._factory)(url, protocols))
        .then(connection => {
          if (generation !== this._generation || settled) {
            this._closeSocket(connection.socket)
            return
          }
          this._attach(
            connection,
            () => {
              this._attempts = 0
              finish()
            },
            error => {
              if (!settled) fail(error)
              else {
                this._drop(error)
                this._setState('closed')
                this._scheduleReconnect()
              }
            }
          )
        })
        .catch(fail)
    })
    this._connecting = promise
    void promise.then(
      () => {
        if (this._connecting === promise) this._connecting = undefined
      },
      () => {
        if (this._connecting === promise) this._connecting = undefined
      }
    )
    return promise
  }

  private _attach(
    connection: ClientConnection,
    opened: () => void,
    closed: (reason: unknown) => void
  ) {
    const socket = connection.socket
    this._connection = connection
    socket.binaryType = 'arraybuffer'
    const peer: ClientPeer = { id: `client-${++nextPeerId}`, state: 'closed', socket }
    this._peer = peer
    const controller = new AbortController()
    this._receiveController = controller
    const open = () => {
      if (this._peer !== peer) return
      peer.state = 'open'
      this._setState('open')
      opened()
    }
    const message = (event: { data: unknown }) => {
      if (this._peer !== peer || peer.state !== 'open') return
      let release = () => {}
      try {
        if (this._receiveBytes.enabled)
          release = this._receiveBytes.reserve(peer, wireByteLength(event.data))
      } catch (error) {
        if (error instanceof TransportLimitError)
          this._handleResourceFailure(peer, error, true)
        else this._report(error)
        return
      }
      void this._receiveScheduler
        .schedule({
          type: 'receive',
          peerId: peer.id,
          signal: controller.signal,
          run: async () => {
            if (this._peer !== peer || peer.state !== 'open') return
            const raw = await (this._options.codec ?? jsonCodec).decode(event.data)
            if (this._peer === peer && peer.state === 'open')
              await this._runtime.dispatchers.receive.dispatch(peer, raw)
          }
        })
        .finally(release)
        .catch(error => {
          if (controller.signal.aborted) return
          if (error instanceof QueueCapacityError)
            this._handleResourceFailure(peer, error, true)
          else this._reportTaskError(error)
        })
    }
    const close = () => {
      if (this._peer === peer) closed(new Error('Connection is closed'))
    }
    const error = (event: any) => {
      if (this._peer !== peer) return
      const reason =
        event.error instanceof Error ? event.error : new Error('WebSocket error')
      if (peer.state !== 'open') closed(reason)
      else this._report(reason)
    }
    const listeners = [
      ['open', open],
      ['message', message],
      ['close', close],
      ['error', error]
    ] as const
    for (const [name, listener] of listeners) socket.addEventListener(name, listener)
    this._unbind = () => {
      for (const [name, listener] of listeners) socket.removeEventListener(name, listener)
    }
    if (socket.readyState === 1) open()
    else if (socket.readyState !== 0) close()
  }

  attach(connection: ClientConnection) {
    if (this._state === 'disposed' || this._connection || this._connecting)
      throw new Error('Client cannot attach in its current state')
    if (connection.socket.readyState !== 1)
      throw new Error('Takeover requires an open WebSocket')
    if (this._retry) clearTimeout(this._retry)
    this._retry = undefined
    this._manual = false
    this._url = undefined
    this._attach(
      connection,
      () => {},
      error => {
        this._drop(error)
        this._setState('closed')
      }
    )
    return this
  }

  private _drop(error: unknown) {
    this._unbind?.()
    this._unbind = undefined
    this._receiveController?.abort(error)
    this._receiveController = undefined
    if (this._peer) {
      this._receiveBytes.closePeer(this._peer)
      this._sendBytes.closePeer(this._peer)
      this._peer.state = 'closed'
      this._runtime.closePeer(this._peer, error)
    }
    this._peer = undefined
    this._connection = undefined
    this._runtime.clearSubscriptions()
  }

  private _scheduleReconnect() {
    const policy = this._options.reconnect
    if (
      !policy ||
      this._manual ||
      !this._url ||
      this._state === 'disposed' ||
      this._attempts >= (policy.maxAttempts ?? 5)
    )
      return
    this._attempts++
    this._setState('reconnecting')
    this._retry = setTimeout(() => {
      this._retry = undefined
      if (!this._manual && this._url)
        void this._open(this._url, this._protocols).catch(error => this._report(error))
    }, policy.delayMs ?? 1000)
  }

  close(code?: number, reason?: string) {
    if (this._state === 'disposed') return
    this._manual = true
    if (this._retry) clearTimeout(this._retry)
    this._retry = undefined
    this._cancelConnect?.(new Error('Connection is closed'))
    this._generation++
    const socket = this._connection?.socket
    this._drop(new Error('Connection is closed'))
    if (socket) this._closeSocket(socket, code, reason)
    this._setState('closed')
  }

  detach() {
    if (this._state === 'disposed') return
    if (this._connecting) throw new Error('Cannot detach while connecting')
    this._manual = true
    if (this._retry) clearTimeout(this._retry)
    this._retry = undefined
    this._generation++
    this._drop(new Error('Connection detached'))
    this._setState('closed')
  }

  dispose() {
    if (this._state === 'disposed') return
    this.close()
    this._runtime.dispose()
    this._receiveBytes.clear()
    this._sendBytes.clear()
    this._streamHandlers.clear()
    this._setState('disposed')
    this._states.clear()
    this._issues.clear()
  }

  private _openPeer() {
    if (!this._peer || this._peer.state !== 'open')
      throw new Error('Connection is not open')
    return this._peer
  }

  async send(data: unknown, options?: SendOptions<ClientPeer>) {
    return this._runtime.send(this._openPeer(), data, options)
  }

  async emit(
    event: Extract<keyof Events, string>,
    data: Events[keyof Events],
    options?: SendOptions<ClientPeer>
  ) {
    return this._runtime.emit(this._openPeer(), event, data, options)
  }

  async request<Result = unknown>(
    event: string,
    input: unknown,
    options?: RequestOptions<ClientPeer>
  ): Promise<Result> {
    return this._runtime.request<Result>(this._openPeer(), event, input, options)
  }

  requestStream<Chunk = unknown>(
    event: string,
    input: unknown,
    options?: RequestStreamOptions<ClientPeer>
  ) {
    return this._runtime.requestStream<Chunk>(this._openPeer(), event, input, options)
  }

  handleRequest<Input = unknown, Result = unknown>(
    event: string,
    handler: RequestHandler<ClientPeer, Input, Result>
  ) {
    return this._runtime.handleRequest(event, handler)
  }

  handleStream<Input = unknown, Chunk = unknown>(
    event: string,
    handler: StreamRequestHandler<ClientPeer, Input, Chunk>
  ) {
    return this._runtime.handleStream(event, handler)
  }

  async stream(
    event: string,
    source: AsyncGenerator<unknown> | (() => AsyncGenerator<unknown>),
    options?: SendStreamOptions<ClientPeer>
  ) {
    const sender = this._runtime.createSendStream(this._openPeer(), event, options)
    try {
      return await sender.runGenerator(typeof source === 'function' ? source() : source)
    } catch (error) {
      sender.cancel(error)
      throw error
    }
  }

  subscribe(
    filter: SubscriptionFilter<ClientPeer>,
    options?: Omit<SubscriptionOptions<ClientPeer>, 'peer'>
  ) {
    return this._runtime.subscribe(filter, options)
  }

  on(
    event: Extract<keyof Events, string>,
    handler: (
      data: Events[keyof Events],
      message: ReceivedMessage<ClientPeer>
    ) => void | Promise<void>,
    options?: Omit<SubscriptionOptions<ClientPeer>, 'peer'>
  ) {
    const subscription = this.subscribe((_, info) => info.event === event, options)
    void (async () => {
      try {
        for await (const message of subscription)
          await handler(message.data as Events[keyof Events], message)
      } catch (error) {
        this._report(error)
      } finally {
        subscription.cancel()
      }
    })()
    return () => subscription.cancel()
  }

  onStream(
    event: string,
    handler: (ctx: CoreReceiveContext<ClientPeer>) => void | Promise<void>
  ) {
    if (this._state === 'disposed') throw new Error('Client is disposed')
    if (this._streamHandlers.has(event))
      throw new Error(`Stream handler already registered: ${event}`)
    this._streamHandlers.set(event, handler)
    return () => {
      if (this._streamHandlers.get(event) === handler) this._streamHandlers.delete(event)
    }
  }

  use(plugin: Parameters<RuntimePipelines<ClientPeer>['use']>[0]) {
    this._runtime.pipelines.use(plugin)
    return this
  }

  useReceive(middleware: Parameters<RuntimePipelines<ClientPeer>['useReceive']>[0]) {
    this._runtime.pipelines.useReceive(middleware)
    return this
  }

  useSend(middleware: Parameters<RuntimePipelines<ClientPeer>['useSend']>[0]) {
    this._runtime.pipelines.useSend(middleware)
    return this
  }

  useReceiveStream(
    middleware: Parameters<RuntimePipelines<ClientPeer>['useReceiveStream']>[0]
  ) {
    this._runtime.pipelines.useReceiveStream(middleware)
    return this
  }

  useSendStream(
    middleware: Parameters<RuntimePipelines<ClientPeer>['useSendStream']>[0]
  ) {
    this._runtime.pipelines.useSendStream(middleware)
    return this
  }
}
