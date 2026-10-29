import { WebSocketServer, WebSocket, type RawData } from 'ws'
import { createEnvelopeBuilder, EnvelopeBuilder } from '@ws-flow/envelope'
import {
  type CoreReceiveContext,
  type SendTransportLimits,
  ByteBudget,
  RuntimeCore,
  TransportLimitError,
  TaskCapacityError,
  TaskTimeoutError,
  TransportSendQueue,
  validateSendLimits,
  waitForWritable,
  wireByteLength
} from '@ws-flow/runtime-core'
import { WSSession } from './session.js'
import { RoomManager } from './roomManager.js'
import { Sender } from './sender.js'
import { WSSIssueHub } from './issues.js'
import {
  withSession,
  adaptPluginToCore,
  adaptReceiveMiddlewaresToCore,
  adaptReceiveEventRecordsToCore,
  adaptSendMiddlewaresToCore,
  adaptSendEventRecordsToCore,
  adaptStreamMiddlewaresToCore,
  adaptSendOptionsToCore,
  adaptSendStreamOptionsToCore
} from './runtimeAdapters.js'
import { resolveAsyncGenerator } from '../utils/index.js'
import type { WSSRequestHandler, WSSStreamHandler } from '../types/request.js'
import type {
  SubscriptionFilter,
  SubscriptionOptions,
  ReceivedMessage
} from '@ws-flow/runtime-core'
import {
  type WSScheduler,
  QueueCapacityError,
  SessionScheduler
} from '@ws-flow/scheduler'
import type {
  AnyRecord,
  WSEnvelope,
  WSSHandler,
  Plugin,
  WSReceiveContext,
  ReceiveMiddleware,
  SendMiddleware,
  StreamMiddleware,
  EventMatcher,
  AutoSendMode,
  ReceiveEventMiddlewareRecord,
  SendEventMiddlewareRecord,
  WSServerOptions,
  WSSCodec,
  WSSessionDeps,
  WSSIssueHandler,
  WSSIssueInput,
  WSSIssueType,
  WSSTypedIssueHandler,
  SendOptions,
  SendStreamOptions
} from '../types/index.js'

export class WSServer<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> {
  private _wss: WebSocketServer | null = null
  private _ownedWSS: WebSocketServer | null = null
  private _closed = false
  private _streamHandlers = new Map<string, WSSHandler<Events, Meta>>()
  private _connectionHandler: ((ws: WebSocket) => void) | null = null
  private _serverErrorHandler: ((error: Error) => void) | null = null

  private _runtime: RuntimeCore<WSSession<Events, Meta>>
  private _issues = new WSSIssueHub<Events, Meta>()
  private _resourceErrors = new WeakSet<
    QueueCapacityError | TransportLimitError | TaskCapacityError | TaskTimeoutError
  >()
  private _receiveBytes: ByteBudget<WSSession<Events, Meta>>
  private _sendBytes: ByteBudget<WSSession<Events, Meta>>
  private _sendLimits: SendTransportLimits
  private _sendQueue = new TransportSendQueue<WSSession<Events, Meta>>()
  private _codec: Required<WSSCodec>
  private _envelopeBuilder: EnvelopeBuilder<Events>

  private _sessions = new Set<WSSession<Events, Meta>>()
  private _bindings = new Map<
    WSSession<Events, Meta>,
    {
      message: (raw: RawData) => void
      close: () => void
      error: (error: Error) => void
    }
  >()

  private _roomManager = new RoomManager<Events, Meta>()

  private _onConnect: ((session: WSSession<Events, Meta>) => void)[] = []
  private _onDisconnect: ((session: WSSession<Events, Meta>) => void)[] = []

  constructor(options: WSServerOptions<Events> = {}) {
    this._receiveBytes = new ByteBudget(options.transportLimits?.receive)
    this._sendLimits = validateSendLimits(options.transportLimits?.send)
    this._sendBytes = new ByteBudget(this._sendLimits)
    this._codec = this._createCodec(options.message)
    this._envelopeBuilder = createEnvelopeBuilder<Events>(options.message?.builder)
    this._runtime = this._createRuntime(options)
    this.useReceiveEvent({
      matcher: () => true,
      middlewares: async (ctx, next) => {
        const handler = this._streamHandlers.get(ctx.envelope.event ?? '')
        if (ctx.stream && handler) {
          await handler(ctx)
          return
        }
        await next()
      }
    })
  }

  private _createCodec(input?: WSSCodec): Required<WSSCodec> {
    return {
      encode: input?.encode || ((envelope: WSEnvelope) => JSON.stringify(envelope)),
      decode:
        input?.decode ||
        ((raw: RawData) => {
          const text =
            typeof raw === 'string'
              ? raw
              : Array.isArray(raw)
                ? Buffer.concat(raw).toString('utf8')
                : raw instanceof ArrayBuffer
                  ? Buffer.from(raw).toString('utf8')
                  : Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength).toString(
                      'utf8'
                    )

          return JSON.parse(text)
        })
    }
  }

  private _createRuntime(options: WSServerOptions<Events>) {
    if (options.schedulers?.in && options.queueLimits?.receive !== undefined)
      throw new TypeError('queueLimits.receive cannot be combined with schedulers.in')
    if (options.schedulers?.out && options.queueLimits?.send !== undefined)
      throw new TypeError('queueLimits.send cannot be combined with schedulers.out')
    const base = {
      builder: {
        envelope: this._envelopeBuilder
      },
      transport: this._createTransport()
    }
    const schedulers = {
      in: options.schedulers?.in || new SessionScheduler(options.queueLimits?.receive),
      out: options.schedulers?.out || new SessionScheduler(options.queueLimits?.send)
    }

    const runtime = new RuntimeCore<WSSession<Events, Meta>>({
      base,
      schedulers,
      taskLimits: options.taskLimits,
      taskTimeouts: options.taskTimeouts
    })

    runtime.useTaskErrorObserver(info => {
      this._resourceErrors.add(info.error)
      void this._emitIssue({
        type: 'resource_error',
        message: '[WSServer] task resource limit exceeded',
        session: info.peer,
        direction: info.kind.startsWith('incoming') ? 'receive' : 'send',
        taskKind: info.kind,
        error: info.error
      })
    })

    runtime.useQueueErrorObserver(info => {
      this._resourceErrors.add(info.error)
      if (info.fatal && this._sessions.has(info.peer)) {
        info.peer.close()
        this.detach(info.peer)
      }
      void this._emitIssue({
        type: 'resource_error',
        message: '[WSServer] message queue capacity exceeded',
        session: info.peer,
        direction: info.direction,
        error: info.error
      })
    })

    runtime.useTransportErrorObserver(info => {
      this._resourceErrors.add(info.error)
      if (info.fatal && this._sessions.has(info.peer)) {
        info.peer.close()
        this.detach(info.peer)
      }
      void this._emitIssue({
        type: 'resource_error',
        message: '[WSServer] transport resource limit exceeded',
        session: info.peer,
        direction: info.direction,
        error: info.error
      })
    })

    runtime.useInvalidStreamObserver(info =>
      this._emitIssue({
        type: 'invalid_stream',
        message: info.message,
        session: info.peer,
        envelope: info.envelope,
        reason: info.reason
      })
    )
    runtime.useStreamErrorObserver((error, ctx) => {
      if (this._isReportedResourceError(error)) return
      return this._emitIssue({
        type: 'process_error',
        message: '[WSServer] stream handler failed',
        session: ctx.peer,
        envelope: ctx.envelope,
        error
      })
    })
    runtime.useRequestErrorObserver((error, session) => {
      if (this._isReportedResourceError(error)) return
      return this._emitIssue({
        type: 'process_error',
        message: '[WSServer] request response failed',
        session,
        error
      })
    })

    return runtime
  }

  private _createTransport() {
    const prepare = async (
      session: WSSession<Events, Meta>,
      envelope: WSEnvelope,
      signal?: AbortSignal
    ) => {
      let raw: any

      try {
        raw = this._codec.encode(envelope)
      } catch (error) {
        await this._emitIssue({
          type: 'transport_error',
          message: '[WSServer] encode failed',
          session,
          envelope,
          error
        })
        throw error
      }
      signal?.throwIfAborted()

      let release = () => {}
      try {
        const bytes =
          this._sendBytes.enabled || this._sendLimits.highWaterMark !== undefined
            ? wireByteLength(raw)
            : 0
        release = this._sendBytes.reserve(session, bytes)
        const factory = async () => {
          if (this._sendLimits.highWaterMark !== undefined)
            await waitForWritable(session.ws, bytes, this._sendLimits, signal)
          return {
            release,
            send: async () => {
              signal?.throwIfAborted()
              if (session.state !== 'open' || session.ws.readyState !== WebSocket.OPEN)
                throw new Error('Session is closed')
              try {
                await new Promise<void>((resolve, reject) => {
                  session.ws.send(raw, err => (err ? reject(err) : resolve()))
                })
              } catch (error) {
                await this._emitIssue({
                  type: 'transport_error',
                  message: '[WSServer] transport send failed',
                  session,
                  envelope,
                  error
                })
                throw error
              }
            }
          }
        }
        return this._sendLimits.highWaterMark !== undefined
          ? await this._sendQueue.prepare(session, signal, factory)
          : await factory()
      } catch (error) {
        release()
        throw error
      }
    }
    return {
      prepare,
      send: async (session: WSSession<Events, Meta>, envelope: WSEnvelope) => {
        const prepared = await prepare(session, envelope)
        try {
          await prepared.send()
        } finally {
          prepared.release()
        }
      }
    }
  }

  private _isReportedResourceError(error: unknown) {
    return (
      (error instanceof QueueCapacityError ||
        error instanceof TransportLimitError ||
        error instanceof TaskCapacityError ||
        error instanceof TaskTimeoutError) &&
      this._resourceErrors.has(error)
    )
  }

  private _bindWSS(wss: WebSocketServer) {
    this._wss = wss
    this._connectionHandler = (ws: WebSocket) => {
      if (this._closed || ws.readyState !== WebSocket.OPEN) return
      this.attach(ws)
    }
    this._serverErrorHandler = error => {
      void this._emitIssue({
        type: 'server_error',
        message: '[WSServer] WebSocket server error',
        error
      })
    }
    wss.on('connection', this._connectionHandler)
    wss.on('error', this._serverErrorHandler)
  }

  private _createMessageHandler(session: WSSession<Events, Meta>) {
    const messageHandler = (raw: RawData) => {
      if (session.state !== 'open' || !this._sessions.has(session)) return
      let release = () => {}
      try {
        if (this._receiveBytes.enabled)
          release = this._receiveBytes.reserve(session, wireByteLength(raw))
      } catch (error) {
        if (error instanceof TransportLimitError) {
          this._resourceErrors.add(error)
          this._runtime.closePeer(session, error)
          session.close()
          this.detach(session)
          void this._emitIssue({
            type: 'resource_error',
            message: '[WSServer] inbound transport resource limit exceeded',
            session,
            direction: 'receive',
            error
          })
          return
        }
        throw error
      }
      let data: any

      try {
        data = this._codec.decode(raw)
      } catch (error) {
        release()
        void this._emitIssue({
          type: 'decode_error',
          message: '[WSServer] decode failed',
          session,
          raw,
          error
        })
        return
      }

      void this._runtime.dispatchers.receive
        .dispatch(session, data)
        .catch(error => {
          if (this._isReportedResourceError(error) || session.state !== 'open') return
          void this._emitIssue({
            type: 'process_error',
            message: '[WSServer] receive dispatch failed',
            session,
            error
          })
        })
        .finally(release)
    }

    return messageHandler
  }

  private _bindMessage(session: WSSession<Events, Meta>) {
    const messageHandler = this._createMessageHandler(session)
    session.ws.on('message', messageHandler)
    return messageHandler
  }

  private _createSessionDeps(): WSSessionDeps<Events, Meta> {
    return {
      subscribe: (peer, filter, options) =>
        this._runtime.subscribe(filter, { ...options, peer }),
      report: (error, session) => {
        if (this._isReportedResourceError(error)) return
        void this._emitIssue({
          type: 'process_error',
          message: '[WSServer] subscription callback failed',
          session,
          error
        })
      },
      request: (peer, event, input, options) =>
        this._runtime.request(peer, event, input, {
          ...adaptSendOptionsToCore(options),
          timeoutMs: options?.timeoutMs
        }),
      handleRequest: <Input, Result>(
        peer: WSSession<Events, Meta>,
        event: string,
        handler: WSSRequestHandler<Events, Meta, Input, Result>
      ) =>
        this._runtime.handleRequest<Input, Result>(
          event,
          ctx => handler({ ...ctx, session: ctx.peer }),
          peer
        ),
      requestStream: (peer, event, input, options) =>
        this._runtime.requestStream(peer, event, input, {
          ...adaptSendOptionsToCore(options),
          timeoutMs: options?.timeoutMs,
          capacity: options?.capacity,
          idleTimeoutMs: options?.idleTimeoutMs
        }),
      handleStream: <Input, Chunk>(
        peer: WSSession<Events, Meta>,
        event: string,
        handler: WSSStreamHandler<Events, Meta, Input, Chunk>
      ) =>
        this._runtime.handleStream<Input, Chunk>(
          event,
          ctx => handler({ ...ctx, session: ctx.peer }),
          peer
        ),
      envelopeBuilder: this._envelopeBuilder,
      sendDispatcher: {
        dispatch: (
          peer: WSSession<Events, Meta>,
          env: WSEnvelope,
          options?: SendOptions<Events, Meta>
        ) =>
          this._runtime.dispatchers.send.dispatch(
            peer,
            env,
            adaptSendOptionsToCore(options)
          )
      },
      roomManager: this._roomManager,
      createSendStream: (
        peer: WSSession<Events, Meta>,
        event?: string,
        options?: SendStreamOptions<Events, Meta>
      ) =>
        this._runtime.createSendStream(peer, event, adaptSendStreamOptionsToCore(options))
    }
  }

  private _createHandlerMiddleware(
    handler: WSSHandler<Events, Meta>,
    autoSend: AutoSendMode = 'smart'
  ) {
    return async (ctx: CoreReceiveContext<WSSession<Events, Meta>>) => {
      const serverCtx = withSession<
        Events,
        Meta,
        CoreReceiveContext<WSSession<Events, Meta>>
      >(ctx)

      try {
        const result = await handler(serverCtx)
        await this._handleAutoSend(serverCtx, result, autoSend)
      } catch (error) {
        try {
          serverCtx.stream?.stop('error', error)
        } catch {
          /* preserve handler error */
        }
        if (this._isReportedResourceError(error)) return
        await this._emitIssue({
          type: 'process_error',
          message: '[WSServer] handler execution failed',
          session: serverCtx.session,
          envelope: serverCtx.envelope,
          error
        })
      }
    }
  }

  private _emitIssue(issue: WSSIssueInput<Events, Meta>) {
    return this._issues.emit({
      ...issue,
      timestamp: Date.now()
    })
  }

  private _isEnvelopeLike(value: any): value is WSEnvelope<Events> {
    if (!value || typeof value !== 'object') return false

    return (
      'event' in value ||
      'data' in value ||
      'streamId' in value ||
      'streamState' in value ||
      'meta' in value ||
      'priority' in value ||
      'error' in value
    )
  }

  private _resolveStreamFactory(result: any) {
    const generator = resolveAsyncGenerator(result)
    if (!generator) return null

    return () => generator
  }

  private _sendStreamFromResult(ctx: WSReceiveContext<Events, Meta>, result: any) {
    const factory = this._resolveStreamFactory(result)
    if (!factory) {
      throw new Error(
        '[WSServer] autoSend="stream" requires an AsyncGenerator or generator factory'
      )
    }

    return ctx.envelope.event
      ? ctx.session.stream(ctx.envelope.event, factory)
      : ctx.session.stream(factory)
  }

  private async _handleAutoSend(
    ctx: WSReceiveContext<Events, Meta>,
    result: any,
    mode: AutoSendMode
  ) {
    if (mode === false || result === undefined) return

    const smart = () => {
      if (this._resolveStreamFactory(result)) {
        return this._sendStreamFromResult(ctx, result)
      }

      if (this._isEnvelopeLike(result)) {
        return ctx.session.sendEnvelope(result)
      }

      return ctx.session.send(result)
    }

    const actions: Record<Exclude<AutoSendMode, false>, () => any> = {
      data: () => ctx.session.send(result),
      envelope: () => ctx.session.sendEnvelope(result),
      stream: () => this._sendStreamFromResult(ctx, result),
      smart
    }

    return actions[mode]()
  }

  static create<Events extends AnyRecord = AnyRecord, Meta extends AnyRecord = AnyRecord>(
    options: ConstructorParameters<typeof WebSocketServer>[0],
    serverOptions?: WSServerOptions<Events>
  ) {
    const server = new WSServer<Events, Meta>(serverOptions)
    const wss = new WebSocketServer(options)
    server._ownedWSS = wss
    server._bindWSS(wss)
    return server
  }

  static takeover<
    Events extends AnyRecord = AnyRecord,
    Meta extends AnyRecord = AnyRecord
  >(wss: WebSocketServer, serverOptions?: WSServerOptions<Events>) {
    const server = new WSServer<Events, Meta>(serverOptions)
    server._ownedWSS = null
    server._bindWSS(wss)
    return server
  }

  setScheduler(s: WSScheduler) {
    this._runtime.schedulers.setScheduler(s)
  }

  setInScheduler(s: WSScheduler) {
    this._runtime.schedulers.setInScheduler(s)
  }

  setOutScheduler(s: WSScheduler) {
    this._runtime.schedulers.setOutScheduler(s)
  }

  use(plugins: Plugin<Events, Meta> | Plugin<Events, Meta>[]): WSServer<Events, Meta> {
    const adapted = Array.isArray(plugins)
      ? plugins.map(plugin => adaptPluginToCore(plugin))
      : adaptPluginToCore(plugins)

    this._runtime.pipelines.use(adapted)
    return this
  }

  useReceive(
    middlewares: ReceiveMiddleware<Events, Meta> | ReceiveMiddleware<Events, Meta>[]
  ): WSServer<Events, Meta> {
    this._runtime.pipelines.useReceive(adaptReceiveMiddlewaresToCore(middlewares))
    return this
  }

  useReceiveEvent(
    records:
      | ReceiveEventMiddlewareRecord<Events, Meta>
      | ReceiveEventMiddlewareRecord<Events, Meta>[]
  ): WSServer<Events, Meta> {
    this._runtime.pipelines.useReceiveEvent(adaptReceiveEventRecordsToCore(records))
    return this
  }

  useReceiveStream(
    middlewares: StreamMiddleware<Events, Meta> | StreamMiddleware<Events, Meta>[]
  ): WSServer<Events, Meta> {
    this._runtime.pipelines.useReceiveStream(adaptStreamMiddlewaresToCore(middlewares))
    return this
  }

  useSend(
    middlewares: SendMiddleware<Events, Meta> | SendMiddleware<Events, Meta>[]
  ): WSServer<Events, Meta> {
    this._runtime.pipelines.useSend(adaptSendMiddlewaresToCore(middlewares))
    return this
  }

  useSendEvent(
    records:
      | SendEventMiddlewareRecord<Events, Meta>
      | SendEventMiddlewareRecord<Events, Meta>[]
  ): WSServer<Events, Meta> {
    this._runtime.pipelines.useSendEvent(adaptSendEventRecordsToCore(records))
    return this
  }

  useSendStream(
    middlewares: StreamMiddleware<Events, Meta> | StreamMiddleware<Events, Meta>[]
  ): WSServer<Events, Meta> {
    this._runtime.pipelines.useSendStream(adaptStreamMiddlewaresToCore(middlewares))
    return this
  }

  attach(ws: WebSocket) {
    if (this._closed) throw new Error('Server is closed')
    if (ws.readyState !== WebSocket.OPEN)
      throw new Error('Attach requires an open WebSocket')
    for (const session of this._sessions) {
      if (session.ws === ws) return session
    }
    const session = new WSSession<Events, Meta>(ws, this._createSessionDeps())

    this._sessions.add(session)

    const messageHandler = this._bindMessage(session)
    const closeHandler = () => {
      this.detach(session)
    }
    ws.on('close', closeHandler)
    const errorHandler = (error: Error) => {
      void this._emitIssue({
        type: 'process_error',
        message: '[WSServer] socket error',
        session,
        error
      })
    }
    ws.on('error', errorHandler)

    this._bindings.set(session, {
      message: messageHandler,
      close: closeHandler,
      error: errorHandler
    })
    this._notifySession(this._onConnect, session)

    return session
  }

  detach(session: WSSession<Events, Meta>) {
    if (!this._sessions.has(session)) return
    const { message, close, error } = this._bindings.get(session)!

    session.ws.off('message', message)
    session.ws.off('close', close)
    session.ws.off('error', error)
    this._bindings.delete(session)

    this._sessions.delete(session)
    session._markClosed()
    session.leaveAll()
    this._runtime.closePeer(session)
    this._receiveBytes.closePeer(session)
    this._sendBytes.closePeer(session)
    this._notifySession(this._onDisconnect, session)
  }

  private _notifySession(
    handlers: ((session: WSSession<Events, Meta>) => void)[],
    session: WSSession<Events, Meta>
  ) {
    const report = (error: unknown) =>
      this._emitIssue({
        type: 'process_error',
        message: '[WSServer] connection callback failed',
        session,
        error
      })
    for (const handler of [...handlers]) {
      try {
        void Promise.resolve(handler(session)).catch(report)
      } catch (error) {
        void report(error)
      }
    }
  }

  route(matcher: EventMatcher, handler: WSSHandler<Events, Meta>) {
    this.useReceiveEvent({
      matcher,
      middlewares: this._createHandlerMiddleware(handler, false)
    })
  }

  handleRequest<Input = unknown, Result = unknown>(
    event: string,
    handler: WSSRequestHandler<Events, Meta, Input, Result>
  ) {
    return this._runtime.handleRequest<Input, Result>(event, ctx =>
      handler({ ...ctx, session: ctx.peer })
    )
  }

  handleStream<Input = unknown, Chunk = unknown>(
    event: string,
    handler: WSSStreamHandler<Events, Meta, Input, Chunk>
  ) {
    return this._runtime.handleStream<Input, Chunk>(event, ctx =>
      handler({ ...ctx, session: ctx.peer })
    )
  }

  subscribe(
    filter: SubscriptionFilter<WSSession<Events, Meta>>,
    options?: SubscriptionOptions<WSSession<Events, Meta>>
  ) {
    return this._runtime.subscribe(filter, options)
  }

  on(
    event: string,
    handler: (
      data: unknown,
      message: ReceivedMessage<WSSession<Events, Meta>>
    ) => void | Promise<void>
  ) {
    const subscription = this.subscribe((_, message) => message.event === event)
    void (async () => {
      try {
        for await (const message of subscription) await handler(message.data, message)
      } catch (error) {
        if (this._isReportedResourceError(error)) return
        await this._emitIssue({
          type: 'subscription_error',
          message: '[WSServer] subscription callback failed',
          error
        })
      } finally {
        subscription.cancel()
      }
    })()
    return () => subscription.cancel()
  }

  onStream(event: string, handler: WSSHandler<Events, Meta>) {
    if (this._closed) throw new Error('Server is closed')
    if (this._streamHandlers.has(event))
      throw new Error(`Stream handler already registered: ${event}`)
    this._streamHandlers.set(event, handler)
    return () => {
      if (this._streamHandlers.get(event) === handler) this._streamHandlers.delete(event)
    }
  }

  handle(
    matcher: EventMatcher,
    handler: WSSHandler<Events, Meta>,
    autoSend: AutoSendMode = 'smart'
  ) {
    this.useReceiveEvent({
      matcher,
      middlewares: this._createHandlerMiddleware(handler, autoSend)
    })
  }

  join(session: WSSession<Events, Meta>, room: string) {
    session.join(room)
  }

  leave(session: WSSession<Events, Meta>, room: string) {
    session.leave(room)
  }

  leaveAll(session: WSSession<Events, Meta>) {
    session.leaveAll()
  }

  to(target: string | ((session: WSSession<Events, Meta>) => boolean)) {
    return new Sender<Events, Meta>(this._sessions).to(target)
  }

  all() {
    return new Sender<Events, Meta>(this._sessions).all()
  }

  onConnect(fn: (session: WSSession<Events, Meta>) => void) {
    this._onConnect.push(fn)
  }

  onDisconnect(fn: (session: WSSession<Events, Meta>) => void) {
    this._onDisconnect.push(fn)
  }

  onIssue(fn: WSSIssueHandler<Events, Meta>): WSServer<Events, Meta>

  onIssue<Type extends WSSIssueType>(
    type: Type,
    fn: WSSTypedIssueHandler<Type, Events, Meta>
  ): WSServer<Events, Meta>

  onIssue(
    typeOrHandler: WSSIssueType | WSSIssueHandler<Events, Meta>,
    fn?: WSSIssueHandler<Events, Meta>
  ) {
    typeof typeOrHandler === 'function'
      ? this._issues.on(typeOrHandler)
      : this._issues.on(typeOrHandler, fn!)
    return this
  }

  close() {
    if (this._closed) return
    this._closed = true
    if (this._wss && this._connectionHandler)
      this._wss.off('connection', this._connectionHandler)
    if (this._wss && this._serverErrorHandler)
      this._wss.off('error', this._serverErrorHandler)
    this._connectionHandler = null
    this._serverErrorHandler = null
    for (const session of [...this._sessions]) {
      session.close()
      this.detach(session)
    }
    this._runtime.dispose()
    this._receiveBytes.clear()
    this._sendBytes.clear()
    this._streamHandlers.clear()
    this._onConnect = []
    this._onDisconnect = []
    this._issues.clear()
    this._wss = null

    if (this._ownedWSS) {
      const owned = this._ownedWSS
      const ignoreError = () => {}
      const rejectConnection = (ws: WebSocket) => ws.terminate()
      owned.on('error', ignoreError)
      owned.on('connection', rejectConnection)
      // Let queued listen errors settle before ws removes its HTTP error listener.
      setImmediate(() => {
        owned.close()
        owned.off('error', ignoreError)
        owned.off('connection', rejectConnection)
      })
      this._ownedWSS = null
    }
  }

  get schedulers() {
    return {
      inScheduler: this._runtime.schedulers.inScheduler,
      outScheduler: this._runtime.schedulers.outScheduler
    }
  }

  get sessions(): ReadonlySet<WSSession<Events, Meta>> {
    return this._sessions
  }

  get rooms(): ReadonlyMap<string, Set<WSSession<Events, Meta>>> {
    return this._roomManager.rooms
  }
}
