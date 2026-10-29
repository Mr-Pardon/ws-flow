import { StreamTask } from './stream-task.js'
import { type WebSocket as WS } from 'ws'
import { 
  WSProxy, 
  BaseProtocol,
  type RouteRule, 
  type Protocol as ProxyProtocol
} from 'ws-event-proxy'
import { 
  WSProtocol
} from './protocol.js'
import { 
  composeMiddleware, 
  composeStreamMiddleware 
} from './compose.js'
import type {
  AnyRecord,
  WSContext,
  WSHandler,
  WSSendOptions,
  WSSubscribeOptions,
  WSWatchOptions,
  WatchTaskRecord
} from '../types/ws.js'
import type {
  EventMatcher,
  EventRouteMiddlewareRecord,
  Middleware,
  SendMiddleware,
  StreamMiddleware,
  WSMessageContext,
  WSSendContext
} from '../types/middleware.js'

export enum WSState {
  IDLE = 'IDLE',
  CONNECTING = 'CONNECTING',
  OPEN = 'OPEN',
  RECONNECTING = 'RECONNECTING',
  CLOSED = 'CLOSED'
}

async function createWebSocket(url: string): Promise<WebSocket | WS> {
  if (typeof window === 'undefined') {
    const { default: WS } = await import('ws')
    return new WS(url)
  }

  return new WebSocket(url)
}

export class WSClient<
  Events extends AnyRecord = AnyRecord
> {
  private _ws: WebSocket | WS | null = null
  private _wsContext: WSContext<Events>
  private _wsProxy: WSProxy
  private _wsProtocal: WSProtocol<Events>
  private _proxyProtocol: ProxyProtocol

  private _middlewares: Middleware<any, Events>[] = []
  private _sendMiddlewares: SendMiddleware<any, Events>[] = []
  private _streamMiddlewares: StreamMiddleware<any, Events>[] = []
  private _eventRouteMiddlewares: EventRouteMiddlewareRecord<Events>[] = []

  private _eventMap = new WeakMap<RouteRule<any>, string>()

  private _state: WSState = WSState.IDLE
  private _manualClose: boolean = false

  private _url: string | null = null

  private _connected: Promise<void>
  private _connectResolve: (() => void) | null = null
  private _connectReject: ((e: Error) => void) | null = null

  private _watchTasks = new Set<WatchTaskRecord<Events>>()

  /**
   * Create a WS client instance.
   *
   * @param protocol - Optional custom protocol definition. If omitted, `WSProtocol` is used.
   * 
   * @example
   * ```ts
   * interface Events {
   *  getState: { state: string },
   *  message: { msg: string }
   * }
   * const wsc = new WSClient<Events>()
   * ```
   */
  constructor(protocol?: WSProtocol<Events>) {
    this._wsContext = this._createContext()
    this._connected = this._createConnectPromise()
    this._wsProtocal = protocol || WSProtocol
    this._proxyProtocol = this._buildProxyProtocal(this._wsProtocal)

    this._wsProxy = new WSProxy(this._proxyProtocol)
  }

  private _pickProxyProtocol(protocol?: WSProtocol<Events>) {
    const proxy: Partial<ProxyProtocol> = {}
    if (!protocol) return proxy

    const proxyKeys = Object.keys(BaseProtocol) as (keyof ProxyProtocol)[]

    proxyKeys.forEach(<K extends keyof ProxyProtocol>(key: K) => {
      const value = protocol[key]
      if (value !== undefined) {
        proxy[key] = value
      }
    })

    return proxy
  }

  private _buildProxyProtocal(protocol?: WSProtocol<Events>) {
    return {
      ...this._pickProxyProtocol(WSProtocol),
      ...(protocol?.proxy || {}),
      ...this._pickProxyProtocol(protocol),
    }
  }

  private _createConnectPromise() {
    return new Promise<void>((resolve, reject) => {
      this._connectResolve = resolve
      this._connectReject = reject
    })
  }

  private _createContext(): WSContext<Events> { 
    return {
      use: this.use.bind(this),
      useSend: this.useSend.bind(this),
      useStream: this.useStream.bind(this),
      useEvent: this.useEvent.bind(this),
      connect: this.connect.bind(this),
      close: this.close.bind(this),
      send: this.send.bind(this),
      request: this.request.bind(this),
      withEvent: this.withEvent.bind(this),
      on: this.on.bind(this),
      once: this.once.bind(this),
      subscribe: this.subscribe.bind(this),
      subscribeEvent: this.subscribeEvent.bind(this),
      subscribeAll: this.subscribeAll.bind(this),
      watch: this.watch.bind(this)
    }
  }

  private async _attemptConnect() { 
    if (this._manualClose || !this._url) return
    try {
      this._ws = await createWebSocket(this._url)

      const handleOpen = () => {
        this._state = WSState.OPEN
        this._wsProxy.bind(this._ws!)
        this._restoreWatchTasks()
        this._connectResolve?.()
        this._connectResolve = null
        this._connectReject = null
      }

      const handleReconnect = () => {
        if (this._manualClose) return
        this._ws = null
        this._scheduleReconnect()
      }

      this._ws.addEventListener('open', handleOpen, { once: true })
      this._ws.addEventListener('error', handleReconnect)
      this._ws.addEventListener('close', handleReconnect)
    } catch (e) {
      this._state = WSState.CLOSED
      this._connectReject?.(e as Error)
      this._connectResolve = null
      this._connectReject = null
    }
  }

  private _scheduleReconnect() {
    if (this._state === WSState.RECONNECTING) return
    if (!this._url) return

    this._state = WSState.RECONNECTING
    this._connected = this._createConnectPromise()

    setTimeout(() => {
      this._attemptConnect()
    }, this._wsProtocal.reconnectTimeout);
  }

  private _assertRPCProtocol<T>(payload: T) {
    const protocol = this._proxyProtocol

    if (!protocol.getRequestId?.(payload)) {
      throw new Error(
        '[WebsocketWithProxy] request() requires protocol.getRequestId and protocol.getResponseId'
      )
    }
  } 

  private async _sendWithMiddleware<Msg>(
    message: Msg,
    expectResponse: boolean = true,
    options?: WSSendOptions<Msg, Events>
  ) {
    const middleware = this._mergeSendMiddleware(options)
    const pipeline = composeMiddleware<Msg, Events>(middleware)

    let stopped = false
    const ctx: WSSendContext<Msg, Events> = {
      data: message,
      raw: message,
      proxy: this._wsProxy,
      ws: this._ws,
      state: this._state,
      ctx: this._wsContext,
      meta: { expectResponse },
      stop: () => {
        stopped = true
      }
    }

    await pipeline(ctx)
    if (stopped) return
    return this._wsProxy.send(ctx.data, { expectResponse })
  }

  private _mergeSendMiddleware<Msg>(
    options?: WSSendOptions<any, Events>
  ) {
    const local = options?.sendMiddleware || []

    if (options?.overrideSendMiddleware) {
      return local
    }

    return [...this._sendMiddlewares, ...local]
  }

  private _subscribeEventWithMiddleware<Receive>(
    rules: RouteRule<Receive>,
    handler: WSHandler<Receive, Events>,
    options?: WSSubscribeOptions<Receive, Events>
  ) { 
    const middleware = this._mergeEventMiddleware<Receive>(rules, options)
    const pipeline = composeMiddleware<Receive, Events>(middleware)

    return this._wsProxy.subscribeEvent(rules, async (data) => {  
      const ctx: WSMessageContext<Receive, Events> = {
        data,
        raw: data,
        event: rules,
        proxy: this._wsProxy,
        ws: this._ws,
        state: this._state,
        ctx: this._wsContext,
        meta: {}
      }
      let stopped = false
      ctx.stop = () => {
        stopped = true
      }
      await pipeline(ctx)
      if (!stopped) handler(ctx.data, this._wsContext)
    })
  }

  private _mergeEventMiddleware<Receive>(
    rule: RouteRule<Receive>,
    options?: WSSubscribeOptions<Receive, Events>
  ) { 
    const global = this._middlewares
    const local = options?.eventMiddleware || []
    const event = this._getEvent<Receive>(rule)

    const route = this._eventRouteMiddlewares
      .filter(r => this._matchEvent(event, r.matcher))
      .flatMap(r => r.middlewares)

    if (options?.overrideEventMiddleware) {
      return [...route, ...local]
    }

    return [...global, ...route, ...local]
  }

  private _matchEvent(
    event: string | undefined,
    matcher: EventMatcher
  ) {
    if (!event) return false

    if (typeof matcher === 'string') {
      return matcher === event
    }

    if (matcher instanceof RegExp) {
      return matcher.test(event)
    }

    if (matcher instanceof Function) {
      return matcher(event)
    }

    return false
  }

  private _getEvent<Receive>(rules: RouteRule<Receive>) {
    return this._eventMap.get(rules)
  }

  private _applyStreamMiddleware<Receive>(
    task: StreamTask<Receive, Events>,
    command: AnyRecord,
    eventOrRule: Extract<keyof Events, string> | RouteRule<Receive>,
    options?: WSWatchOptions<Receive, Events>
  ) { 
    const ctx = {
      task,
      command,
      eventOrRule,
      options,
      wsContext: this._wsContext
    }

    const middlewares = this._mergeStreamMiddleware<Receive>(options)
    const pipeline = composeStreamMiddleware<Receive, Events>(middlewares)
    return pipeline(task, ctx)
  }

  private _mergeStreamMiddleware<Receive>(
    options?: WSWatchOptions<Receive, Events>
  ) { 
    const local = options?.streamMiddleware || []

    if (options?.overrideStreamMiddleware) {
      return local
    }

    return [...this._streamMiddlewares, ...local]
  }

  private _resolveSubscribe<Receive>(
    eventOrRule: Extract<keyof Events, string> | RouteRule<Receive>,
    options?: WSSubscribeOptions<Receive, Events>
  ) {
    if (typeof eventOrRule === 'string') {
      return (handler: WSHandler<Events[typeof eventOrRule], Events>) =>
        this.on(eventOrRule, handler, options as WSSubscribeOptions<any, Events>)
    }

    return (handler: WSHandler<Receive, Events>) =>
      this.subscribe(eventOrRule as RouteRule<Receive>, handler, options)
  }

  private _createStreamTask<Receive>(
    subscribe: (handler: WSHandler<Receive, Events>) => () => void,
    options?: WSWatchOptions<Receive, Events>
  ) {
    return new StreamTask(subscribe, {
      maxQueueSize: options?.maxQueueSize,
      strategy: options?.strategy
    })
  }

  private _subsSignal<Reveive>(
    task: StreamTask<Reveive, Events>,
    options?: WSWatchOptions<Reveive, Events>
  ) {
    if (!options?.signal) return () => {}
    return this.subscribe(options.signal, () => {
      task.stop()
    })
  }

  private _registerWatchTask(
    record: WatchTaskRecord<Events>
  ) {
    this._watchTasks.add(record)

    record.task.onStop(() => {
      this._watchTasks.delete(record)
      record.unsubsSignal?.()
    })
  }

  private _restoreWatchTasks() { 
    this._watchTasks.forEach( watchTask => {
      const { command, task } = watchTask
      task.restart()
      this.send(command)
    })
  }

  /**
   * Create a client instance that "takes over" an existing WebSocket.
   *
   * This is useful when the WebSocket is created elsewhere (for example by
   * a different library or a framework-managed connection), but you still want
   * to use the WSProxy features and middleware pipeline.
   *
   * @param ws - Existing WebSocket instance to bind.
   * @param protocol - Optional custom protocol definition.
   * @returns A WSClient bound to the provided WebSocket.
   */
  static takeover<Events extends AnyRecord = AnyRecord>(
    ws: WebSocket | WS,
    protocol?: WSProtocol<Events>
  ) {
    const instance = new WSClient<Events>(protocol)
    instance._ws = ws
    instance._wsProxy.bind(ws)
    instance._state = WSState.OPEN
    instance._connected = Promise.resolve()
    instance._manualClose = false
    instance._url = null
    return instance
  }

  /**
   * Register global message middleware.
   *
   * Middleware runs for all event messages and can transform payloads, attach
   * metadata, or short-circuit handler execution via `ctx.stop()`.
   *
   * @param middlewares - A middleware or list of middlewares to append.
   *
   * @example
   * ```ts
   * client.use(async (ctx, next) => {
   *   ctx.meta = { ...ctx.meta, traceId: 'abc' }
   *   await next()
   * })
   * ```
   */
  use(
    middlewares: 
      | Middleware<any, Events> 
      | Middleware<any, Events>[]
  ) {
    middlewares = Array.isArray(middlewares) ? middlewares : [ middlewares ]
    this._middlewares.push(...middlewares)
  }

  /**
   * Register global send middleware.
   *
   * Send middleware runs before a message is sent, and can mutate the outgoing
   * payload or stop the send by calling `ctx.stop()`.
   *
   * @param middlewares - A send middleware or list of send middlewares to append.
   *
   * @example
   * ```ts
   * client.useSend(async (ctx, next) => {
   *   ctx.data = { ...ctx.data, token: 'xxx' }
   *   await next()
   * })
   * ```
   */
  useSend(
    middlewares:
      | SendMiddleware<any, Events>
      | SendMiddleware<any, Events>[]
  ) {
    middlewares = Array.isArray(middlewares) ? middlewares : [ middlewares ]
    this._sendMiddlewares.push(...middlewares)
  }

  /**
   * Register global stream middleware used by `watch`.
   *
   * Stream middleware can wrap or replace the `StreamTask` before it starts,
   * enabling behaviors like throttling, batching, or custom queue strategies.
   *
   * @param middlewares - A stream middleware or list of stream middlewares.
   *
   * @example
   * ```ts
   * client.useStream((task, ctx, next) => {
   *   // Replace the task with a custom wrapper if needed
   *   return next(task)
   * })
   * ```
   */
  useStream(
    middlewares:
      | StreamMiddleware<any, Events>
      | StreamMiddleware<any, Events>[]
  ) { 
    middlewares = Array.isArray(middlewares) ? middlewares : [ middlewares ]
    this._streamMiddlewares.push(...middlewares)
  }

  /**
   * Register event-route-specific middleware.
   *
   * The matcher can be a string, RegExp, or predicate function. When a routed
   * event matches, the provided middlewares are added to the pipeline.
   *
   * @param matcher - Event matcher (string | RegExp | predicate).
   * @param middlewares - Middleware(s) to apply for matched events.
   *
   * @example
   * ```ts
   * client.useEvent(/^chat:/, async (ctx, next) => {
   *   ctx.meta = { ...ctx.meta, room: 'public' }
   *   await next()
   * })
   * ```
   */
  useEvent(
    matcher: EventMatcher,
    middlewares:
      | Middleware<any, Events>
      | Middleware<any, Events>[],
) {
    middlewares = Array.isArray(middlewares) ? middlewares : [middlewares]
    this._eventRouteMiddlewares.push({ matcher, middlewares })
  }

  /**
   * Connect to a WebSocket server and initialize the proxy pipeline.
   *
   * Calling `connect` while already connecting/open will return the same
   * in-flight promise.
   *
   * @param url - WebSocket URL (ws:// or wss://).
   * @returns A promise that resolves once the connection is open.
   *
   * @example
   * ```ts
   * await client.connect('wss://example.com/ws')
   * ```
   */
  async connect(url: string) {
    if (this._state === WSState.OPEN) return this._connected
    if (this._state === WSState.CONNECTING) return this._connected

    this._url = url
    this._manualClose = false
    this._state = WSState.CONNECTING

    this._attemptConnect()
    return this._connected
  }

  /**
   * Close the current WebSocket connection and reset connection state.
   *
   * This will prevent automatic reconnect attempts until `connect` is called again.
   *
   * @example
   * ```ts
   * client.close()
   * ```
   */
  close() {
    this._manualClose = true
    this._ws?.close()
    this._ws = null
    
    this._state = WSState.CLOSED
    this._connected = this._createConnectPromise()
  }

  onMessage(handler: WSHandler<Events[keyof Events], Events>) { 
  }

  /**
   * Send a one-way message.
   *
   * Waits for the connection to be open before sending. No response is
   * expected.
   *
   * @param message - Message payload to send.
   * @param options - Optional per-send middleware configuration.
   *
   * @example
   * ```ts
   * await client.send({ type: 'ping' })
   * await client.send(
   *   { type: 'ping' },
   *   { sendMiddleware: [async (ctx, next) => { ctx.data.ts = Date.now(); await next() }] }
   * )
   * ```
   */
  async send<Msg = any>(
    message: Msg,
    options?: WSSendOptions<Msg, Events>
  ): Promise<void> {
    await this._connected
    await this._sendWithMiddleware(message, false, options)
  }

  /**
   * Send a request and await a response (RPC style).
   *
   * Requires protocol to provide `getRequestId` and `getResponseId` so the
   * proxy can correlate replies.
   *
   * @param message - Request payload to send.
   * @param options - Optional per-send middleware configuration.
   * @returns A promise resolving to the response payload.
   *
   * @example
   * ```ts
   * const res = await client.request<{ type: string }, { ok: boolean }>({ type: 'get' })
   * console.log(res) // { ok: true }
   * ```
   */
  async request<Msg = any, Receive = any>(
    message: Msg,
    options?: WSSendOptions<Msg, Events>
  ): Promise<Receive> {
    this._assertRPCProtocol<Msg>(message)
    await this._connected
    return this._sendWithMiddleware(message, true, options)
  }

  /**
   * Associate an event name with a RouteRule.
   *
   * This is typically used internally to map string event names to route rules
   * resolved by the protocol.
   *
   * @param rules - RouteRule to associate.
   * @param event - Event name to map.
   * @returns The original RouteRule.
   *
   * @example
   * ```ts
   * const rule = client.withEvent(
   *   { 
   *    event: 'chat:message',
   *    message: 'hello!'
   *   },
   *   'chat:message'
   * )
   * ```
   */
  withEvent<Receive = any>(
    rules: RouteRule<Receive>,
    event: string
  ): RouteRule<Receive> {
    this._eventMap.set(rules, event)
    return rules
  }

  /**
   * Subscribe to one or more events by name.
   *
   * Uses the protocol to resolve event names to route rules, then applies
   * middleware and registers the handler.
   *
   * @param event - Event name or list of event names.
   * @param handler - Handler invoked for each matched message.
   * @param options - Optional subscription options and middleware overrides.
   * @returns An unsubscribe function.
   *
   * @example
   * ```ts
   * // `on` uses protocol.resolveEventType under the hood.
   * // Make sure your protocol defines resolveEventType for the event names.
   * client.on('chat:message', (data) => {
   *   console.log(data.text)
   * }) // it'll match the event like { type: 'chat:message' }
   * ```
   */
  on<Type extends Extract<keyof Events, string>>(
    event: Type | Type[], 
    handler: WSHandler<Events[Type], Events>,
    options?: WSSubscribeOptions<Events[Type], Events>
  ) {
    if (!this._wsProtocal.resolveEventType) {
      throw new Error ('[WebsocketWithProxy] on() requires protocol.resolveEventType')
    }

    const events = Array.isArray(event) ? event : [event]

    if (events.length === 0) {
      throw new Error('[WebsocketWithProxy] on() requires at least one event')
    }

    const unsubscribers = events
      .map(e => this.withEvent(this._wsProtocal.resolveEventType(e), e))
      .map(rule => this._subscribeEventWithMiddleware(rule, handler, options))

    return () => {
      unsubscribers.forEach(unsubscribe => unsubscribe())
    }
  }

  /**
   * Subscribe to one or more events and auto-unsubscribe after first match.
   *
   * @param event - Event name or list of event names.
   * @param handler - Handler invoked for the first matched message.
   * @param options - Optional subscription options and middleware overrides.
   * @returns A promise resolving with the first received payload.
   *
   * @example
   * ```ts
   * const first = await client.once('chat:message', (data) => {
   *   console.log('first message', data)
   * })
   * console.log('resolved with', first)
   * ```
   */
  once<Type extends Extract<keyof Events, string>>(
    event: Type | Type[], 
    handler: WSHandler<Events[Type], Events>,
    options?: WSSubscribeOptions<Events[Type], Events>
  ) {
    return new Promise<Events[Type]>(resolve => {
      const off = this.on(event, (data, ctx) => {
        handler(data, ctx)
        off()
        resolve(data)
      }, options)
    })
  }

  /**
   * Subscribe using an explicit route rule.
   *
   * @param rules - Route rule that matches incoming messages and supports predicate matching.
   * @param handler - Handler invoked for each matched message.
   * @param options - Optional subscription options and middleware overrides.
   * @returns An unsubscribe function.
   *
   * @example
   * ```ts
   * const rule = { 
   *  type: 'chat:message',
   *  message: (value, e) => value === 'hello!' 
  *  }
   *
   * // If you want the rule to match useEvent middlewares,
   * // register the mapping via withEvent.
   * client.withEvent(rule, 'chat:message')
   *
   * client.subscribe(rule, (data) => {
   *   console.log(data.text)
   * })
   * ```
   */
  subscribe<Reveive = any>(
    rules: RouteRule<Reveive>, 
    handler: WSHandler<Reveive, Events>,
    options?: WSSubscribeOptions<Reveive, Events>
  ) {
    return this._subscribeEventWithMiddleware(rules, handler, options)
  }

  /**
   * Alias of `subscribe` for semantic clarity.
   *
   * @param rules - Route rule that matches incoming messages.
   * @param handler - Handler invoked for each matched message.
   * @param options - Optional subscription options and middleware overrides.
   * @returns An unsubscribe function.
   *
   * @example
   * ```ts
   * const rule = client.withEvent({ event: 'chat:message' }, 'chat:message')
   * client.subscribeEvent(rule, (data) => {
   *   console.log(data.text)
   * })
   * ```
   */
  subscribeEvent<Receive = any>(
    rules: RouteRule<Receive>,
    handler: WSHandler<Receive, Events>,
    options?: WSSubscribeOptions<Receive, Events>
  ) {
    return this.subscribe(rules, handler, options)
  }

  /**
   * Subscribe to all events. It will through the middlwares pipeline.
   *
   * @param handler - Handler invoked for each message.
   * @param options - Optional subscription options and middleware overrides.
   * @returns An unsubscribe function.
   *
   * @example
   * ```ts
   * client.subscribeAll((data) => {
   *   console.log(data)
   * })
   * ```
   */
  subscribeAll<Receive = any>(
    handler: WSHandler<Receive, Events>,
    options?: WSSubscribeOptions<Receive, Events>
  ) { 
    return this.subscribe({}, handler, options)
  }

  /**
   * TODO: command应调整为非必须
   * 
   * Send a command and stream the resulting event(s) through a StreamTask.
   *
   * This starts the stream, sends the command, and registers the task for
   * automatic restart on reconnect. Optionally supports an abort signal to stop
   * the task.
   *
   * @param command - Command payload to send.
   * @param eventOrRule - Event name or route rule to subscribe to.
   * @param options - Stream/watch options including queue strategy and middleware.
   * @returns A StreamTask that can be iterated or subscribed to.
   *
   * @example
   * ```ts
   * // Start a stream and read results with async iteration
   * const task = client.watch(
   *   { type: 'chat:history' },
   *   'chat:message',
   *   { maxQueueSize: 50, strategy: 'drop-head' }
   * )
   *
   * for await (const msg of task) {
   *   console.log(msg)
   * }
   * ```
   */
  watch<Receive = any>(
    command: AnyRecord, 
    eventOrRule: Extract<keyof Events, string> | RouteRule<Receive>, 
    options?: WSWatchOptions<Receive, Events>
  ) {
    const subscribe = this._resolveSubscribe<Receive>(eventOrRule, options)
    let task = this._createStreamTask<Receive>(subscribe, options)
    task = this._applyStreamMiddleware<Receive>(task, command, eventOrRule, options)

    const unsubsSignal = this._subsSignal(task, options)
    
    task.start()
    this.send(command)

    this._registerWatchTask({ command, task, unsubsSignal })
    return task
  }

  /**
   * Get the underlying WebSocket instance.
   *
   * @returns The current WebSocket instance or `null` if not connected.
   */
  getWsInstance() {
    return this._ws
  }

  /**
   * Get the WSProxy instance used by this client.
   *
   * @returns The WSProxy instance.
   */
  getWsProxyInstance() {
    return this._wsProxy
  }
}
