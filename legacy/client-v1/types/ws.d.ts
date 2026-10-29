import type {
  RouteRule
} from 'ws-event-proxy'
import type {
  WSState
} from '../core/ws.js'
import type {
  StreamTask
} from '../core/stream-task.js'
import type {
  EventMatcher,
  Middleware,
  SendMiddleware,
  StreamMiddleware
} from './middleware.js'

export type AnyRecord = Record<string, any>
export type { WSState }

/**
 * Public context passed to handlers and middleware.
 */
export interface WSContext<Events extends AnyRecord = AnyRecord> {
  /**
   * Register global message middleware.
   */
  use(middleware:
    Middleware<any, Events>
    | Middleware<any, Events>[]
  ): void
  /**
   * Register global send middleware.
   */
  useSend(middleware:
    SendMiddleware<any, Events>
    | SendMiddleware<any, Events>[]
  ): void
  /**
   * Register global stream middleware used by `watch`.
   */
  useStream(middleware:
    StreamMiddleware<any, Events>
    | StreamMiddleware<any, Events>[]
  ): void
  /**
   * Register event-route-specific middleware.
   */
  useEvent(
    matcher: EventMatcher,
    middleware: Middleware<any, Events>
      | Middleware<any, Events>[]
  ): void

  /**
   * Connect to a WebSocket server.
   */
  connect(url: string): Promise<void>
  /**
   * Close the current WebSocket connection.
   */
  close(): void
  /**
   * Send a one-way message.
   */
  send<Msg = any>(message: Msg, options?: WSSendOptions<Msg, Events>): Promise<void>
  /**
   * Send a request and await a response (RPC style).
   */
  request<Msg = any, Receive = any>(message: Msg, options?: WSSendOptions<Msg, Events>): Promise<Receive>
  /**
   * Associate an event name with a RouteRule.
   */
  withEvent<Receive = any>(
    rules: RouteRule<Receive>,
    event: string
  ): RouteRule<Receive>
  /**
   * Subscribe to one or more events by name.
   */
  on<Type extends Extract<keyof Events, string>>(
    event: Type | Type[],
    handler: WSHandler<Events[Type], Events>,
    options?: WSSubscribeOptions<Events[Type], Events>
  ): () => void
  /**
   * Subscribe to one or more events and auto-unsubscribe after first match.
   */
  once<Type extends Extract<keyof Events, string>>(
    event: Type | Type[],
    handler: WSHandler<Events[Type], Events>,
    options?: WSSubscribeOptions<Events[Type], Events>
  ): Promise<Events[Type]>
  /**
   * Subscribe using an explicit route rule.
   */
  subscribe<Receive = any>(
    rules: RouteRule<Receive>,
    handler: WSHandler<Receive, Events>,
    options?: WSSubscribeOptions<Receive, Events>
  ): () => void
  /**
   * Alias of `subscribe` for semantic clarity.
   */
  subscribeEvent<Receive = any>(
    rules: RouteRule<Receive>,
    handler: WSHandler<Receive, Events>,
    options?: WSSubscribeOptions<Receive, Events>
  ): () => void
  /**
   * Subscribe to all events.
   */
  subscribeAll<Receive = any>(
    handler: WSHandler<Receive, Events>,
    options?: WSSubscribeOptions<Receive, Events>
  ): () => void
  /**
   * Send a command and stream the resulting event(s).
   */
  watch<Receive = any>(
    command: AnyRecord,
    eventOrRule: Extract<keyof Events, string> | RouteRule<Receive>,
    options?: WSWatchOptions<Receive, Events>
  ): StreamTask<Receive, Events>
}

export type WSHandler<Receive, Events extends AnyRecord = AnyRecord>
  = (data: Receive, ctx: WSContext<Events>) => void

/**
 * Options for per-send middleware.
 */
export interface WSSendOptions<Msg = any, Events extends AnyRecord = AnyRecord> {
  /**
   * Additional middleware to run for this send.
   */
  sendMiddleware?: SendMiddleware<Msg, Events>[]
  /**
   * If true, only per-send middleware is used.
   */
  overrideSendMiddleware?: boolean
}

/**
 * Options for event subscriptions.
 */
export interface WSSubscribeOptions<Receive = any, Events extends AnyRecord = AnyRecord> { 
  /**
   * Additional middleware to run for this subscription.
   */
  eventMiddleware?: Middleware<Receive, Events>[]
  /**
   * If true, only per-subscribe middleware is used.
   */
  overrideEventMiddleware?: boolean
}

/**
 * Options for watch/stream subscriptions.
 */
export interface WSWatchOptions<Receive = any, Events extends AnyRecord = AnyRecord> extends WSSubscribeOptions<Receive, Events> {
  /**
   * A RouteRule used as a stop signal; receiving it stops the task.
   */
  signal?: RouteRule<Receive>
  /**
   * Max queue size for buffered items.
   */
  maxQueueSize?: number
  /**
   * Buffering strategy for incoming items.
   */
  strategy?: QueueStrategyFn<Receive> | 'unbounded' | 'drop-head' | 'drop-tail'
  /**
   * Stream middleware applied to this watch.
   */
  streamMiddleware?: StreamMiddleware<Receive, Events>[]
  /**
   * If true, only per-watch stream middleware is used.
   */
  overrideStreamMiddleware?: boolean
}

/**
 * Internal record used to restore watch tasks on reconnect.
 */
export interface WatchTaskRecord<Events extends AnyRecord = AnyRecord> {
  command: AnyRecord
  task: StreamTask<any, Events>
  unsubsSignal?: () => void
}

/**
 * Queue strategy function signature.
 */
export type QueueStrategyFn<Receive> = (params: { 
  queue: Receive[]
  incoming: Receive
  maxSize?: number 
}) => Receive[]
