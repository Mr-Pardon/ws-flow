import type {
  WebSocket as WS
} from "ws"
import type {
  WSProxy,
  RouteRule
} from "ws-event-proxy"
import type {
  AnyRecord,
  WSContext,
  WSState,
  WSWatchOptions
} from "./ws.js"
import type {
  StreamTask
} from "../core/stream-task.js"

export type Middleware<Receive = any, Events extends AnyRecord = any> = (
  ctx: WSMessageContext<Receive, Events>,
  next: () => Promise<void>
) => Promise<void> | void

/**
 * Context passed to message middleware.
 */
export interface WSMessageContext<Receive = any, Events extends AnyRecord = AnyRecord> {
  /**
   * Mutable data payload for downstream middleware/handlers.
   */
  data: Receive
  /**
   * Original payload before any mutations.
   */
  raw: Receive
  /**
   * Route rule associated with the incoming event (if available).
   */
  event?: RouteRule<Receive>
  /**
   * WSProxy instance bound to the current socket.
   */
  proxy: WSProxy
  /**
   * Underlying WebSocket instance or null if not connected.
   */
  ws: WebSocket | WS | null
  /**
   * Current connection state.
   */
  state: WSState
  /**
   * WS client context for access to public APIs.
   */
  ctx: WSContext<Events>
  /**
   * Optional metadata bag shared across middleware.
   */
  meta?: Record<string, any>
  /**
   * Call to stop further processing and skip the handler.
   */
  stop?: () => void
}

export type SendMiddleware<Msg = any, Events extends AnyRecord = AnyRecord> = (
  ctx: WSSendContext<Msg, Events>,
  next: () => Promise<void>
) => Promise<void> | void

/**
 * Context passed to send middleware.
 */
export interface WSSendContext<Msg = any, Events extends AnyRecord = AnyRecord> {
  /**
   * Mutable outgoing payload for downstream middleware.
   */
  data: Msg,
  /**
   * Original outgoing payload before any mutations.
   */
  raw: Msg,
  /**
   * WSProxy instance used to send the message or subscribe event.
   */
  proxy: WSProxy,
  /**
   * Underlying WebSocket instance or null if not connected.
   */
  ws: WebSocket | WS | null
  /**
   * Current connection state.
   */
  state: WSState
  /**
   * WS client context for access to public APIs.
   */
  ctx: WSContext<Events>
  /**
   * Optional metadata bag shared across middleware.
   */
  meta?: Record<string, any>
  /**
   * Call to stop the send operation.
   */
  stop?: () => void
}

export type Pipeline<Receive = any, Events extends AnyRecord = AnyRecord>
  = (ctx: WSMessageContext<Receive, Events>) => Promise<void>

export type StreamMiddleware<Receive = any, Events extends AnyRecord = AnyRecord> = (
  task: StreamTask<Receive, Events>,
  ctx: StreamCtx<Receive, Events>,
  next: (task?: StreamTask<Receive, Events>) => StreamTask<Receive, Events>
) => StreamTask<Receive, Events>

/**
 * Context passed to stream middleware.
 */
export interface StreamCtx<Receive = any, Events extends AnyRecord = AnyRecord> {
  /**
   * Command payload sent to initiate the stream.
   */
  command: AnyRecord
  /**
   * Event name or route rule used to subscribe.
   */
  eventOrRule: Extract<keyof Events, string> | RouteRule<Receive>, 
  /**
   * Watch options for this stream.
   */
  options?: WSWatchOptions<Receive, Events>
  /**
   * WS client context for access to public APIs.
   */
  wsContext: WSContext<Events>
}

export type StreamPipeline<Receive = any, Events extends AnyRecord = AnyRecord> = (
  task: StreamTask<Receive, Events>,
  ctx: StreamCtx<Receive, Events>
) => StreamTask<Receive, Events>

/**
 * Matcher used to select event-specific middleware.
 */
export type EventMatcher = 
  | string
  | RegExp
  | ((event: string) => boolean)

/**
 * Mapping of matcher to its middleware list.
 */
export type EventRouteMiddlewareRecord<Events extends AnyRecord = AnyRecord> = {
  matcher: EventMatcher
  middlewares: Middleware<any, Events>[]
}
