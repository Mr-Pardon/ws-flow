import type { RuntimeMessage } from '../protocol.js'
import type { Middleware, EventMiddlewareRecord } from '@ws-flow/pipeline'
import type { PeerLike, ReceiveStreamLike } from './index.js'

export interface BaseContext {
  stop(): void
  /** Dispatcher cancellation; middleware should cooperate with shutdown. */
  readonly signal?: AbortSignal
}

export interface CoreReceiveContext<Peer extends PeerLike = PeerLike>
  extends BaseContext {
  /** Internal message view; retained name for existing server middleware. */
  envelope: RuntimeMessage
  raw: unknown
  stream?: ReceiveStreamLike
  peer: Peer
}

export interface CoreSendContext<Peer extends PeerLike = PeerLike> extends BaseContext {
  envelope: RuntimeMessage
  raw: unknown
  peer: Peer
}

export interface CoreStreamContext<Peer extends PeerLike = PeerLike, Data = any>
  extends BaseContext {
  peer: Peer
  data: Data
  state: 'start' | 'chunk' | 'end' | 'error'
  error?: unknown

  end(): void
  fail(reason?: unknown): void
  reject(reason?: unknown): void
}

export type CoreReceiveMiddleware<Peer extends PeerLike = PeerLike> = Middleware<
  CoreReceiveContext<Peer>
>

export type CoreReceiveEventMiddlewareRecord<Peer extends PeerLike = PeerLike> =
  EventMiddlewareRecord<CoreReceiveContext<Peer>>

export type CoreSendMiddleware<Peer extends PeerLike = PeerLike> = Middleware<
  CoreSendContext<Peer>
>

export type CoreSendEventMiddlewareRecord<Peer extends PeerLike = PeerLike> =
  EventMiddlewareRecord<CoreSendContext<Peer>>

export type CoreStreamMiddleware<
  Peer extends PeerLike = PeerLike,
  Data = any
> = Middleware<CoreStreamContext<Peer, Data>>
