import type { RequestContext } from '@ws-flow/runtime-core'
import type { WSSession } from '../core/session.js'
import type { AnyRecord, SendOptions } from './wss.js'

export type WSSRequestOptions<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = SendOptions<Events, Meta> & { timeoutMs?: number | null }

export type WSSRequestContext<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord,
  Input = unknown
> = RequestContext<WSSession<Events, Meta>, Input> & {
  readonly session: WSSession<Events, Meta>
}

export type WSSRequestHandler<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord,
  Input = unknown,
  Result = unknown
> = (ctx: WSSRequestContext<Events, Meta, Input>) => Result | Promise<Result>

export type WSSRequestStreamOptions<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = WSSRequestOptions<Events, Meta> & { capacity?: number; idleTimeoutMs?: number | null }

export type WSSStreamHandler<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord,
  Input = unknown,
  Chunk = unknown
> = WSSRequestHandler<Events, Meta, Input, AsyncGenerator<Chunk>>
