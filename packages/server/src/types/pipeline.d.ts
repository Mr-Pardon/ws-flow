import { PipelinePlugin } from '@ws-flow/pipeline'
import {
  Middleware,
  EventMiddlewareRecord,
  EventMatcher as PipelineEventMatcher
} from '@ws-flow/pipeline'
import {
  CoreReceiveContext,
  CoreSendContext,
  CoreStreamContext
} from '@ws-flow/runtime-core'
import { AnyRecord, SendOptions } from './wss.js'
import { WSSession } from '../core/index.js'

export type Plugin<Events extends AnyRecord, Meta extends AnyRecord> = PipelinePlugin<
  WSReceiveContext<Events, Meta>,
  WSSendContext<Events, Meta>,
  WSStreamContext<Events, Meta>
>

export type WSReceiveContext<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = CoreReceiveContext<WSSession<Events, Meta>> & {
  session: WSSession<Events, Meta>
}

export type WSSendContext<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = CoreSendContext<WSSession<Events, Meta>> & {
  session: WSSession<Events, Meta>
}

export type WSStreamContext<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = CoreStreamContext<WSSession<Events, Meta>> & {
  session: WSSession<Events, Meta>
}

export type ReceiveMiddleware<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = Middleware<WSReceiveContext<Events, Meta>>

export type ReceiveEventMiddlewareRecord<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = EventMiddlewareRecord<WSReceiveContext<Events, Meta>>

export type SendMiddleware<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = Middleware<WSSendContext<Events, Meta>>

export type SendEventMiddlewareRecord<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = EventMiddlewareRecord<WSSendContext<Events, Meta>>

export type StreamMiddleware<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = Middleware<WSStreamContext<Events, Meta>>

export type EventMatcher = PipelineEventMatcher
