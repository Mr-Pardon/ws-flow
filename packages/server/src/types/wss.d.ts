import type { RawData } from 'ws'
import type { EnvelopeBuilderLike, WSEnvelope as BaseWSEnvelope } from '@ws-flow/envelope'
import type { QueueLimits, WSScheduler } from '@ws-flow/scheduler'
import type {
  ByteLimits,
  SendTransportLimits,
  RuntimeTaskLimits,
  RuntimeTaskTimeouts
} from '@ws-flow/runtime-core'
import { WSSession } from '../core/index.js'
import type { WSReceiveContext, SendMiddleware, StreamMiddleware } from './pipeline.js'

export type AnyRecord = Record<string, any>

export type WSEnvelope<Events extends AnyRecord = AnyRecord> = BaseWSEnvelope<Events>

export type WSSendData = string | Buffer | ArrayBuffer | ArrayBufferView

export type WSSCodec = {
  encode?: (envelope: WSEnvelope<AnyRecord>) => WSSendData
  decode?: (raw: RawData) => any
}

export type WSServerMessageConfig<Events extends AnyRecord = AnyRecord> = {
  builder?: EnvelopeBuilderLike
} & WSSCodec

export type WSServerOptions<Events extends AnyRecord = AnyRecord> = {
  message?: WSServerMessageConfig<Events>
  schedulers?: {
    in?: WSScheduler
    out?: WSScheduler
  }
  queueLimits?: { receive?: QueueLimits; send?: QueueLimits }
  transportLimits?: { receive?: ByteLimits; send?: SendTransportLimits }
  taskLimits?: RuntimeTaskLimits
  taskTimeouts?: RuntimeTaskTimeouts
}

export type WSSHandler<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = (ctx: WSReceiveContext<Events, Meta>) => any | Promise<any>

export type Rooms<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = Map<string, Set<WSSession<Events, Meta>>>

export type SendOptions<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = {
  signal?: AbortSignal
  priority?: number
  meta?: AnyRecord
  sendMiddlewares?: SendMiddleware<Events, Meta>[]
}

export type SendStreamOptions<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = SendOptions<Events, Meta> & {
  idleTimeoutMs?: number | null
  streamMiddlewares?: StreamMiddleware<Events, Meta>[]
}

export type AutoSendMode = false | 'data' | 'envelope' | 'stream' | 'smart'
