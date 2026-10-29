import type { RawData } from 'ws'
import type { WSEnvelope } from '@ws-flow/envelope'
import type {
  InvalidStreamReason,
  TransportLimitError,
  TaskCapacityError,
  TaskTimeoutError,
  ActiveTaskKind
} from '@ws-flow/runtime-core'
import type { QueueCapacityError } from '@ws-flow/scheduler'
import type { AnyRecord } from './wss.js'
import type { WSSession } from '../core/index.js'

export type WSSIssueType =
  | 'server_error'
  | 'invalid_stream'
  | 'decode_error'
  | 'process_error'
  | 'transport_error'
  | 'subscription_error'
  | 'resource_error'

type WSSBaseIssueInput<
  Type extends WSSIssueType,
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = {
  type: Type
  message: string
  session?: WSSession<Events, Meta>
}

export type WSSInvalidStreamIssueInput<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = WSSBaseIssueInput<'invalid_stream', Events, Meta> & {
  session: WSSession<Events, Meta>
  envelope: WSEnvelope
  reason: InvalidStreamReason
}

export type WSSDecodeIssueInput<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = WSSBaseIssueInput<'decode_error', Events, Meta> & {
  raw: RawData
  error: unknown
}

export type WSSProcessIssueInput<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = WSSBaseIssueInput<'process_error', Events, Meta> & {
  session: WSSession<Events, Meta>
  envelope?: WSEnvelope
  error: unknown
}

export type WSSTransportIssueInput<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = WSSBaseIssueInput<'transport_error', Events, Meta> & {
  session: WSSession<Events, Meta>
  envelope: WSEnvelope
  error: unknown
}

export type WSSIssueInput<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> =
  | (WSSBaseIssueInput<'server_error', Events, Meta> & { error: unknown })
  | WSSInvalidStreamIssueInput<Events, Meta>
  | WSSDecodeIssueInput<Events, Meta>
  | WSSProcessIssueInput<Events, Meta>
  | WSSTransportIssueInput<Events, Meta>
  | (WSSBaseIssueInput<'resource_error', Events, Meta> & {
      session: WSSession<Events, Meta>
      direction: 'send' | 'receive'
      error:
        | QueueCapacityError
        | TransportLimitError
        | TaskCapacityError
        | TaskTimeoutError
      taskKind?: ActiveTaskKind
    })
  | (WSSBaseIssueInput<'subscription_error', Events, Meta> & { error: unknown })

export type WSSIssue<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = WSSIssueInput<Events, Meta> & {
  timestamp: number
}

export type WSSIssueOfType<
  Type extends WSSIssueType,
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = Extract<WSSIssue<Events, Meta>, { type: Type }>

export type WSSIssueHandler<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = (issue: WSSIssue<Events, Meta>) => void | Promise<void>

export type WSSTypedIssueHandler<
  Type extends WSSIssueType,
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = (issue: WSSIssueOfType<Type, Events, Meta>) => void | Promise<void>
