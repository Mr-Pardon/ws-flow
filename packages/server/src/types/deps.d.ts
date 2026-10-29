import { SendStreamLike } from '@ws-flow/runtime-core'
import type { EnvelopeBuilder, WSEnvelope } from '@ws-flow/envelope'
import type { AnyRecord, SendOptions, SendStreamOptions } from './wss.js'
import type { WSSession } from '../core/index.js'
import type { RoomManager } from '../core/roomManager.js'
import type {
  WSSRequestOptions,
  WSSRequestHandler,
  WSSRequestStreamOptions,
  WSSStreamHandler
} from './request.js'
import type { StreamTask } from '@ws-flow/runtime-core'
import type {
  MessageSubscription,
  SubscriptionFilter,
  SubscriptionOptions
} from '@ws-flow/runtime-core'

export type WSSessionDeps<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> = {
  subscribe(
    peer: WSSession<Events, Meta>,
    filter: SubscriptionFilter<WSSession<Events, Meta>>,
    options?: Omit<SubscriptionOptions<WSSession<Events, Meta>>, 'peer'>
  ): MessageSubscription<WSSession<Events, Meta>>
  report(error: unknown, peer: WSSession<Events, Meta>): void
  request<Result>(
    peer: WSSession<Events, Meta>,
    event: string,
    input: unknown,
    options?: WSSRequestOptions<Events, Meta>
  ): Promise<Result>
  requestStream<Chunk>(
    peer: WSSession<Events, Meta>,
    event: string,
    input: unknown,
    options?: WSSRequestStreamOptions<Events, Meta>
  ): StreamTask<Chunk>
  handleStream<Input, Chunk>(
    peer: WSSession<Events, Meta>,
    event: string,
    handler: WSSStreamHandler<Events, Meta, Input, Chunk>
  ): () => void
  handleRequest<Input, Result>(
    peer: WSSession<Events, Meta>,
    event: string,
    handler: WSSRequestHandler<Events, Meta, Input, Result>
  ): () => void
  envelopeBuilder: EnvelopeBuilder<Events>
  sendDispatcher: {
    dispatch: (
      peer: WSSession<Events, Meta>,
      env: WSEnvelope,
      options?: SendOptions<Events, Meta>
    ) => Promise<void>
  }
  roomManager: RoomManager<Events, Meta>
  createSendStream: (
    peer: WSSession<Events, Meta>,
    event?: string,
    options?: SendStreamOptions<Events, Meta>
  ) => SendStreamLike
}
