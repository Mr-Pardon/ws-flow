import type { EnvelopeBuilderLike } from '@ws-flow/envelope'
import type { WSScheduler } from '@ws-flow/scheduler'
import type { PipelinePlugin } from '@ws-flow/pipeline'
import type { ProtocolAdapter } from '../protocol.js'
import type {
  CoreReceiveContext,
  CoreSendContext,
  CoreStreamContext,
  CoreReceiveMiddleware,
  CoreReceiveEventMiddlewareRecord,
  CoreSendMiddleware,
  CoreSendEventMiddlewareRecord,
  CoreStreamMiddleware,
  PeerLike,
  ReceiveStreamLike,
  SendDispatcherLike,
  ReceiveDispatcherLike
} from './index.js'

export type RuntimeBase<Peer extends PeerLike = PeerLike> = {
  /** Runtime-owned message/frame builder; does not enable protocol capabilities. */
  builder?: {
    envelope: EnvelopeBuilderLike
  }
  protocol?: ProtocolAdapter
  transport: {
    send(peer: Peer, data: unknown): Promise<void> | void
    /** Optional admission/encoding phase, before a task is marked as sent. */
    prepare?(
      peer: Peer,
      data: unknown,
      signal?: AbortSignal
    ): PreparedTransportSend | Promise<PreparedTransportSend>
  }
}

export interface PreparedTransportSend {
  /** Commit to the captured connection; do not retarget after reconnection. */
  send(): Promise<void> | void
  /** Idempotently release owned resources; must not throw. */
  release(): void
}

export type RuntimePipelines<Peer extends PeerLike = PeerLike> = {
  receiveMiddlewares: CoreReceiveMiddleware<Peer>[]
  receiveEventMiddlewares: CoreReceiveEventMiddlewareRecord<Peer>[]
  sendMiddlewares: CoreSendMiddleware<Peer>[]
  sendEventMiddlewares: CoreSendEventMiddlewareRecord<Peer>[]
  receiveStreamMiddlewares: CoreStreamMiddleware<Peer>[]
  sendStreamMiddlewares: CoreStreamMiddleware<Peer>[]

  use(
    plugins:
      | PipelinePlugin<
          CoreReceiveContext<Peer>,
          CoreSendContext<Peer>,
          CoreStreamContext<Peer>
        >
      | PipelinePlugin<
          CoreReceiveContext<Peer>,
          CoreSendContext<Peer>,
          CoreStreamContext<Peer>
        >[]
  ): RuntimePipelines<Peer>

  useReceive(
    middlewares: CoreReceiveMiddleware<Peer> | CoreReceiveMiddleware<Peer>[]
  ): RuntimePipelines<Peer>

  useReceiveEvent(
    records:
      | CoreReceiveEventMiddlewareRecord<Peer>
      | CoreReceiveEventMiddlewareRecord<Peer>[]
  ): RuntimePipelines<Peer>

  useReceiveStream(
    middlewares: CoreStreamMiddleware<Peer> | CoreStreamMiddleware<Peer>[]
  ): RuntimePipelines<Peer>

  useSend(
    middlewares: CoreSendMiddleware<Peer> | CoreSendMiddleware<Peer>[]
  ): RuntimePipelines<Peer>

  useSendEvent(
    records: CoreSendEventMiddlewareRecord<Peer> | CoreSendEventMiddlewareRecord<Peer>[]
  ): RuntimePipelines<Peer>

  useSendStream(
    middlewares: CoreStreamMiddleware<Peer> | CoreStreamMiddleware<Peer>[]
  ): RuntimePipelines<Peer>
}

export type RuntimeDispatchers<Peer extends PeerLike = PeerLike> = {
  send: SendDispatcherLike<Peer>
  receive: ReceiveDispatcherLike<Peer>
}

export type RuntimeSchedulers = {
  inScheduler: WSScheduler
  outScheduler: WSScheduler
  setScheduler: (scheduler: WSScheduler) => void
  setInScheduler: (scheduler: WSScheduler) => void
  setOutScheduler: (scheduler: WSScheduler) => void
}

export type RuntimeContext<Peer extends PeerLike = PeerLike> = {
  base: RuntimeBase<Peer>
  pipelines: RuntimePipelines<Peer>
  dispatchers: RuntimeDispatchers<Peer>
  schedulers: RuntimeSchedulers
}
