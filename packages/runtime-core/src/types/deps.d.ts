import type { EnvelopeBuilderLike } from '@ws-flow/envelope'
import type { WSScheduler, QueueCapacityError } from '@ws-flow/scheduler'
import type { ProtocolAdapter } from '../protocol.js'
import type { RuntimeBase } from './runtime.js'
import type { TransportLimitError } from '../transportLimits.js'
import type {
  ActiveTaskBudget,
  ActiveTaskKind,
  TaskCapacityError,
  TaskTimeoutError
} from '../taskResources.js'
import type {
  PeerLike,
  InvalidStreamObserver,
  CoreReceiveMiddleware,
  CoreReceiveContext,
  CoreReceiveEventMiddlewareRecord,
  CoreStreamMiddleware,
  CoreSendMiddleware,
  CoreSendEventMiddlewareRecord,
  ReceiveStreamLike,
  SendDispatcherLike
} from '../types/index.js'

export type ReceiveDispatcherDeps<Peer extends PeerLike = PeerLike> = {
  taskBudget?: ActiveTaskBudget<Peer>
  streamIdleMs?: number | null
  onTaskError?: (
    peer: Peer,
    error: TaskCapacityError | TaskTimeoutError,
    kind: ActiveTaskKind
  ) => void
  onQueueError?: (peer: Peer, error: QueueCapacityError) => void
  protocol?: ProtocolAdapter
  publish?: (ctx: CoreReceiveContext<Peer>) => void
  cancelStream?: (peer: Peer, streamId: string) => void
  notifyStreamCancel?: (peer: Peer, streamId: string) => void
  onStreamError?: (error: unknown, ctx: CoreReceiveContext<Peer>) => void | Promise<void>
  acceptRequest?: (ctx: CoreReceiveContext<Peer>) => boolean | Promise<boolean>
  rejectRequest?: (ctx: CoreReceiveContext<Peer>, error: unknown) => void
  pipelines: {
    receiveMiddlewares: CoreReceiveMiddleware<Peer>[]
    receiveEventMiddlewares: CoreReceiveEventMiddlewareRecord<Peer>[]
    receiveStreamMiddlewares: CoreStreamMiddleware<Peer>[]
  }
  schedulers: {
    readonly inScheduler: WSScheduler
  }
  invalidStreamObservers: InvalidStreamObserver<Peer>[]
  createReceiveStream: (peer: Peer) => ReceiveStreamLike<any>
}

export type SendDispatcherDeps<Peer extends PeerLike = PeerLike> = {
  onTransportLimit?: (peer: Peer, error: TransportLimitError, fatal: boolean) => void
  onQueueError?: (peer: Peer, error: QueueCapacityError, fatal: boolean) => void
  protocol?: ProtocolAdapter
  transport: RuntimeBase<Peer>['transport']
  pipelines: {
    sendMiddlewares: CoreSendMiddleware<Peer>[]
    sendEventMiddlewares: CoreSendEventMiddlewareRecord<Peer>[]
  }
  schedulers: {
    readonly outScheduler: WSScheduler
  }
}

export type SendStreamDeps<Peer extends PeerLike = PeerLike> = {
  onIdleTimeout?: (error: TaskTimeoutError) => void
  onStart?: (streamId: string) => void
  envelopeBuilder: EnvelopeBuilderLike
  sendDispatcher: SendDispatcherLike<Peer>
  sendStreamMiddlewares: CoreStreamMiddleware<Peer>[]
}
