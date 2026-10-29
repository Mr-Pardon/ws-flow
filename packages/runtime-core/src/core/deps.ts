import { ReceiveStream } from './receiveStream.js'
import { wsFlowAdapter, ProtocolCapabilityError } from '../protocol.js'
import { createEnvelopeBuilder } from '@ws-flow/envelope'
import type { ProtocolAdapter } from '../protocol.js'
import type {
  PeerLike,
  InvalidStreamObserver,
  RuntimeBase,
  RuntimePipelines,
  RuntimeSchedulers,
  RuntimeDispatchers,
  SendStreamDeps,
  SendDispatcherDeps,
  ReceiveDispatcherDeps
} from '../types/index.js'

export function createReceiveDispatcherDeps<Peer extends PeerLike = PeerLike>(input: {
  taskBudget?: ReceiveDispatcherDeps<Peer>['taskBudget']
  streamIdleMs?: number | null
  onTaskError?: ReceiveDispatcherDeps<Peer>['onTaskError']
  protocol?: ProtocolAdapter
  onQueueError?: ReceiveDispatcherDeps<Peer>['onQueueError']
  pipelines: RuntimePipelines<Peer>
  schedulers: RuntimeSchedulers
  invalidStreamObservers: InvalidStreamObserver<Peer>[]
  publish?: ReceiveDispatcherDeps<Peer>['publish']
  cancelStream?: ReceiveDispatcherDeps<Peer>['cancelStream']
  notifyStreamCancel?: ReceiveDispatcherDeps<Peer>['notifyStreamCancel']
  onStreamError?: ReceiveDispatcherDeps<Peer>['onStreamError']
  acceptRequest?: ReceiveDispatcherDeps<Peer>['acceptRequest']
  rejectRequest?: ReceiveDispatcherDeps<Peer>['rejectRequest']
}): ReceiveDispatcherDeps<Peer> {
  return {
    taskBudget: input.taskBudget,
    streamIdleMs: input.streamIdleMs,
    onTaskError: input.onTaskError,
    protocol: input.protocol,
    onQueueError: input.onQueueError,
    publish: input.publish,
    cancelStream: input.cancelStream,
    notifyStreamCancel: input.notifyStreamCancel,
    onStreamError: input.onStreamError,
    acceptRequest: input.acceptRequest,
    rejectRequest: input.rejectRequest,
    pipelines: {
      receiveMiddlewares: input.pipelines.receiveMiddlewares,
      receiveEventMiddlewares: input.pipelines.receiveEventMiddlewares,
      receiveStreamMiddlewares: input.pipelines.receiveStreamMiddlewares
    },
    schedulers: {
      get inScheduler() {
        return input.schedulers.inScheduler
      }
    },
    invalidStreamObservers: input.invalidStreamObservers,
    createReceiveStream: (peer: Peer) =>
      new ReceiveStream<Peer>(peer, input.pipelines.receiveStreamMiddlewares, {
        idleTimeoutMs: input.streamIdleMs,
        onIdleTimeout: error => input.onTaskError?.(peer, error, 'incomingStreams')
      })
  }
}

export function createSendDispatcherDeps<Peer extends PeerLike = PeerLike>(input: {
  onTransportLimit?: SendDispatcherDeps<Peer>['onTransportLimit']
  onQueueError?: SendDispatcherDeps<Peer>['onQueueError']
  base: RuntimeBase
  pipelines: RuntimePipelines<Peer>
  schedulers: RuntimeSchedulers
}): SendDispatcherDeps<Peer> {
  return {
    protocol: input.base.protocol,
    onQueueError: input.onQueueError,
    transport: input.base.transport,
    onTransportLimit: input.onTransportLimit,
    pipelines: {
      sendMiddlewares: input.pipelines.sendMiddlewares,
      sendEventMiddlewares: input.pipelines.sendEventMiddlewares
    },
    schedulers: {
      get outScheduler() {
        return input.schedulers.outScheduler
      }
    }
  }
}

export function createSendStreamDeps<Peer extends PeerLike = PeerLike>(input: {
  base: RuntimeBase
  dispatchers: Pick<RuntimeDispatchers<Peer>, 'send'>
  pipelines: Pick<RuntimePipelines<Peer>, 'sendStreamMiddlewares'>
}): SendStreamDeps<Peer> {
  const protocol = input.base.protocol ?? wsFlowAdapter()
  if (!protocol.stream) throw new ProtocolCapabilityError(protocol.name, 'stream sending')
  return {
    envelopeBuilder: input.base.builder?.envelope ?? createEnvelopeBuilder(),
    sendDispatcher: input.dispatchers.send,
    sendStreamMiddlewares: input.pipelines.sendStreamMiddlewares
  }
}
