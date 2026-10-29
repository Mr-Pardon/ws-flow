import { PipelineRegistry } from '@ws-flow/pipeline'
import { SchedulerRuntime, WSScheduler } from '@ws-flow/scheduler'
import { SendDispatcher } from './sendDispatcher.js'
import { ReceiveDispatcher } from './receiveDispatcher.js'
import { SendStream } from './sendStream.js'
import { wsFlowAdapter } from '../protocol.js'
import { createEnvelopeBuilder } from '@ws-flow/envelope'
import { SubscriptionRegistry } from './subscriptions.js'
import { RequestManager, RequestError } from './requests.js'
import type {
  RequestOptions,
  RequestHandler,
  RequestStreamOptions,
  StreamRequestHandler
} from './requests.js'
import { StreamTask } from './streamTask.js'
import type { QueueFailure, TransportFailure, TaskFailure } from '../types/resources.js'
import {
  ActiveTaskBudget,
  TaskCapacityError,
  TaskTimeoutError,
  validateTaskTimeouts
} from '../taskResources.js'
import type {
  ActiveTaskKind,
  RuntimeTaskLimits,
  RuntimeTaskTimeouts
} from '../taskResources.js'
import type { SubscriptionFilter, SubscriptionOptions } from './subscriptions.js'
import {
  createReceiveDispatcherDeps,
  createSendDispatcherDeps,
  createSendStreamDeps
} from './deps.js'
import type {
  PeerLike,
  RuntimeContext,
  RuntimeBase,
  RuntimeDispatchers,
  RuntimePipelines,
  RuntimeSchedulers,
  CoreReceiveContext,
  CoreSendContext,
  CoreStreamContext,
  InvalidStreamObserver,
  SendOptions,
  SendStreamOptions
} from '../types/index.js'

type SchedulerOptions = {
  in: WSScheduler
  out: WSScheduler
}

export class RuntimeCore<Peer extends PeerLike = PeerLike> {
  private _base: RuntimeBase<Peer>
  private _pipelines: RuntimePipelines<Peer>
  private _schedulers: RuntimeSchedulers
  private _dispatchers: { send: SendDispatcher<Peer>; receive: ReceiveDispatcher<Peer> }
  private _disposed = false
  private _taskBudget: ActiveTaskBudget<Peer>
  private _taskTimeouts: RuntimeTaskTimeouts
  private _taskErrorObservers: ((info: TaskFailure<Peer>) => void | Promise<void>)[] = []
  private _transportErrorObservers: ((
    info: TransportFailure<Peer>
  ) => void | Promise<void>)[] = []
  private _queueErrorObservers: ((info: QueueFailure<Peer>) => void | Promise<void>)[] =
    []
  private _closedPeers = new WeakSet<Peer>()
  private _streamErrorObservers: ((
    error: unknown,
    ctx: CoreReceiveContext<Peer>
  ) => void | Promise<void>)[] = []
  private _invalidStreamObservers: InvalidStreamObserver<Peer>[] = []
  private _subscriptions = new SubscriptionRegistry<Peer>()
  private _sendStreams = new Map<Peer, Set<SendStream<Peer>>>()
  private _sendStreamsById = new Map<Peer, Map<string, SendStream<Peer>>>()
  private _remoteCancelledStreams = new WeakSet<SendStream<Peer>>()
  private _requests: RequestManager<Peer>
  private _requestErrorObservers: ((
    error: unknown,
    peer: Peer
  ) => void | Promise<void>)[] = []

  constructor(input: {
    base: RuntimeBase<Peer>
    schedulers?: SchedulerOptions
    taskLimits?: RuntimeTaskLimits
    taskTimeouts?: RuntimeTaskTimeouts
  }) {
    this._taskBudget = new ActiveTaskBudget(input.taskLimits)
    this._taskTimeouts = validateTaskTimeouts(input.taskTimeouts)
    // based
    this._base = {
      ...input.base,
      protocol: input.base.protocol ?? wsFlowAdapter(),
      builder: input.base.builder ?? { envelope: createEnvelopeBuilder() }
    }

    // pipelines
    this._pipelines = new PipelineRegistry<
      CoreReceiveContext<Peer>,
      CoreSendContext<Peer>,
      CoreStreamContext<Peer>
    >() as RuntimePipelines<Peer>

    // schedulers
    const { schedulers } = input
    this._schedulers = new SchedulerRuntime(schedulers?.in, schedulers?.out)

    // dispatchers
    this._dispatchers = this._createDispatchers()
    this._requests = new RequestManager({
      budget: this._taskBudget,
      timeouts: this._taskTimeouts,
      onTaskError: (peer, error, kind) => this._taskFailure({ peer, error, kind }),
      protocol: this._base.protocol!,
      send: (peer, message, options, onTransportStart) =>
        this._dispatchers.send.dispatch(peer, message, options, onTransportStart),
      sendStream: (peer, requestId, source, signal, onStart, onTerminal, onTaskTimeout) =>
        this._sendResponseStream(
          peer,
          requestId,
          source,
          signal,
          onStart,
          onTerminal,
          onTaskTimeout
        ),
      report: (error, peer) => {
        if (!this._requestErrorObservers.length)
          console.error('[RuntimeCore] request response failed', error)
        for (const observer of [...this._requestErrorObservers]) {
          void Promise.resolve()
            .then(() => observer(error, peer))
            .catch(failure =>
              console.error('[RuntimeCore] request observer failed', failure)
            )
        }
      }
    })
  }

  private _createDispatchers() {
    const sendDispatcherDeps = createSendDispatcherDeps<Peer>({
      onTransportLimit: (peer, error, fatal) =>
        this._transportFailure({ peer, error, fatal, direction: 'send' }),
      onQueueError: (peer, error, fatal) =>
        this._queueFailure({ peer, error, fatal, direction: 'send' }),
      base: this._base,
      pipelines: this._pipelines,
      schedulers: this._schedulers
    })
    const send = new SendDispatcher<Peer>(sendDispatcherDeps)

    const receiveDispatcherDeps = createReceiveDispatcherDeps<Peer>({
      taskBudget: this._taskBudget,
      streamIdleMs: this._taskTimeouts.streamIdleMs,
      onTaskError: (peer, error, kind) => this._taskFailure({ peer, error, kind }),
      onQueueError: (peer, error) =>
        this._queueFailure({ peer, error, fatal: true, direction: 'receive' }),
      protocol: this._base.protocol,
      pipelines: this._pipelines,
      schedulers: this._schedulers,
      invalidStreamObservers: this._invalidStreamObservers,
      publish: ctx => this._subscriptions.publish(ctx.raw, ctx.envelope, ctx.peer),
      cancelStream: (peer, streamId) => {
        const stream = this._sendStreamsById.get(peer)?.get(streamId)
        if (stream) {
          this._remoteCancelledStreams.add(stream)
          stream.cancel(new RequestError('CANCELLED', 'Remote cancelled stream'))
        }
      },
      notifyStreamCancel: (peer, streamId) => {
        if (!this._base.protocol?.stream?.cancel) return
        void this._dispatchers.send
          .dispatch(peer, { kind: 'stream_cancel', streamId })
          .catch(error => {
            if (!this._closedPeers.has(peer) && !this._disposed)
              console.error('[RuntimeCore] stream cancellation failed', error)
          })
      },
      acceptRequest: ctx => this._requests.accept(ctx.peer, ctx.envelope),
      rejectRequest: (ctx, error) => this._requests.reject(ctx.peer, ctx.envelope, error),
      onStreamError: async (error, ctx) => {
        if (!this._streamErrorObservers.length)
          console.error('[RuntimeCore] stream handler failed', error)
        const results = await Promise.allSettled(
          this._streamErrorObservers.map(fn =>
            Promise.resolve().then(() => fn(error, ctx))
          )
        )
        for (const result of results) {
          if (result.status === 'rejected')
            console.error('[RuntimeCore] stream error observer failed', result.reason)
        }
      }
    })
    const receive = new ReceiveDispatcher<Peer>(receiveDispatcherDeps)

    return { send, receive }
  }

  useInvalidStreamObserver(observer: InvalidStreamObserver<Peer>) {
    this._invalidStreamObservers.push(observer)
    return this
  }

  useTaskErrorObserver(observer: (info: TaskFailure<Peer>) => void | Promise<void>) {
    this._taskErrorObservers.push(observer)
    return this
  }

  private _taskFailure(info: TaskFailure<Peer>) {
    if (this._disposed || this._closedPeers.has(info.peer)) return
    for (const observer of [...this._taskErrorObservers]) {
      try {
        void Promise.resolve(observer(info)).catch(error =>
          console.error('[RuntimeCore] task observer failed', error)
        )
      } catch (error) {
        console.error('[RuntimeCore] task observer failed', error)
      }
    }
  }

  getActiveTaskCount(peer?: Peer, kind?: ActiveTaskKind) {
    return this._taskBudget.getActiveCount(peer, kind)
  }

  useQueueErrorObserver(observer: (info: QueueFailure<Peer>) => void | Promise<void>) {
    this._queueErrorObservers.push(observer)
    return this
  }

  private _queueFailure(info: QueueFailure<Peer>) {
    if (this._disposed || this._closedPeers.has(info.peer)) return
    if (info.fatal) this.closePeer(info.peer, info.error)
    for (const observer of [...this._queueErrorObservers]) {
      try {
        void Promise.resolve(observer(info)).catch(error =>
          console.error('[RuntimeCore] queue observer failed', error)
        )
      } catch (error) {
        console.error('[RuntimeCore] queue observer failed', error)
      }
    }
  }

  useTransportErrorObserver(
    observer: (info: TransportFailure<Peer>) => void | Promise<void>
  ) {
    this._transportErrorObservers.push(observer)
    return this
  }

  private _transportFailure(info: TransportFailure<Peer>) {
    if (this._disposed || this._closedPeers.has(info.peer)) return
    if (info.fatal) this.closePeer(info.peer, info.error)
    for (const observer of [...this._transportErrorObservers]) {
      try {
        void Promise.resolve(observer(info)).catch(error =>
          console.error('[RuntimeCore] transport observer failed', error)
        )
      } catch (error) {
        console.error('[RuntimeCore] transport observer failed', error)
      }
    }
  }

  private async _sendResponseStream(
    peer: Peer,
    requestId: string,
    source: AsyncGenerator<any>,
    signal: AbortSignal,
    onStart?: (streamId: string) => void,
    onTerminal?: () => void,
    onTaskTimeout?: (error: TaskTimeoutError) => void
  ) {
    let started = false,
      terminal = false,
      streamId: string | undefined
    const serialize = (error: unknown) => ({
      code:
        error instanceof RequestError || error instanceof TaskTimeoutError
          ? error.code
          : 'STREAM_ERROR',
      message: error instanceof Error ? error.message : 'Stream failed'
    })
    const stream = new SendStream<Peer>(
      {
        onIdleTimeout: error => {
          if (onTaskTimeout) onTaskTimeout(error)
          else this._taskFailure({ peer, error, kind: 'incomingRequests' })
        },
        envelopeBuilder: this._base.builder!.envelope!,
        sendStreamMiddlewares: this._pipelines.sendStreamMiddlewares,
        sendDispatcher: {
          dispatch: async (target, frame, options, check) => {
            const message = {
              ...frame,
              kind: 'request_stream' as const,
              requestId,
              ...(frame.streamState === 'error' ? { error: serialize(frame.error) } : {})
            }
            await this._dispatchers.send.dispatch(target, message, options, () => {
              check?.()
              if (frame.streamState === 'start') {
                started = true
                streamId = frame.streamId
                onStart?.(frame.streamId!)
              }
              if (frame.streamState === 'end' || frame.streamState === 'error') {
                terminal = true
                onTerminal?.()
              }
            })
            if (frame.streamState === 'end' || frame.streamState === 'error')
              terminal = true
          }
        }
      },
      peer,
      undefined,
      { signal, idleTimeoutMs: this._taskTimeouts.streamIdleMs }
    )
    try {
      await stream.runGenerator(source)
      if (!terminal && !signal.aborted)
        throw new Error('Response stream ended without a terminal frame')
    } catch (error) {
      if (signal.aborted || terminal) return
      if (!started) throw error
      // Once start was attempted, failures stay in the stream protocol.
      await this._dispatchers.send
        .dispatch(
          peer,
          {
            kind: 'request_stream',
            requestId,
            streamId,
            streamState: 'error',
            error: serialize(error)
          },
          { signal },
          onTerminal
        )
        .catch(failure => {
          if (!signal.aborted) {
            for (const observer of this._requestErrorObservers) {
              void Promise.resolve()
                .then(() => observer(failure, peer))
                .catch(() => {})
            }
            if (!this._requestErrorObservers.length)
              console.error('[RuntimeCore] stream response failed', failure)
          }
        })
    }
  }

  useStreamErrorObserver(
    observer: (error: unknown, ctx: CoreReceiveContext<Peer>) => void | Promise<void>
  ) {
    this._streamErrorObservers.push(observer)
    return this
  }

  closePeer(peer: Peer, reason: unknown = new Error('Peer is closed')) {
    this._closedPeers.add(peer)
    this._dispatchers.send.closePeer(peer, reason)
    this._requests.closePeer(peer, reason)
    for (const stream of this._sendStreams.get(peer) ?? []) stream.cancel(reason)
    this._dispatchers.receive.closePeer(peer, reason)
    this._subscriptions.clear(peer)
    this._taskBudget.closePeer(peer)
  }

  dispose() {
    if (this._disposed) return
    this._disposed = true
    this._dispatchers.send.dispose()
    this._requests.dispose()
    for (const streams of this._sendStreams.values()) {
      for (const stream of streams) stream.cancel(new Error('Runtime is closed'))
    }
    this._dispatchers.receive.dispose()
    this._subscriptions.clear()
    this._taskBudget.clear()
  }

  /** Raw decoded message matching, independent of event extraction. */
  subscribe(filter: SubscriptionFilter<Peer>, options?: SubscriptionOptions<Peer>) {
    if (this._disposed) throw new Error('Runtime is closed')
    if (
      options?.peer &&
      (this._closedPeers.has(options.peer) || options.peer.state !== 'open')
    )
      throw new Error('Peer is closed')
    return this._subscriptions.subscribe(filter, options)
  }

  async request<Result = unknown>(
    peer: Peer,
    event: string,
    input: unknown,
    options?: RequestOptions<Peer>
  ): Promise<Result> {
    if (this._disposed || this._closedPeers.has(peer) || peer.state !== 'open')
      throw new Error('Peer is closed')
    return this._requests.request<Result>(peer, event, input, options)
  }

  requestStream<Chunk = unknown>(
    peer: Peer,
    event: string,
    input: unknown,
    options?: RequestStreamOptions<Peer>
  ) {
    const task = new StreamTask<Chunk>(
      peer,
      this._pipelines.receiveStreamMiddlewares,
      options?.capacity,
      options?.idleTimeoutMs === undefined
        ? this._taskTimeouts.streamIdleMs
        : options.idleTimeoutMs,
      error => this._taskFailure({ peer, error, kind: 'outgoingRequests' })
    )
    if (this._disposed || this._closedPeers.has(peer) || peer.state !== 'open')
      task.finish(new Error('Peer is closed'), true)
    else
      void this._requests
        .request<void>(peer, event, input, options, task)
        .catch(error => task.finish(error, true))
    return task
  }

  handleStream<Input = unknown, Chunk = unknown>(
    event: string,
    handler: StreamRequestHandler<Peer, Input, Chunk>,
    peer?: Peer
  ) {
    if (
      this._disposed ||
      (peer && (this._closedPeers.has(peer) || peer.state !== 'open'))
    )
      throw new Error('Runtime or peer is closed')
    return this._requests.register(event, handler, peer, 'stream')
  }

  handleRequest<Input = unknown, Result = unknown>(
    event: string,
    handler: RequestHandler<Peer, Input, Result>,
    peer?: Peer
  ) {
    if (
      this._disposed ||
      (peer && (this._closedPeers.has(peer) || peer.state !== 'open'))
    )
      throw new Error('Runtime or peer is closed')
    return this._requests.register(event, handler, peer)
  }

  useRequestErrorObserver(
    observer: (error: unknown, peer: Peer) => void | Promise<void>
  ) {
    this._requestErrorObservers.push(observer)
    return this
  }

  /** Only clears subscriptions; full peer/task disposal is a separate concern. */
  clearSubscriptions(peer?: Peer) {
    this._subscriptions.clear(peer)
  }

  /** Explicit data entry: reserved-looking business fields remain payload. */
  send(peer: Peer, data: unknown, options?: SendOptions<Peer>) {
    return this._dispatchers.send.dispatch(peer, { data }, options)
  }

  emit(peer: Peer, event: string, data: unknown, options?: SendOptions<Peer>) {
    return this._dispatchers.send.dispatch(peer, { event, data }, options)
  }

  setScheduler(scheduler: WSScheduler) {
    this._schedulers.setScheduler(scheduler)
  }

  setInScheduler(scheduler: WSScheduler) {
    this._schedulers.setInScheduler(scheduler)
  }

  setOutScheduler(scheduler: WSScheduler) {
    this._schedulers.setOutScheduler(scheduler)
  }

  createSendStream(peer: Peer, event?: string, options?: SendStreamOptions<Peer>) {
    if (this._disposed || this._closedPeers.has(peer) || peer.state !== 'open')
      throw new Error('Peer is closed')
    const deps = createSendStreamDeps<Peer>({
      base: this._base,
      dispatchers: this._dispatchers,
      pipelines: this._pipelines
    })
    let release: () => void
    try {
      release = this._taskBudget.reserve(peer, 'outgoingStreams')
    } catch (error) {
      if (error instanceof TaskCapacityError)
        this._taskFailure({ peer, error, kind: 'outgoingStreams' })
      throw error
    }
    let registeredId: string | undefined
    let started = false,
      terminal = false
    let stream: SendStream<Peer>
    try {
      stream = new SendStream<Peer>(
        {
          ...deps,
          onIdleTimeout: error =>
            this._taskFailure({ peer, error, kind: 'outgoingStreams' }),
          sendDispatcher: {
            dispatch: async (target, frame, sendOptions, check) => {
              await this._dispatchers.send.dispatch(target, frame, sendOptions, () => {
                check?.()
                if (frame.streamState === 'start') started = true
                if (frame.streamState === 'end' || frame.streamState === 'error')
                  terminal = true
              })
              if (frame.streamState === 'end' || frame.streamState === 'error')
                terminal = true
            }
          },
          onStart: streamId => {
            let byId = this._sendStreamsById.get(peer)
            if (!byId) this._sendStreamsById.set(peer, (byId = new Map()))
            if (byId.has(streamId)) throw new Error('Duplicate outgoing streamId')
            byId.set(streamId, stream)
            registeredId = streamId
          }
        },
        peer,
        event,
        {
          ...options,
          idleTimeoutMs:
            options?.idleTimeoutMs === undefined
              ? this._taskTimeouts.streamIdleMs
              : options.idleTimeoutMs
        }
      )
    } catch (error) {
      release()
      throw error
    }
    let streams = this._sendStreams.get(peer)
    if (!streams) this._sendStreams.set(peer, (streams = new Set()))
    streams.add(stream)
    const current = streams
    stream.onClose(() => {
      release()
      const byId = this._sendStreamsById.get(peer)
      if (registeredId && byId?.get(registeredId) === stream) byId.delete(registeredId)
      if (byId && !byId.size) this._sendStreamsById.delete(peer)
      current.delete(stream)
      if (!current.size) this._sendStreams.delete(peer)
      if (
        started &&
        !terminal &&
        registeredId &&
        !this._remoteCancelledStreams.has(stream) &&
        !this._disposed &&
        !this._closedPeers.has(peer) &&
        peer.state === 'open'
      ) {
        void this._dispatchers.send
          .dispatch(peer, {
            streamId: registeredId,
            streamState: stream.failed ? 'error' : 'end',
            ...(stream.failed
              ? {
                  error: {
                    code:
                      stream.error instanceof TaskTimeoutError
                        ? stream.error.code
                        : 'STREAM_ERROR',
                    message:
                      stream.error instanceof Error
                        ? stream.error.message
                        : 'Stream cancelled'
                  }
                }
              : {})
          })
          .catch(error => console.error('[RuntimeCore] stream termination failed', error))
      }
    })
    return stream
  }

  get base() {
    return this._base
  }

  get pipelines() {
    return this._pipelines
  }

  get schedulers() {
    return this._schedulers
  }

  get dispatchers() {
    return this._dispatchers
  }

  get context(): RuntimeContext<Peer> {
    return {
      base: this._base,
      pipelines: this._pipelines,
      dispatchers: this._dispatchers,
      schedulers: this._schedulers
    }
  }
}
