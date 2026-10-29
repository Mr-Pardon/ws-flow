import {
  wsFlowAdapter,
  validateRuntimeMessage,
  assertTaskIdentity,
  hasTaskIdentity
} from '../protocol.js'
import type { RuntimeMessage, ProtocolAdapter } from '../protocol.js'
import { composeMiddleware } from '@ws-flow/pipeline'
import { matchEvent } from '../utils/matcher.js'
import { PeerTasks } from '../utils/peerTasks.js'
import { QueueCapacityError } from '@ws-flow/scheduler'
import { TaskCapacityError } from '../taskResources.js'
import type {
  PeerLike,
  ReceiveDispatcherDeps,
  CoreReceiveContext,
  InvalidStreamReason,
  ReceiveStreamLike
} from '../types/index.js'

export class ReceiveDispatcher<Peer extends PeerLike = PeerLike> {
  private _protocol: ProtocolAdapter
  private _receiveStreams = new Map<Peer, Map<string, ReceiveStreamLike>>()
  private _closedPeers = new WeakSet<Peer>()
  private _disposed = false
  private _tasks = new PeerTasks<Peer>()
  private _remoteTerminals = new WeakSet<ReceiveStreamLike>()

  constructor(private _deps: ReceiveDispatcherDeps<Peer>) {
    this._protocol = _deps.protocol ?? wsFlowAdapter()
  }

  private _assertOpen(peer: Peer) {
    if (this._disposed || this._closedPeers.has(peer) || peer.state !== 'open') {
      throw new Error('Peer is closed')
    }
  }

  closePeer(peer: Peer, reason: unknown = new Error('Peer is closed')) {
    this._closedPeers.add(peer)
    this._tasks.close(peer, reason)
    const streams = this._receiveStreams.get(peer)
    this._receiveStreams.delete(peer)
    for (const stream of streams?.values() ?? []) {
      try {
        stream.stop('error', reason)
      } catch (error) {
        console.error('[ReceiveDispatcher] stream cleanup failed', error)
      }
    }
  }

  dispose() {
    this._disposed = true
    this._tasks.dispose()
    for (const peer of [...this._receiveStreams.keys()]) this.closePeer(peer)
  }

  private _createContext(
    peer: Peer,
    envelope: RuntimeMessage,
    raw: unknown
  ): CoreReceiveContext<Peer> {
    return {
      envelope,
      raw,
      peer: peer,
      stop() {}
    }
  }

  private _isStreamEnvelope(envelope: RuntimeMessage) {
    return envelope.streamId !== undefined && envelope.streamState !== undefined
  }

  private _validateStreamState(envelope: RuntimeMessage): InvalidStreamReason | null {
    const { streamId, streamState } = envelope

    if (!streamId) return null
    if (!streamState) return 'missing_state'

    const valid = ['start', 'chunk', 'end', 'error']
    if (!valid.includes(streamState)) {
      return 'invalid_state'
    }

    return null
  }

  private async _rejectStream(
    ctx: CoreReceiveContext<Peer>,
    reason: InvalidStreamReason
  ) {
    const messageMap: Record<InvalidStreamReason, string> = {
      missing_state: '[ReceiveDispatcher] streamState is required',
      invalid_state: `[ReceiveDispatcher] invalid streamState: ${ctx.envelope.streamState}`,
      duplicate_start: `[ReceiveDispatcher] duplicate stream start: ${ctx.envelope.streamId}`,
      missing_start: `[ReceiveStream] stream ${ctx.envelope.streamId} rejected: first frame must be start`
    }

    const info = {
      reason,
      envelope: ctx.envelope,
      peer: ctx.peer,
      message: messageMap[reason]
    }

    const results = await Promise.allSettled(
      this._deps.invalidStreamObservers.map(fn => Promise.resolve().then(() => fn(info)))
    )
    for (const result of results) {
      if (result.status === 'rejected')
        console.error('[ReceiveDispatcher] observer failed', result.reason)
    }
  }

  private async _runReceive(ctx: CoreReceiveContext<Peer>) {
    const pipeline = composeMiddleware<CoreReceiveContext<Peer>>(
      this._deps.pipelines.receiveMiddlewares
    )

    await pipeline(ctx)
  }

  private async _runRoute(ctx: CoreReceiveContext<Peer>, stream?: ReceiveStreamLike) {
    if (stream) ctx.stream = stream

    const event = ctx.envelope.event
    const middlewares = this._deps.pipelines.receiveEventMiddlewares
      .filter(({ matcher }) => matchEvent(event, matcher))
      .flatMap(({ middlewares }) => middlewares)

    const pipeline = composeMiddleware<CoreReceiveContext<Peer>>(middlewares)

    await pipeline(ctx)
  }

  private async _runConsumeStream(ctx: CoreReceiveContext<Peer>, streamId: string) {
    let map = this._receiveStreams.get(ctx.peer)

    let stream = map?.get(streamId)
    const state = ctx.envelope.streamState as 'start' | 'chunk' | 'end' | 'error'
    const isNew = !stream

    if (isNew && state !== 'start') {
      await this._rejectStream(ctx, 'missing_start')
      return
    }

    if (!isNew && state === 'start') {
      await this._rejectStream(ctx, 'duplicate_start')
      return
    }

    if (isNew) {
      let release = () => {}
      try {
        release = this._deps.taskBudget?.reserve(ctx.peer, 'incomingStreams') ?? release
      } catch (error) {
        if (error instanceof TaskCapacityError) {
          this._deps.onTaskError?.(ctx.peer, error, 'incomingStreams')
          this._deps.notifyStreamCancel?.(ctx.peer, streamId)
        }
        throw error
      }
      try {
        stream = this._deps.createReceiveStream(ctx.peer)
      } catch (error) {
        release()
        try {
          this._deps.notifyStreamCancel?.(ctx.peer, streamId)
        } catch {
          /* retain the stream factory error */
        }
        throw error
      }
      if (!map) {
        map = new Map()
        this._receiveStreams.set(ctx.peer, map)
      }
      map.set(streamId, stream)
      const current = stream
      const currentMap = map
      current.onClose(() => {
        release()
        if (currentMap.get(streamId) === current) currentMap.delete(streamId)
        if (!currentMap.size && this._receiveStreams.get(ctx.peer) === currentMap) {
          this._receiveStreams.delete(ctx.peer)
        }
        if (
          !this._disposed &&
          !this._closedPeers.has(ctx.peer) &&
          ctx.peer.state === 'open' &&
          !this._remoteTerminals.has(current)
        ) {
          this._deps.notifyStreamCancel?.(ctx.peer, streamId)
        }
      })
      // Start the consumer handler, but never wait for its entire lifetime in
      // the frame scheduler: it may await chunks that have not arrived yet.
      void this._runRoute(ctx, current).catch(async error => {
        try {
          current.stop('error', error)
        } catch {
          /* retain the handler error */
        }
        try {
          if (this._deps.onStreamError) await this._deps.onStreamError(error, ctx)
          else console.error('[ReceiveDispatcher] stream handler failed', error)
        } catch (observerError) {
          console.error('[ReceiveDispatcher] error observer failed', observerError)
        }
      })
    }

    if (state === 'end' || state === 'error') this._remoteTerminals.add(stream!)
    await stream!.push(ctx.envelope.data, state, ctx.envelope.error)
  }

  async dispatch<Receive = any>(peer: Peer, data: Receive) {
    this._assertOpen(peer)
    let stopped = false

    const adapted = this._protocol.read(data)
    validateRuntimeMessage(adapted)
    const envelope = { ...adapted }
    const ctx = this._createContext(peer, envelope, data)
    const identity = { ...envelope }
    ctx.stop = () => {
      stopped = true
    }

    const priority = envelope.priority

    try {
      await this._tasks.run(peer, undefined, signal => {
        Object.defineProperty(ctx, 'signal', { value: signal, enumerable: true })
        return this._deps.schedulers.inScheduler.schedule({
          type: 'receive',
          peerId: peer.id,
          priority,
          signal,
          run: async () => {
            this._assertOpen(peer)
            signal.throwIfAborted()
            try {
              await this._runReceive(ctx)
              this._assertOpen(peer)
              signal.throwIfAborted()
              assertTaskIdentity(identity, ctx.envelope)
              const task = identity.kind ? 'Request' : 'Stream'
              if (hasTaskIdentity(identity) && ctx.peer !== peer)
                throw new Error(`${task} peer cannot be changed`)
              if (stopped && hasTaskIdentity(identity))
                throw new Error(`${task} message blocked by receive middleware`)
              if (stopped) return
              validateRuntimeMessage(ctx.envelope)
            } catch (error) {
              if (identity.kind)
                this._deps.rejectRequest?.({ ...ctx, peer, envelope: identity }, error)
              else if (identity.streamId) {
                const stream = this._receiveStreams.get(peer)?.get(identity.streamId)
                if (stream) stream.stop('error', error)
                else if (
                  !this._disposed &&
                  !this._closedPeers.has(peer) &&
                  peer.state === 'open'
                ) {
                  this._deps.notifyStreamCancel?.(peer, identity.streamId)
                }
              }
              throw error
            }
            if (ctx.envelope.kind === 'stream_cancel') {
              this._deps.cancelStream?.(peer, ctx.envelope.streamId!)
              return
            }
            if (await this._deps.acceptRequest?.(ctx)) return
            this._assertOpen(peer)
            signal.throwIfAborted()

            if (this._isStreamEnvelope(ctx.envelope)) {
              const reason = this._validateStreamState(ctx.envelope)
              if (reason) {
                await this._rejectStream(ctx, reason)
                return
              }
              await this._runConsumeStream(ctx, ctx.envelope.streamId!)
            } else {
              this._deps.publish?.(ctx)
              this._assertOpen(peer)
              signal.throwIfAborted()
              await this._runRoute(ctx)
            }
          }
        })
      })
    } catch (error) {
      if (error instanceof QueueCapacityError) this._deps.onQueueError?.(peer, error)
      throw error
    }
  }
}
