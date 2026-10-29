import {
  wsFlowAdapter,
  validateRuntimeMessage,
  ProtocolCapabilityError,
  assertTaskIdentity,
  hasTaskIdentity
} from '../protocol.js'
import type { RuntimeMessage, ProtocolAdapter } from '../protocol.js'
import { composeMiddleware } from '@ws-flow/pipeline'
import { matchEvent } from '../utils/matcher.js'
import { PeerTasks } from '../utils/peerTasks.js'
import { QueueCapacityError } from '@ws-flow/scheduler'
import { TransportLimitError } from '../transportLimits.js'
import type {
  PeerLike,
  SendDispatcherDeps,
  CoreSendContext,
  SendOptions
} from '../types/index.js'

export class SendDispatcher<Peer extends PeerLike = PeerLike> {
  private _protocol: ProtocolAdapter
  private _closedPeers = new WeakSet<Peer>()
  private _disposed = false
  private _tasks = new PeerTasks<Peer>()

  constructor(private _deps: SendDispatcherDeps<Peer>) {
    this._protocol = _deps.protocol ?? wsFlowAdapter()
  }

  private _assertOpen(peer: Peer) {
    if (this._disposed || this._closedPeers.has(peer) || peer.state !== 'open')
      throw new Error('Peer is closed')
  }

  closePeer(peer: Peer, reason: unknown = new Error('Peer is closed')) {
    this._closedPeers.add(peer)
    this._tasks.close(peer, reason)
  }

  dispose() {
    this._disposed = true
    this._tasks.dispose()
  }

  private _createContext(
    peer: Peer,
    env: RuntimeMessage,
    raw: RuntimeMessage
  ): CoreSendContext<Peer> {
    return {
      envelope: env,
      raw,
      peer: peer,
      stop: () => {}
    }
  }

  private async _runSend(ctx: CoreSendContext<Peer>, options?: SendOptions<Peer>) {
    const middlewares = this._mergeSendMiddlewares(ctx.envelope, options)
    const pipeline = composeMiddleware<CoreSendContext<Peer>>(middlewares)

    await pipeline(ctx)
  }

  private async _runTransport(
    ctx: CoreSendContext<Peer>,
    signal?: AbortSignal,
    onTransportStart?: () => void
  ) {
    validateRuntimeMessage(ctx.envelope)
    let message: unknown
    if (ctx.envelope.kind === 'stream_cancel') {
      if (!this._protocol.stream?.cancel)
        throw new ProtocolCapabilityError(this._protocol.name, 'stream cancellation')
      message = this._protocol.stream.cancel.write(ctx.envelope)
    } else if (ctx.envelope.kind === 'request_stream') {
      if (!this._protocol.request?.stream)
        throw new ProtocolCapabilityError(this._protocol.name, 'stream requests')
      message = this._protocol.request.stream.write(ctx.envelope)
    } else if (ctx.envelope.kind === 'request_cancel') {
      if (!this._protocol.request?.cancel)
        throw new ProtocolCapabilityError(this._protocol.name, 'request cancellation')
      message = this._protocol.request.cancel.write(ctx.envelope)
    } else if (ctx.envelope.kind !== undefined) {
      if (!this._protocol.request)
        throw new ProtocolCapabilityError(this._protocol.name, 'requests')
      message = this._protocol.request.write(ctx.envelope)
    } else if (ctx.envelope.streamState !== undefined) {
      if (!this._protocol.stream)
        throw new ProtocolCapabilityError(this._protocol.name, 'stream sending')
      message = this._protocol.stream.write(ctx.envelope)
    } else {
      message = this._protocol.write(ctx.envelope)
    }
    signal?.throwIfAborted()
    const transport = this._deps.transport
    if (transport.prepare) {
      const prepared = await transport.prepare(ctx.peer, message, signal)
      let failed = false
      try {
        this._assertOpen(ctx.peer)
        signal?.throwIfAborted()
        onTransportStart?.()
        await prepared.send()
      } catch (error) {
        failed = true
        throw error
      } finally {
        try {
          prepared.release()
        } catch (error) {
          if (!failed) throw error
          console.error('[RuntimeCore] Transport release failed', error)
        }
      }
    } else {
      onTransportStart?.()
      await transport.send(ctx.peer, message)
    }
  }

  private _mergeSendMiddlewares(envelope: RuntimeMessage, options?: SendOptions<Peer>) {
    const event = envelope.event

    const routeMiddlewares = this._deps.pipelines.sendEventMiddlewares
      .filter(({ matcher }) => matchEvent(event, matcher))
      .flatMap(({ middlewares }) => middlewares)

    return [
      ...this._deps.pipelines.sendMiddlewares,
      ...routeMiddlewares,
      ...(options?.sendMiddlewares || [])
    ]
  }

  async dispatch(
    peer: Peer,
    env: RuntimeMessage,
    options?: SendOptions<Peer>,
    onTransportStart?: () => void
  ) {
    this._assertOpen(peer)
    options?.signal?.throwIfAborted()
    let stopped = false

    validateRuntimeMessage(env)
    const envelope = { ...env }
    if (options?.priority !== undefined) envelope.priority = options.priority
    if (options?.meta !== undefined) envelope.meta = options.meta
    validateRuntimeMessage(envelope)
    const priority = envelope.priority

    const ctx = this._createContext(peer, envelope, env)
    const identity = { ...envelope }
    ctx.stop = () => {
      stopped = true
    }

    try {
      await this._tasks.run(peer, options?.signal, signal => {
        const context = { ...ctx, signal }
        return this._deps.schedulers.outScheduler.schedule({
          type: 'send',
          peerId: peer.id,
          priority,
          signal,
          run: async () => {
            this._assertOpen(peer)
            signal.throwIfAborted()
            await this._runSend(context, options)
            this._assertOpen(peer)
            signal.throwIfAborted()
            assertTaskIdentity(identity, context.envelope)
            const task = identity.kind ? 'Request' : 'Stream'
            if (hasTaskIdentity(identity) && context.peer !== peer)
              throw new Error(`${task} peer cannot be changed`)
            if (stopped && hasTaskIdentity(identity))
              throw new Error(`${task} message blocked by send middleware`)
            if (stopped) return

            await this._runTransport(context, signal, onTransportStart)
          }
        })
      })
    } catch (error) {
      if (error instanceof QueueCapacityError)
        this._deps.onQueueError?.(peer, error, hasTaskIdentity(identity))
      if (error instanceof TransportLimitError)
        this._deps.onTransportLimit?.(peer, error, hasTaskIdentity(identity))
      throw error
    }
  }
}
