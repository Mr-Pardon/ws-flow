import type { RuntimeMessage } from '../protocol.js'
import { PeerLike, CoreSendMiddleware } from './index.js'

export type SendOptions<Peer extends PeerLike = PeerLike> = {
  signal?: AbortSignal
  priority?: number
  meta?: Record<string, unknown>
  sendMiddlewares?: CoreSendMiddleware<Peer>[]
}

export type SendDispatcherLike<Peer extends PeerLike = PeerLike> = {
  dispatch(
    peer: Peer,
    envelope: RuntimeMessage,
    options?: SendOptions<Peer>,
    /** Final synchronous admission check before committing to transport. */
    onTransportStart?: () => void
  ): Promise<void>
}

export type ReceiveDispatcherLike<Peer extends PeerLike = PeerLike> = {
  dispatch(peer: Peer, data: any): Promise<void>
}
