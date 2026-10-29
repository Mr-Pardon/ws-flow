import type { WSEnvelope } from '@ws-flow/envelope'
import type { PeerLike } from './index.js'

export type InvalidStreamReason =
  | 'missing_state'
  | 'invalid_state'
  | 'missing_start'
  | 'duplicate_start'

export interface InvalidStreamInfo<
  Peer extends PeerLike = PeerLike,
  Envelope = WSEnvelope
> {
  reason: InvalidStreamReason
  envelope: Envelope
  peer: Peer
  message: string
}

export type InvalidStreamObserver<
  Peer extends PeerLike = PeerLike,
  Envelope = WSEnvelope
> = (info: InvalidStreamInfo<Peer, Envelope>) => void | Promise<void>
