export type PeerState = 'open' | 'closing' | 'closed'

export interface PeerLike {
  id: string
  state: PeerState
}
