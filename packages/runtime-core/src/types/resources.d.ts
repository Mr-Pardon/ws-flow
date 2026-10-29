import type { QueueCapacityError } from '@ws-flow/scheduler'
import type { PeerLike } from './peer.js'
import type { TransportLimitError } from '../transportLimits.js'
import type {
  ActiveTaskKind,
  TaskCapacityError,
  TaskTimeoutError
} from '../taskResources.js'

export interface TaskFailure<Peer extends PeerLike = PeerLike> {
  peer: Peer
  error: TaskCapacityError | TaskTimeoutError
  kind: ActiveTaskKind
}

export interface QueueFailure<Peer extends PeerLike = PeerLike> {
  peer: Peer
  error: QueueCapacityError
  direction: 'send' | 'receive'
  /** Runtime has closed this peer; the host must close the underlying transport. */
  fatal: boolean
}

export interface TransportFailure<Peer extends PeerLike = PeerLike> {
  peer: Peer
  error: TransportLimitError
  direction: 'send' | 'receive'
  fatal: boolean
}
