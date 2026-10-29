import {
  PeerLike,
  SendOptions,
  CoreSendMiddleware,
  CoreStreamMiddleware
} from './index.js'

export interface ReceiveStreamLike<Data = any> extends AsyncIterableIterator<Data> {
  push(
    data: Data,
    state: 'start' | 'chunk' | 'end' | 'error',
    error?: unknown
  ): Promise<void>
  isClosed(): boolean
  stop(state: 'end' | 'error', reason?: unknown): void
  onClose(handler: () => void): () => void
  return(): Promise<IteratorResult<Data>>
  onData(handler: (data: Data) => void): void
}

export type ReceiveStreamRecord = Map<string, ReceiveStreamLike<any>>

export interface SendStreamLike {
  cancel(reason?: unknown): void
  isClosed(): boolean
  emit(
    state: 'start' | 'chunk' | 'end' | 'error',
    data?: any,
    err?: unknown
  ): Promise<void>
  runGenerator(gen: AsyncGenerator<any>): Promise<void>
}

export interface SendStreamOptions<Peer extends PeerLike = PeerLike>
  extends SendOptions<Peer> {
  /** Overrides runtime streamIdleMs for this producer; null disables expiry. */
  idleTimeoutMs?: number | null
  streamMiddlewares?: CoreStreamMiddleware<Peer>[]
}
