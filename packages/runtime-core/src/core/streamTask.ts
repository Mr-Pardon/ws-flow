import { ReceiveStream } from './receiveStream.js'
import type { RuntimeMessage } from '../protocol.js'
import type { PeerLike, CoreStreamMiddleware } from '../types/index.js'
import { TaskTimeoutError } from '../taskResources.js'

export type StreamTaskState = 'waiting' | 'open' | 'ended' | 'failed' | 'cancelled'

/** One requested result stream; normal end drains buffered chunks. */
export class StreamTask<Chunk = unknown> implements AsyncIterableIterator<Chunk> {
  private _stream: ReceiveStream<any>
  private _state: StreamTaskState = 'waiting'
  private _streamId?: string
  private _remoteTerminal = false
  private _settle?: (error: unknown, failed: boolean, notifyRemote: boolean) => void

  constructor(
    peer: PeerLike,
    middleware: CoreStreamMiddleware<any>[],
    capacity?: number,
    idleTimeoutMs?: number | null,
    onIdleTimeout?: (error: TaskTimeoutError) => void
  ) {
    this._stream = new ReceiveStream(peer, middleware, {
      capacity,
      idleTimeoutMs,
      onIdleTimeout
    })
    this._stream.onState(event => {
      if (event.state === 'end' || event.state === 'error') {
        const localIdleTimeout =
          event.error instanceof TaskTimeoutError &&
          event.error.code === 'STREAM_IDLE_TIMEOUT'
        this.finish(
          event.error,
          event.state === 'error',
          localIdleTimeout || !this._remoteTerminal
        )
      }
    })
  }

  get state() {
    return this._state
  }

  get closed() {
    return !['waiting', 'open'].includes(this._state)
  }

  /** Internal lifetime binding; pending ownership stays in RequestManager. */
  bind(settle: (error: unknown, failed: boolean, notifyRemote: boolean) => void) {
    this._settle = settle
  }

  finish(error?: unknown, failed = false, notifyRemote = false) {
    if (this.closed) return
    this._state = failed ? 'failed' : 'ended'
    const settle = this._settle
    this._settle = undefined
    this._stream.stop(failed ? 'error' : 'end', error)
    settle?.(error, failed, notifyRemote)
  }

  cancel(reason: unknown = new Error('Stream request cancelled')) {
    if (this.closed) return
    this.finish(reason, true, true)
    this._state = 'cancelled'
  }

  async accept(message: RuntimeMessage) {
    if (this.closed) return
    try {
      if (this._state === 'waiting') {
        if (message.streamState !== 'start')
          throw new Error('Requested stream must begin with start')
        this._streamId = message.streamId
        this._state = 'open'
      } else if (message.streamState === 'start' || message.streamId !== this._streamId) {
        throw new Error('Requested stream identity or frame order is invalid')
      }
      this._remoteTerminal =
        message.streamState === 'end' || message.streamState === 'error'
      await this._stream.push(message.data, message.streamState!, message.error)
    } catch (error) {
      this.finish(error, true, true)
    } finally {
      this._remoteTerminal = false
    }
  }

  next(): Promise<IteratorResult<Chunk>> {
    return this._stream.next()
  }

  async return(): Promise<IteratorResult<Chunk>> {
    if (!this.closed) {
      this.finish(undefined, false, true)
      this._state = 'cancelled'
    }
    return this._stream.return()
  }

  [Symbol.asyncIterator]() {
    return this
  }
}
