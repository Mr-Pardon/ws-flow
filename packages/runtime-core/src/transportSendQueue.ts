import { SessionScheduler } from '@ws-flow/scheduler'
import type { PreparedTransportSend } from './types/runtime.js'

/** Serialize a connection's admission check through completion of its actual send. */
export class TransportSendQueue<Peer extends { id: string }> {
  private readonly _scheduler = new SessionScheduler()
  private readonly _keys = new WeakMap<Peer, string>()
  private _sequence = 0

  private _key(peer: Peer) {
    let key = this._keys.get(peer)
    if (key === undefined) {
      key = `transport-${++this._sequence}`
      this._keys.set(peer, key)
    }
    return key
  }

  /** Callers own byte reservations, including cleanup when a queued factory never runs. */
  prepare(
    peer: Peer,
    signal: AbortSignal | undefined,
    factory: () => Promise<PreparedTransportSend>
  ): Promise<PreparedTransportSend> {
    return new Promise((resolve, reject) => {
      let settled = false
      const fail = (error: unknown) => {
        if (settled) return
        settled = true
        signal?.removeEventListener('abort', abort)
        reject(error)
      }
      const abort = () => fail(signal!.reason)
      if (signal?.aborted) {
        abort()
        return
      }
      signal?.addEventListener('abort', abort, { once: true })

      void this._scheduler
        .schedule({
          type: 'send',
          peerId: this._key(peer),
          signal,
          run: async () => {
            signal?.throwIfAborted()
            const prepared = await factory()
            let released = false
            let resume!: () => void
            const held = new Promise<void>(done => {
              resume = done
            })
            const release = () => {
              if (released) return
              released = true
              try {
                prepared.release()
              } finally {
                resume()
              }
            }

            if (signal?.aborted) {
              release()
              signal.throwIfAborted()
            }
            settled = true
            signal?.removeEventListener('abort', abort)
            resolve({
              send: () => {
                if (released) throw new Error('Prepared transport is released')
                signal?.throwIfAborted()
                return prepared.send()
              },
              release
            })
            // Cancellation cannot pretend an already committed socket write has ended.
            // The owner releases this slot in the send operation's finally block.
            await held
          }
        })
        .catch(fail)
    })
  }
}
