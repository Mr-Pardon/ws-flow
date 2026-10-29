/** Limits count encoded wire bytes, including work already running. */
export interface ByteLimits {
  maxMessageBytes?: number
  maxPendingBytes?: number
  maxPendingBytesPerPeer?: number
}

export interface SendTransportLimits extends ByteLimits {
  highWaterMark?: number
  lowWaterMark?: number
  waitTimeoutMs?: number
  pollIntervalMs?: number
}

export type TransportLimitCode =
  | 'MESSAGE_TOO_LARGE'
  | 'PENDING_BYTES_EXCEEDED'
  | 'SEND_BUFFER_TIMEOUT'
  | 'INVALID_BUFFERED_AMOUNT'
  | 'UNSUPPORTED_WIRE_DATA'

export class TransportLimitError extends Error {
  constructor(
    readonly code: TransportLimitCode,
    message: string,
    readonly limit?: number,
    readonly actual?: number,
    readonly scope?: 'message' | 'global' | 'peer'
  ) {
    super(message)
    this.name = 'TransportLimitError'
  }
}

const MAX_TIMER_MS = 2 ** 31 - 1

function positiveInteger(value: unknown, name: string, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || (value as number) < 1 || (value as number) > max)
    throw new RangeError(`${name} must be a positive safe integer no greater than ${max}`)
}

function validBytes(bytes: number) {
  if (!Number.isSafeInteger(bytes) || bytes < 0)
    throw new RangeError('Wire byte length must be a nonnegative safe integer')
}

function validateByteLimits(limits: ByteLimits = {}): ByteLimits {
  if (!limits || typeof limits !== 'object' || Array.isArray(limits))
    throw new TypeError('Byte limits must be an object')
  const copy = { ...limits }
  for (const name of [
    'maxMessageBytes',
    'maxPendingBytes',
    'maxPendingBytesPerPeer'
  ] as const) {
    if (copy[name] !== undefined) positiveInteger(copy[name], name)
  }
  return copy
}

/** Validate at construction time; omitted limits retain existing unbounded behavior. */
export function validateSendLimits(
  limits: SendTransportLimits = {}
): SendTransportLimits {
  const copy = validateByteLimits(limits) as SendTransportLimits
  const { highWaterMark, lowWaterMark, waitTimeoutMs, pollIntervalMs } = copy
  if (highWaterMark === undefined) {
    if (
      lowWaterMark !== undefined ||
      waitTimeoutMs !== undefined ||
      pollIntervalMs !== undefined
    ) {
      throw new RangeError('Send water mark settings require highWaterMark')
    }
    return copy
  }
  positiveInteger(highWaterMark, 'highWaterMark')
  copy.lowWaterMark =
    lowWaterMark === undefined ? Math.floor(highWaterMark / 2) : lowWaterMark
  if (
    !Number.isSafeInteger(copy.lowWaterMark) ||
    copy.lowWaterMark < 0 ||
    copy.lowWaterMark >= highWaterMark
  ) {
    throw new RangeError(
      'lowWaterMark must be a nonnegative safe integer below highWaterMark'
    )
  }
  copy.waitTimeoutMs = waitTimeoutMs === undefined ? 10_000 : waitTimeoutMs
  copy.pollIntervalMs = pollIntervalMs === undefined ? 10 : pollIntervalMs
  positiveInteger(copy.waitTimeoutMs, 'waitTimeoutMs', MAX_TIMER_MS)
  positiveInteger(copy.pollIntervalMs, 'pollIntervalMs', MAX_TIMER_MS)
  return copy
}

/** Exact UTF-8 count without allocating an encoded copy of the entire string. */
export function wireByteLength(raw: unknown): number {
  if (typeof raw === 'string') {
    let bytes = 0
    for (let index = 0; index < raw.length; index++) {
      const code = raw.charCodeAt(index)
      if (code < 0x80) bytes++
      else if (code < 0x800) bytes += 2
      else if (code >= 0xd800 && code <= 0xdbff && index + 1 < raw.length) {
        const next = raw.charCodeAt(index + 1)
        if (next >= 0xdc00 && next <= 0xdfff) {
          bytes += 4
          index++
        } else bytes += 3
      } else bytes += 3
    }
    return bytes
  }
  if (raw instanceof ArrayBuffer || ArrayBuffer.isView(raw)) return raw.byteLength
  if (typeof Blob !== 'undefined' && raw instanceof Blob) return raw.size
  if (Array.isArray(raw) && raw.every(ArrayBuffer.isView)) {
    const bytes = raw.reduce((total, part) => total + wireByteLength(part), 0)
    validBytes(bytes)
    return bytes
  }
  throw new TransportLimitError(
    'UNSUPPORTED_WIRE_DATA',
    'Byte limits require a string, ArrayBuffer, buffer view, Blob, or array of buffer views'
  )
}

type PeerBudget = { bytes: number; reservations: number }

/** Reservations cover queued and running operations until their release callback runs. */
export class ByteBudget<Peer extends object = object> {
  private readonly _limits: ByteLimits
  private readonly _peers = new Map<Peer, PeerBudget>()
  private _pendingBytes = 0

  constructor(limits?: ByteLimits) {
    this._limits = validateByteLimits(limits)
  }

  get enabled() {
    return (
      this._limits.maxMessageBytes !== undefined ||
      this._limits.maxPendingBytes !== undefined ||
      this._limits.maxPendingBytesPerPeer !== undefined
    )
  }

  get pendingBytes() {
    return this._pendingBytes
  }

  getPendingBytes(peer: Peer) {
    return this._peers.get(peer)?.bytes ?? 0
  }

  reserve(peer: Peer, bytes: number): () => void {
    validBytes(bytes)
    const { maxMessageBytes, maxPendingBytes, maxPendingBytesPerPeer } = this._limits
    if (maxMessageBytes !== undefined && bytes > maxMessageBytes) {
      throw new TransportLimitError(
        'MESSAGE_TOO_LARGE',
        'Message exceeds maxMessageBytes',
        maxMessageBytes,
        bytes,
        'message'
      )
    }
    const record = this._peers.get(peer) ?? { bytes: 0, reservations: 0 }
    const globalLimit = maxPendingBytes ?? Number.MAX_SAFE_INTEGER
    if (bytes > globalLimit - this._pendingBytes) {
      throw new TransportLimitError(
        'PENDING_BYTES_EXCEEDED',
        'Pending wire bytes exceed the global limit',
        globalLimit,
        this._pendingBytes + bytes,
        'global'
      )
    }
    const peerLimit = maxPendingBytesPerPeer ?? Number.MAX_SAFE_INTEGER
    if (bytes > peerLimit - record.bytes) {
      throw new TransportLimitError(
        'PENDING_BYTES_EXCEEDED',
        'Pending wire bytes exceed the peer limit',
        peerLimit,
        record.bytes + bytes,
        'peer'
      )
    }
    record.bytes += bytes
    record.reservations++
    this._pendingBytes += bytes
    this._peers.set(peer, record)
    let released = false
    return () => {
      if (released) return
      released = true
      if (this._peers.get(peer) !== record) return
      record.bytes -= bytes
      record.reservations--
      this._pendingBytes -= bytes
      if (record.reservations === 0) this._peers.delete(peer)
    }
  }

  closePeer(peer: Peer) {
    const record = this._peers.get(peer)
    if (!record) return
    this._pendingBytes -= record.bytes
    this._peers.delete(peer)
  }

  clear() {
    this._peers.clear()
    this._pendingBytes = 0
  }
}

function abortReason(signal: AbortSignal) {
  return signal.reason ?? new DOMException('The operation was aborted', 'AbortError')
}

/** Wait for socket capacity; callers must perform their send directly after awaiting it. */
export async function waitForWritable(
  socket: { readyState: number; bufferedAmount?: number },
  bytes: number,
  limits: SendTransportLimits,
  signal?: AbortSignal
): Promise<void> {
  const normalized = validateSendLimits(limits)
  validBytes(bytes)
  if (signal?.aborted) throw abortReason(signal)
  const { highWaterMark, lowWaterMark, waitTimeoutMs, pollIntervalMs } = normalized
  if (highWaterMark === undefined) return
  if (bytes > highWaterMark) {
    throw new TransportLimitError(
      'MESSAGE_TOO_LARGE',
      'Message exceeds the send highWaterMark',
      highWaterMark,
      bytes,
      'message'
    )
  }
  await new Promise<void>((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined
    let settled = false
    let blocked = false
    const deadline = performance.now() + waitTimeoutMs!
    const finish = (error?: unknown) => {
      if (settled) return
      settled = true
      if (timer !== undefined) clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
      if (error === undefined) resolve()
      else reject(error)
    }
    const onAbort = () => finish(abortReason(signal!))
    const check = () => {
      try {
        if (signal?.aborted) return onAbort()
        if (socket.readyState !== 1) throw new Error('WebSocket is not open')
        const amount = socket.bufferedAmount
        if (typeof amount !== 'number' || !Number.isFinite(amount) || amount < 0) {
          throw new TransportLimitError(
            'INVALID_BUFFERED_AMOUNT',
            'Send water marks require a finite nonnegative socket.bufferedAmount',
            undefined,
            amount
          )
        }
        const remaining = deadline - performance.now()
        if (remaining <= 0) {
          throw new TransportLimitError(
            'SEND_BUFFER_TIMEOUT',
            'Timed out waiting for the WebSocket send buffer to drain',
            highWaterMark,
            amount
          )
        }
        if (amount <= highWaterMark - bytes && (!blocked || amount <= lowWaterMark!)) {
          finish()
          return
        }
        blocked = true
        timer = setTimeout(check, Math.min(pollIntervalMs!, Math.max(1, remaining)))
      } catch (error) {
        finish(error)
      }
    }
    signal?.addEventListener('abort', onAbort, { once: true })
    check()
  })
}
