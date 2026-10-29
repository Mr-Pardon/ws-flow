/** Internal processing information, not a required wire format. */
export interface RuntimeMessage {
  kind?:
    | 'request'
    | 'response'
    | 'request_error'
    | 'request_cancel'
    | 'request_stream'
    | 'stream_cancel'
  resultMode?: 'single' | 'stream'
  requestId?: string
  id?: string
  event?: string
  data?: unknown
  priority?: number
  meta?: Record<string, unknown>
  streamId?: string
  streamState?: 'start' | 'chunk' | 'end' | 'error'
  error?: unknown
}

/** Operates on decoded messages. Codec and transport belong to the host. */
export interface ProtocolAdapter {
  readonly name: string
  read(input: unknown): RuntimeMessage
  write(message: RuntimeMessage): unknown
  /** Optional wire encoding only; the runtime owns IDs and lifecycle. */
  readonly stream?: {
    write(frame: RuntimeMessage): unknown
    cancel?: { write(frame: RuntimeMessage): unknown }
  }
  readonly request?: {
    write(frame: RuntimeMessage): unknown
    /** Opt-in: supporting requests does not imply a remote cancellation command. */
    cancel?: { write(frame: RuntimeMessage): unknown }
    stream?: { write(frame: RuntimeMessage): unknown }
  }
}

export class ProtocolCapabilityError extends Error {
  constructor(adapter: string, capability: string) {
    super(`Protocol adapter "${adapter}" does not support ${capability}`)
    this.name = 'ProtocolCapabilityError'
  }
}

export function validateRuntimeMessage(message: RuntimeMessage): void {
  if (!message || typeof message !== 'object' || Array.isArray(message)) {
    throw new TypeError('Adapter must return a RuntimeMessage object')
  }
  for (const key of ['id', 'event'] as const) {
    if (message[key] !== undefined && typeof message[key] !== 'string') {
      throw new TypeError(`${key} must be a string`)
    }
  }
  if (message.priority !== undefined && !Number.isFinite(message.priority)) {
    throw new TypeError('priority must be a finite number')
  }
  if (
    message.meta !== undefined &&
    (!message.meta || typeof message.meta !== 'object' || Array.isArray(message.meta))
  ) {
    throw new TypeError('meta must be an object')
  }
  if (message.streamId !== undefined || message.streamState !== undefined) {
    if (typeof message.streamId !== 'string' || !message.streamId) {
      throw new TypeError('Stream frames require a nonempty streamId')
    }
    if (
      message.kind !== 'stream_cancel' &&
      !['start', 'chunk', 'end', 'error'].includes(message.streamState ?? '')
    ) {
      throw new TypeError('Stream frames require a valid streamState')
    }
  }
  if (message.kind === 'stream_cancel') {
    if (
      !message.streamId ||
      message.streamState !== undefined ||
      message.requestId !== undefined ||
      message.resultMode !== undefined
    ) {
      throw new TypeError('stream_cancel requires only a streamId as task identity')
    }
    return
  }
  if (
    message.resultMode !== undefined &&
    (message.kind !== 'request' || !['single', 'stream'].includes(message.resultMode))
  ) {
    throw new TypeError('resultMode belongs to requests and must be single or stream')
  }
  if (message.kind !== undefined || message.requestId !== undefined) {
    if (
      ![
        'request',
        'response',
        'request_error',
        'request_cancel',
        'request_stream'
      ].includes(message.kind ?? '') ||
      typeof message.requestId !== 'string' ||
      !message.requestId
    ) {
      throw new TypeError('Request messages require a valid kind and nonempty requestId')
    }
    if (message.kind === 'request_stream') {
      if (!message.streamId || !message.streamState)
        throw new TypeError('Requested stream frames require streamId and streamState')
    } else if (message.streamId !== undefined)
      throw new TypeError('Only request_stream messages may carry stream frames')
    if (message.kind === 'request' && !message.event)
      throw new TypeError('Requests require a nonempty event')
    if (
      message.kind === 'request_error' ||
      (message.kind === 'request_stream' && message.streamState === 'error')
    ) {
      const error = message.error as { code?: unknown; message?: unknown } | undefined
      if (!error || typeof error.code !== 'string' || typeof error.message !== 'string') {
        throw new TypeError('Request errors require code and message')
      }
    }
  }
}

/** Middleware may transform payloads, but may not redirect task ownership. */
export function hasTaskIdentity(message: RuntimeMessage) {
  return ['kind', 'requestId', 'resultMode', 'streamId', 'streamState'].some(
    key => message[key as keyof RuntimeMessage] !== undefined
  )
}

export function assertTaskIdentity(before: RuntimeMessage, after: RuntimeMessage) {
  if (hasTaskIdentity(before) || hasTaskIdentity(after)) {
    const task = before.kind || after.kind ? 'Request' : 'Stream'
    for (const key of [
      'kind',
      'requestId',
      'event',
      'resultMode',
      'streamId',
      'streamState'
    ] as const) {
      if (before[key] !== after[key])
        throw new Error(`${task} control field ${key} cannot be changed`)
    }
  }
}

/** Standard ws-flow compatibility adapter; plain data is also accepted inbound. */
export function wsFlowAdapter(): ProtocolAdapter {
  return {
    name: 'ws-flow',
    stream: {
      write: frame => ({
        ...frame,
        ...(frame.error instanceof Error
          ? { error: { code: 'STREAM_ERROR', message: frame.error.message } }
          : {})
      }),
      cancel: { write: frame => ({ ...frame }) }
    },
    request: {
      write: frame => ({ ...frame }),
      cancel: { write: frame => ({ ...frame }) },
      stream: { write: frame => ({ ...frame }) }
    },
    read(input) {
      const fields = [
        'id',
        'event',
        'data',
        'meta',
        'priority',
        'streamId',
        'streamState',
        'error',
        'kind',
        'requestId',
        'resultMode'
      ]
      const isEnvelope =
        input !== null &&
        typeof input === 'object' &&
        !Array.isArray(input) &&
        fields.some(key => Object.prototype.hasOwnProperty.call(input, key))
      const message: RuntimeMessage = isEnvelope ? { ...input } : { data: input }
      validateRuntimeMessage(message)
      if (
        !message.kind &&
        message.streamState === 'error' &&
        !(message.error instanceof Error)
      ) {
        const error = message.error as { code?: string; message?: string } | undefined
        if (typeof error?.message === 'string')
          message.error = Object.assign(new Error(error.message), { code: error.code })
      }
      return message
    },
    write(message) {
      validateRuntimeMessage(message)
      return { ...message }
    }
  }
}

export interface GenericAdapterOptions {
  /** Optional event extraction; original decoded data remains in message.data. */
  event?: (input: unknown) => string | undefined
  /** Explicitly creates the remote service's outbound event shape. */
  writeEvent?: (event: string, data: unknown) => unknown
}

export function genericAdapter(options: GenericAdapterOptions = {}): ProtocolAdapter {
  const { event, writeEvent } = options
  return {
    name: 'generic',
    read(input) {
      return { data: input, event: event?.(input) }
    },
    write(message) {
      if (message.streamId !== undefined || message.streamState !== undefined) {
        throw new ProtocolCapabilityError('generic', 'streams')
      }
      if (message.event !== undefined) {
        if (!writeEvent) throw new ProtocolCapabilityError('generic', 'event sending')
        return writeEvent(message.event, message.data)
      }
      return message.data
    }
  }
}
