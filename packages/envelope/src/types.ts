export type AnyRecord = Record<string, any>

export type BaseWSEnvelope = {
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

  streamId?: string
  streamState?: 'start' | 'chunk' | 'end' | 'error'

  priority?: number
  meta?: Record<string, any>

  error?: unknown
}

export type EventKey<Events extends AnyRecord> = Extract<keyof Events, string>

export type EventWSEnvelope<
  Events extends AnyRecord = AnyRecord,
  K extends EventKey<Events> = EventKey<Events>
> = Omit<BaseWSEnvelope, 'event' | 'data'> & {
  event: K
  data?: Events[K]
}

export type PlainWSEnvelope = Omit<BaseWSEnvelope, 'event'> & {
  event?: undefined
}

export type WSEnvelope<
  Events extends AnyRecord = AnyRecord,
  K extends EventKey<Events> = EventKey<Events>
> = PlainWSEnvelope | EventWSEnvelope<Events, K>

export type BaseMessageInput = {
  id?: string
  data?: unknown
  meta?: Record<string, any>
  priority?: number
}

export type EventMessageInput<
  Events extends AnyRecord = AnyRecord,
  K extends EventKey<Events> = EventKey<Events>
> = BaseMessageInput & {
  event: K
  data?: Events[K]
}

export type PlainMessageInput = BaseMessageInput & {
  event?: undefined
}

export type BaseStreamStartInput = {
  id?: string
  meta?: Record<string, any>
  priority?: number
}

export type EventStreamStartInput<
  Events extends AnyRecord = AnyRecord,
  K extends EventKey<Events> = EventKey<Events>
> = BaseStreamStartInput & {
  event: K
}

export type PlainStreamStartInput = BaseStreamStartInput & {
  event?: undefined
}

export type EnvelopeBuilderLike = {
  createMessage: (input: any) => BaseWSEnvelope
  createStreamStart: (input: any) => BaseWSEnvelope
  createStreamChunk: (input: any) => BaseWSEnvelope
  createStreamEnd: (input: any) => BaseWSEnvelope
  createStreamError: (input: any) => BaseWSEnvelope
}

export type EnvelopeBuilderPlugin = (base: EnvelopeBuilderLike) => EnvelopeBuilderLike

export type EnvelopeBuilderInput = Partial<EnvelopeBuilderLike> | EnvelopeBuilderPlugin[]
