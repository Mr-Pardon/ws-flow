import type {
  AnyRecord,
  EnvelopeBuilderLike,
  EventKey,
  EventMessageInput,
  EventStreamStartInput,
  PlainMessageInput,
  PlainStreamStartInput,
  WSEnvelope
} from './types.js'

export class EnvelopeBuilder<Events extends AnyRecord = AnyRecord>
  implements EnvelopeBuilderLike
{
  constructor(private _impl: EnvelopeBuilderLike) {}

  createMessage<K extends EventKey<Events>>(
    input: EventMessageInput<Events, K>
  ): WSEnvelope<Events, K>

  createMessage(input: PlainMessageInput): WSEnvelope<Events>

  createMessage(input: any) {
    return this._impl.createMessage(input) as WSEnvelope<Events>
  }

  createStreamStart<K extends EventKey<Events>>(
    input: EventStreamStartInput<Events, K>
  ): WSEnvelope<Events, K>

  createStreamStart(input: PlainStreamStartInput): WSEnvelope<Events>

  createStreamStart(input: any) {
    return this._impl.createStreamStart(input) as WSEnvelope<Events>
  }

  createStreamChunk(input: any) {
    return this._impl.createStreamChunk(input) as WSEnvelope<Events>
  }

  createStreamEnd(input: any) {
    return this._impl.createStreamEnd(input) as WSEnvelope<Events>
  }

  createStreamError(input: any) {
    return this._impl.createStreamError(input) as WSEnvelope<Events>
  }
}
