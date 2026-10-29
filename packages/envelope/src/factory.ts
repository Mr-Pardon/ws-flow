import type { AnyRecord, EnvelopeBuilderLike, EnvelopeBuilderPlugin } from './types.js'
import { EnvelopeBuilder } from './envelope.js'

export function createEnvelopeBuilder<Events extends AnyRecord = AnyRecord>(
  input?: Partial<EnvelopeBuilderLike> | EnvelopeBuilderPlugin[]
) {
  let impl = createDefaultEnvelopeShape()

  if (Array.isArray(input)) {
    // plugin 模式
    for (const plugin of input) {
      impl = plugin(impl)
    }
  } else if (input) {
    // overrides 模式
    impl = {
      ...impl,
      ...input
    }
  }

  return new EnvelopeBuilder<Events>(impl)
}

function createDefaultEnvelopeShape(): EnvelopeBuilderLike {
  let msgIdSeed = 0
  let streamIdSeed = 0

  return {
    createMessage({ id, event, data, meta, priority }) {
      return {
        id: id ?? `msg_${msgIdSeed++}`,
        event,
        data,
        meta,
        priority
      }
    },

    createStreamStart({ id, event, meta, priority }) {
      return {
        id,
        event,
        streamId: `stream_${streamIdSeed++}`,
        streamState: 'start',
        meta,
        priority
      }
    },

    createStreamChunk({ id, streamId, data }) {
      return {
        id,
        streamId,
        streamState: 'chunk',
        data
      }
    },

    createStreamEnd({ id, streamId }) {
      return {
        id,
        streamId,
        streamState: 'end'
      }
    },

    createStreamError({ id, streamId, error }) {
      return {
        id,
        streamId,
        streamState: 'error',
        error
      }
    }
  }
}
