import test from 'node:test'
import assert from 'node:assert/strict'
import {
  RuntimeCore,
  genericAdapter,
  wsFlowAdapter,
  ProtocolCapabilityError
} from '../packages/runtime-core/dist/index.js'
import { createEnvelopeBuilder } from '../packages/envelope/dist/index.js'

const peer = { id: 'peer', state: 'open' }

function runtime(protocol, extra = {}) {
  const sent = []
  const core = new RuntimeCore({
    base: {
      protocol,
      transport: {
        send: async (_, message) => {
          sent.push(message)
        }
      },
      ...extra
    }
  })
  return { core, sent }
}

test('generic messages retain reserved-looking fields without activating protocol behavior', async () => {
  const { core, sent } = runtime(genericAdapter())
  const input = {
    event: 123,
    streamId: 'business-id',
    priority: 'urgent',
    data: { value: 1 }
  }
  let observed
  core.pipelines.useReceive(ctx => {
    observed = ctx
  })
  await core.dispatchers.receive.dispatch(peer, input)
  assert.equal(observed.raw, input)
  assert.equal(observed.envelope.data, input)
  assert.equal(observed.envelope.streamId, undefined)
  await core.send(peer, input, { priority: 5, meta: { trace: 'local' } })
  assert.equal(sent[0], input)
  assert.deepEqual(input, {
    event: 123,
    streamId: 'business-id',
    priority: 'urgent',
    data: { value: 1 }
  })
})

test('generic adapter preserves primitives and binary messages', async () => {
  const { core, sent } = runtime(genericAdapter())
  const values = ['text', 42, null, new Uint8Array([1, 2, 3])]
  for (const value of values) {
    await core.send(peer, value)
    assert.equal(sent.at(-1), value)
  }
})

test('generic event extraction and explicit event encoding are independent', async () => {
  const { core, sent } = runtime(
    genericAdapter({
      event: input => input.type,
      writeEvent: (event, data) => ({ action: event, payload: data })
    })
  )
  let seen
  core.pipelines.useReceiveEvent({
    matcher: 'progress',
    middlewares: ctx => {
      seen = ctx.envelope.data
    }
  })
  const incoming = { type: 'progress', percent: 60 }
  await core.dispatchers.receive.dispatch(peer, incoming)
  assert.equal(seen, incoming)
  await core.emit(peer, 'start', { job: 42 })
  assert.deepEqual(sent, [{ action: 'start', payload: { job: 42 } }])
})

test('unsupported event and stream sends fail without touching transport', async () => {
  const { core, sent } = runtime(genericAdapter({ event: input => input.type }))
  await assert.rejects(core.emit(peer, 'start', {}), ProtocolCapabilityError)
  assert.throws(() => core.createSendStream(peer, 'chunks'), ProtocolCapabilityError)
  await assert.rejects(
    core.dispatchers.send.dispatch(peer, { streamId: 'x', streamState: 'start' }),
    ProtocolCapabilityError
  )
  assert.deepEqual(sent, [])
})

test('standard adapter preserves plain envelopes and send options only override explicit fields', async () => {
  const { core, sent } = runtime(wsFlowAdapter())
  const input = { id: 'm1', data: 42, priority: 9, meta: { trace: 'original' } }
  let observed
  core.pipelines.useReceive(ctx => {
    observed = ctx.envelope
  })
  await core.dispatchers.receive.dispatch(peer, input)
  assert.deepEqual(observed, input)
  await core.dispatchers.send.dispatch(peer, input)
  assert.deepEqual(sent[0], input)
  await core.dispatchers.send.dispatch(peer, input, { priority: 0 })
  assert.deepEqual(sent[1], { ...input, priority: 0 })
  assert.equal(input.priority, 9)
  await core.send(peer, { event: 'business', data: 123 })
  assert.deepEqual(sent[2], { data: { event: 'business', data: 123 } })
})

test('adapter exceptions reject dispatch and never send malformed data', async () => {
  const { core, sent } = runtime({
    name: 'broken',
    read() {
      throw new Error('read failed')
    },
    write() {
      throw new Error('write failed')
    }
  })
  await assert.rejects(core.dispatchers.receive.dispatch(peer, {}), /read failed/)
  await assert.rejects(core.send(peer, {}), /write failed/)
  assert.deepEqual(sent, [])
})

test('internal message validation applies to adapters and middleware replacements', async () => {
  const { core, sent } = runtime(wsFlowAdapter())
  await assert.rejects(
    core.dispatchers.receive.dispatch(peer, { streamId: 'x' }),
    /streamState/
  )
  await assert.rejects(
    core.dispatchers.receive.dispatch(peer, { priority: 'urgent' }),
    /priority/
  )
  const invalid = runtime({ name: 'invalid', read: () => null, write: value => value })
  await assert.rejects(
    invalid.core.dispatchers.receive.dispatch(peer, {}),
    /RuntimeMessage/
  )
  core.pipelines.useSend(ctx => {
    ctx.envelope = { streamState: 'chunk' }
  })
  // Middleware cannot introduce stream control fields into an ordinary message.
  await assert.rejects(core.send(peer, 1), /Stream control field streamState/)
  assert.deepEqual(sent, [])
})

test('receive envelope replacement changes routing while preserving original input', async () => {
  const { core } = runtime(genericAdapter())
  const input = { route: 'before', value: 1 }
  core.pipelines.useReceive(ctx => {
    ctx.envelope = { event: 'after', data: 2 }
  })
  let seen
  core.pipelines.useReceiveEvent({
    matcher: 'after',
    middlewares: ctx => {
      seen = ctx
    }
  })
  await core.dispatchers.receive.dispatch(peer, input)
  assert.equal(seen.envelope.data, 2)
  assert.equal(seen.raw, input)
})

test('custom protocol translates generated stream frames instead of leaking standard wire fields', async () => {
  const { core, sent } = runtime({
    name: 'custom-stream',
    stream: {
      write: message => ({
        channel: message.streamId,
        phase: message.streamState,
        payload: message.data
      })
    },
    read: input => ({ data: input }),
    write: message => message.data
  })
  await core.createSendStream(peer, 'chunks').runGenerator(
    (async function* () {
      yield 7
    })()
  )
  assert.deepEqual(
    sent.map(frame => frame.phase),
    ['start', 'chunk', 'end']
  )
  assert.equal(sent[1].payload, 7)
  assert.equal(
    sent.some(frame => 'streamId' in frame),
    false
  )
})

test('legacy builder remains effective when no explicit adapter is supplied', async () => {
  const builder = createEnvelopeBuilder({
    createStreamStart: () => ({ streamId: 'legacy', streamState: 'start' })
  })
  const { core, sent } = runtime(undefined, { builder: { envelope: builder } })
  await core.createSendStream(peer).runGenerator(
    (async function* () {
      yield 1
    })()
  )
  assert.equal(sent[0].streamId, 'legacy')
})

test('adapter reuse does not allocate IDs and ordinary writes never handle stream frames', async () => {
  let ordinaryWrites = 0
  const adapter = {
    name: 'external',
    read: data => ({ data }),
    write: message => {
      ordinaryWrites++
      return message.data
    },
    stream: { write: frame => ({ key: frame.streamId, phase: frame.streamState }) }
  }
  const builder = createEnvelopeBuilder({
    createStreamStart: () => ({ streamId: 'owned-by-runtime', streamState: 'start' })
  })
  const { core, sent } = runtime(adapter, { builder: { envelope: builder } })
  await core.createSendStream(peer).runGenerator(
    (async function* () {
      yield 1
    })()
  )
  assert.equal(sent[0].key, 'owned-by-runtime')
  assert.equal(ordinaryWrites, 0)
  await core.send(peer, 'hello')
  assert.equal(ordinaryWrites, 1)
})
