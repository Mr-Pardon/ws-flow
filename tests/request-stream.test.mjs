import test from 'node:test'
import assert from 'node:assert/strict'
import {
  RuntimeCore,
  genericAdapter,
  wsFlowAdapter
} from '../packages/runtime-core/dist/index.js'
import { SessionScheduler } from '../packages/scheduler/dist/index.js'

const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}
const make = send =>
  new RuntimeCore({
    base: { transport: { send } },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
  })
const pair = t => {
  const ap = { id: 'p', state: 'open' },
    bp = { id: 'p', state: 'open' },
    frames = []
  const a = make((_, frame) => b.dispatchers.receive.dispatch(bp, frame))
  const b = make((_, frame) => {
    frames.push(frame)
    return a.dispatchers.receive.dispatch(ap, frame)
  })
  t.after(() => {
    a.dispose()
    b.dispose()
  })
  return { a, b, ap, bp, frames }
}
const collect = async task => {
  const values = []
  for await (const value of task) values.push(value)
  return values
}

test(
  'requestStream immediately returns an iterator with ordered transformed chunks',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t)
    b.handleStream('numbers', async function* () {
      yield 1
      yield 2
    })
    b.pipelines.useSendStream(async (ctx, next) => {
      if (ctx.state === 'chunk') ctx.data *= 2
      await next()
    })
    a.pipelines.useReceiveStream(async (ctx, next) => {
      if (ctx.state === 'chunk') ctx.data += 1
      await next()
    })
    const task = a.requestStream(ap, 'numbers', null)
    assert.equal(task.state, 'waiting')
    assert.deepEqual(await collect(task), [3, 5])
    assert.equal(task.state, 'ended')
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'chunk', 'chunk', 'end']
    )
    assert.equal(new Set(frames.map(frame => frame.requestId)).size, 1)
  }
)

test(
  'empty streams, missing handlers and result-mode mismatch settle',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t)
    b.handleStream('empty', async function* () {})
    b.handleRequest('single', () => 1)
    assert.deepEqual(await collect(a.requestStream(ap, 'empty', null)), [])
    await assert.rejects(collect(a.requestStream(ap, 'unknown', null)), {
      code: 'METHOD_NOT_FOUND'
    })
    await assert.rejects(collect(a.requestStream(ap, 'single', null)), {
      code: 'MODE_MISMATCH'
    })
    await assert.rejects(a.request(ap, 'empty', null), { code: 'MODE_MISMATCH' })
    assert.throws(() => b.handleRequest('empty', () => 1), /already registered/)
  }
)

test(
  'failure before start uses request_error and generator failure uses stream error',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t)
    b.handleStream('before', () => {
      throw new Error('before start')
    })
    b.handleStream('during', async function* () {
      yield 1
      throw new Error('during stream')
    })
    await assert.rejects(collect(a.requestStream(ap, 'before', null)), /before start/)
    assert.equal(frames[0].kind, 'request_error')
    const task = a.requestStream(ap, 'during', null)
    assert.equal((await task.next()).value, 1)
    await assert.rejects(task.next(), /during stream/)
    assert.equal(task.state, 'failed')
    assert.equal(frames.at(-1).streamState, 'error')
    assert.equal(frames.at(-1).error.message, 'during stream')
  }
)

test(
  'early iteration break cancels remote signal and eventually returns the generator',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t),
      aborted = deferred(),
      cleaned = deferred(),
      gate = deferred()
    t.after(() => gate.resolve())
    b.handleStream('wait', async function* (ctx) {
      ctx.signal.addEventListener('abort', () => aborted.resolve(), { once: true })
      try {
        yield 1
        await gate.promise
        yield 2
      } finally {
        cleaned.resolve()
      }
    })
    const task = a.requestStream(ap, 'wait', null)
    for await (const value of task) {
      assert.equal(value, 1)
      break
    }
    assert.equal(task.state, 'cancelled')
    await aborted.promise
    gate.resolve()
    await cleaned.promise
    assert.equal((await task.next()).done, true)
  }
)

test(
  'total timeout ends a stream waiting for chunks and notifies producer',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t),
      gate = deferred(),
      aborted = deferred()
    t.after(() => gate.resolve())
    b.handleStream('slow', async function* (ctx) {
      ctx.signal.addEventListener('abort', () => aborted.resolve(), { once: true })
      yield 1
      await gate.promise
    })
    const task = a.requestStream(ap, 'slow', null, { timeoutMs: 30 })
    assert.equal((await task.next()).value, 1)
    await assert.rejects(task.next(), { code: 'TIMEOUT' })
    await aborted.promise
  }
)

test(
  'bounded buffer overflow fails and cancels the producer',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t),
      aborted = deferred()
    b.handleStream('fast', async function* (ctx) {
      ctx.signal.addEventListener('abort', () => aborted.resolve(), { once: true })
      yield 1
      yield 2
      yield 3
    })
    const task = a.requestStream(ap, 'fast', null, { capacity: 1 })
    await aborted.promise
    await assert.rejects(task.next(), /capacity/)
    assert.equal(task.state, 'failed')
  }
)

test('disconnect fails pending reads and cancellation before send produces no request', async t => {
  const frames = [],
    a = make((_, frame) => frames.push(frame)),
    ap = { id: 'p', state: 'open' }
  t.after(() => a.dispose())
  const task = a.requestStream(ap, 'wait', null)
  task.cancel(new Error('cancelled now'))
  await assert.rejects(task.next(), /cancelled now/)
  await a.send(ap, 'barrier')
  assert.deepEqual(frames, [{ data: 'barrier' }])
  const other = a.requestStream(ap, 'wait', null)
  const rejected = assert.rejects(other.next(), /closed/)
  a.closePeer(ap)
  await rejected
})

test('protocol mistakes in stream order fail the task and late frames are ignored', async t => {
  let id
  const a = make((_, frame) => {
      if (frame.kind === 'request') id = frame.requestId
    }),
    p = { id: 'p', state: 'open' }
  t.after(() => a.dispose())
  const task = a.requestStream(p, 'wait', null)
  await a.send(p, 'barrier')
  await a.dispatchers.receive.dispatch(p, {
    kind: 'request_stream',
    requestId: id,
    streamId: 's',
    streamState: 'chunk',
    data: 1
  })
  await assert.rejects(task.next(), /begin with start/)
  await a.dispatchers.receive.dispatch(p, {
    kind: 'request_stream',
    requestId: id,
    streamId: 's',
    streamState: 'end'
  })
})

test('generic protocol rejects unsupported streaming without sending', async () => {
  const runtime = new RuntimeCore({
    base: {
      protocol: genericAdapter(),
      transport: { send: () => assert.fail('unsupported send') }
    }
  })
  const task = runtime.requestStream({ id: 'p', state: 'open' }, 'x', null)
  await assert.rejects(task.next(), /does not support/)
  runtime.dispose()
})

test('concurrent requested streams do not mix chunks', { timeout: 2000 }, async t => {
  const { a, b, ap } = pair(t)
  b.handleStream('echo', async function* (ctx) {
    yield ctx.data
    yield ctx.data
  })
  assert.deepEqual(
    await Promise.all([
      collect(a.requestStream(ap, 'echo', 'A')),
      collect(a.requestStream(ap, 'echo', 'B'))
    ]),
    [
      ['A', 'A'],
      ['B', 'B']
    ]
  )
})

test(
  'early stream middleware termination and failure produce terminal results',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t)
    b.handleStream('values', async function* () {
      yield 1
      yield 2
    })
    b.pipelines.useSendStream(async (ctx, next) => {
      if (ctx.state === 'chunk') ctx.end()
      await next()
    })
    assert.deepEqual(await collect(a.requestStream(ap, 'values', null)), [])
    const other = pair(t)
    other.b.handleStream('values', async function* () {
      yield 1
    })
    other.b.pipelines.useSendStream(ctx => {
      if (ctx.state === 'start') ctx.stop()
    })
    await assert.rejects(
      collect(other.a.requestStream(other.ap, 'values', null)),
      /terminal frame/
    )
  }
)

test(
  'custom requested-stream encoder does not require proactive stream capability',
  { timeout: 2000 },
  async t => {
    const ap = { id: 'a', state: 'open' },
      bp = { id: 'b', state: 'open' },
      wire = []
    const adapter = () => ({
      ...wsFlowAdapter(),
      stream: undefined,
      read: input => input.packet ?? input,
      request: {
        write: frame => ({ packet: frame }),
        stream: { write: frame => ({ packet: frame }) }
      }
    })
    const a = new RuntimeCore({
      base: {
        protocol: adapter(),
        transport: { send: (_, frame) => b.dispatchers.receive.dispatch(bp, frame) }
      }
    })
    const b = new RuntimeCore({
      base: {
        protocol: adapter(),
        transport: {
          send: (_, frame) => {
            wire.push(frame)
            return a.dispatchers.receive.dispatch(ap, frame)
          }
        }
      }
    })
    t.after(() => {
      a.dispose()
      b.dispose()
    })
    b.handleStream('values', async function* () {
      yield 42
    })
    assert.deepEqual(await collect(a.requestStream(ap, 'values', null)), [42])
    assert.equal(
      wire.every(
        frame => Object.keys(frame).length === 1 && frame.packet.kind === 'request_stream'
      ),
      true
    )
  }
)

test(
  'aborting while the handler prepares its generator suppresses start and closes the late source',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t),
      gate = deferred(),
      started = deferred(),
      cleaned = deferred()
    b.handleStream('prepare', async () => {
      started.resolve()
      await gate.promise
      return {
        next: async () => ({ done: true }),
        return: async () => {
          cleaned.resolve()
          return { done: true }
        },
        [Symbol.asyncIterator]() {
          return this
        }
      }
    })
    const task = a.requestStream(ap, 'prepare', null)
    await started.promise
    task.cancel()
    await assert.rejects(task.next(), /cancelled/)
    // Let the incoming cancel pass the receive scheduler before the factory returns.
    await a.send(ap, 'barrier')
    gate.resolve()
    await cleaned.promise
    assert.deepEqual(frames, [])
  }
)
