import test from 'node:test'
import assert from 'node:assert/strict'
import { RuntimeCore } from '../packages/runtime-core/dist/index.js'
import { SessionScheduler } from '../packages/scheduler/dist/index.js'
import { StreamConsumer } from '../packages/consumer/dist/index.js'

const make = () =>
  new RuntimeCore({
    base: { transport: { send() {} } },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
  })
const frame = (state, data, id = 'shared') => ({
  event: 'numbers',
  streamId: id,
  streamState: state,
  data
})
const bounded = async promise => {
  let timer
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('operation blocked')), 300)
      })
    ])
  } finally {
    clearTimeout(timer)
  }
}

test('same stream ID on two peer objects remains isolated', async () => {
  const core = make()
  const a = { id: 'same', state: 'open' },
    b = { id: 'same', state: 'open' }
  const streams = new Map()
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: ctx => {
      streams.set(ctx.peer, ctx.stream)
    }
  })
  await core.dispatchers.receive.dispatch(a, frame('start'))
  await core.dispatchers.receive.dispatch(b, frame('start'))
  await core.dispatchers.receive.dispatch(a, frame('chunk', 'A'))
  await core.dispatchers.receive.dispatch(b, frame('chunk', 'B'))
  assert.equal(streams.size, 2)
  assert.equal((await streams.get(a).next()).value, 'A')
  assert.equal((await streams.get(b).next()).value, 'B')
  await core.dispatchers.receive.dispatch(a, frame('end'))
  await core.dispatchers.receive.dispatch(b, frame('end'))
})

test('for-await route does not block incoming frames under session serialization', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  const values = []
  let finish
  const done = new Promise(resolve => {
    finish = resolve
  })
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: async ctx => {
      for await (const value of ctx.stream) values.push(value)
      finish()
    }
  })
  await bounded(core.dispatchers.receive.dispatch(peer, frame('start')))
  await core.dispatchers.receive.dispatch(peer, frame('chunk', 1))
  await core.dispatchers.receive.dispatch(peer, frame('end'))
  await bounded(done)
  assert.deepEqual(values, [1])
})

test('remote error reaches pending and future iterator reads', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  let stream
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: ctx => {
      stream = ctx.stream
    }
  })
  await core.dispatchers.receive.dispatch(peer, frame('start'))
  const reason = new Error('remote failure')
  const pending = assert.rejects(stream.next(), error => error === reason)
  await core.dispatchers.receive.dispatch(peer, { ...frame('error'), error: reason })
  await pending
  await assert.rejects(stream.next(), error => error === reason)
})

test('closePeer ends receive streams and scoped subscriptions, refusing late traffic', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  let stream
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: ctx => {
      stream = ctx.stream
    }
  })
  const subscription = core.subscribe(() => true, { peer })
  await core.dispatchers.receive.dispatch(peer, frame('start'))
  const pending = assert.rejects(stream.next(), /closed/)
  core.closePeer(peer)
  core.closePeer(peer)
  await pending
  assert.equal(subscription.closed, true)
  await assert.rejects(
    core.dispatchers.receive.dispatch(peer, frame('chunk', 1)),
    /closed/
  )
  await assert.rejects(core.send(peer, 1), /closed/)
})

test('early iterator return releases the stream without affecting another one', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  const streams = new Map()
  const invalid = []
  core.useInvalidStreamObserver(info => {
    invalid.push(info.reason)
  })
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: ctx => {
      streams.set(ctx.envelope.streamId, ctx.stream)
    }
  })
  await core.dispatchers.receive.dispatch(peer, frame('start', undefined, 'a'))
  await core.dispatchers.receive.dispatch(peer, frame('start', undefined, 'b'))
  await streams.get('a').return()
  await core.dispatchers.receive.dispatch(peer, frame('chunk', 1, 'a'))
  await core.dispatchers.receive.dispatch(peer, frame('chunk', 2, 'b'))
  assert.deepEqual(invalid, ['missing_start'])
  assert.equal((await streams.get('b').next()).value, 2)
  await core.dispatchers.receive.dispatch(peer, frame('end', undefined, 'b'))
})

test('consumer callback mode does not accumulate an unused iterator buffer', () => {
  const consumer = new StreamConsumer({ capacity: 1 })
  let count = 0
  consumer.onData(() => {
    count++
  })
  for (let i = 0; i < 100; i++) consumer.emit({ state: 'chunk', data: i })
  assert.equal(count, 100)
  assert.equal(consumer.bufferedSize, 0)
  assert.equal(consumer.isEnded(), false)
  consumer.stop('end')
})

test('duplicate start preserves the active stream and reports the invalid frame', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  const streams = [],
    invalid = []
  core.useInvalidStreamObserver(info => {
    invalid.push(info.reason)
  })
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: ctx => {
      streams.push(ctx.stream)
    }
  })
  await core.dispatchers.receive.dispatch(peer, frame('start'))
  await core.dispatchers.receive.dispatch(peer, frame('start'))
  await core.dispatchers.receive.dispatch(peer, frame('chunk', 3))
  assert.equal(streams.length, 1)
  assert.deepEqual(invalid, ['duplicate_start'])
  assert.equal((await streams[0].next()).value, 3)
  core.dispose()
})

test('async route failure closes the stream and reaches the error observer', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  const reason = new Error('handler failed')
  let stream, report
  const reported = new Promise(resolve => {
    report = resolve
  })
  core.useStreamErrorObserver(error => {
    report(error)
  })
  core.pipelines.useReceiveEvent({
    matcher: 'numbers',
    middlewares: async ctx => {
      stream = ctx.stream
      await ctx.stream.next()
      throw reason
    }
  })
  await core.dispatchers.receive.dispatch(peer, frame('start'))
  await core.dispatchers.receive.dispatch(peer, frame('chunk', 1))
  assert.equal(await bounded(reported), reason)
  await assert.rejects(stream.next(), error => error === reason)
  core.dispose()
})

test('close during receive middleware prevents delayed delivery and queued frames', async () => {
  const core = make(),
    peer = { id: 'p', state: 'open' }
  let release, entered
  const gate = new Promise(resolve => {
    release = resolve
  })
  const started = new Promise(resolve => {
    entered = resolve
  })
  core.pipelines.useReceive(async (_, next) => {
    entered()
    await gate
    await next()
  })
  const subscription = core.subscribe(() => true)
  const first = assert.rejects(
    core.dispatchers.receive.dispatch(peer, { data: 1 }),
    /closed/
  )
  await started
  const second = assert.rejects(
    core.dispatchers.receive.dispatch(peer, { data: 2 }),
    /closed/
  )
  core.closePeer(peer)
  release()
  await Promise.all([first, second])
  core.dispose()
  assert.deepEqual(await subscription.next(), { value: undefined, done: true })
  assert.throws(() => core.subscribe(() => true), /closed/)
})

test('normal end drains buffered chunks while iterator return discards them', async () => {
  const consumer = new StreamConsumer()
  consumer.emit({ state: 'chunk', data: 1 })
  consumer.stop('end')
  assert.deepEqual(await consumer.next(), { value: 1, done: false })
  assert.equal((await consumer.next()).done, true)
  const cancelled = new StreamConsumer()
  cancelled.emit({ state: 'chunk', data: 2 })
  await cancelled.return()
  assert.equal(cancelled.bufferedSize, 0)
  assert.equal((await cancelled.next()).done, true)
})

test('consumer queue has a finite capacity and fails without discarding silently', async () => {
  const consumer = new StreamConsumer({ capacity: 1 })
  consumer.emit({ state: 'chunk', data: 1 })
  assert.throws(() => consumer.emit({ state: 'chunk', data: 2 }), /capacity/)
  await assert.rejects(consumer.next(), /capacity/)
})
