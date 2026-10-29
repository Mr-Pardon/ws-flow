import test from 'node:test'
import assert from 'node:assert/strict'
import {
  ActiveTaskBudget,
  ReceiveDispatcher,
  ReceiveStream,
  RuntimeCore,
  StreamTask,
  TaskCapacityError,
  TaskTimeoutError
} from '../packages/runtime-core/dist/index.js'
import { SessionScheduler } from '../packages/scheduler/dist/index.js'

const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

const flush = () => new Promise(resolve => setImmediate(resolve))

function clock(t) {
  let now = 0,
    sequence = 0
  const timers = new Map()
  t.mock.method(performance, 'now', () => now)
  t.mock.method(globalThis, 'setTimeout', (callback, delay) => {
    const id = ++sequence
    timers.set(id, { callback, at: now + delay })
    return id
  })
  t.mock.method(globalThis, 'clearTimeout', id => timers.delete(id))
  return {
    jump(ms) {
      now += ms
    },
    async advance(ms) {
      now += ms
      for (const [id, timer] of [...timers]) {
        if (timers.has(id) && timer.at <= now) {
          timers.delete(id)
          timer.callback()
        }
      }
      await flush()
    },
    get pending() {
      return timers.size
    }
  }
}

function setup(t, options = {}) {
  const frames = [],
    streams = [],
    errors = [],
    invalid = []
  const peer = { id: 'one', state: 'open' }
  const core = new RuntimeCore({
    base: { transport: { send: (peer, frame) => frames.push({ peer, frame }) } },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() },
    ...options
  })
  core.useTaskErrorObserver(info => errors.push(info))
  core.useInvalidStreamObserver(info => invalid.push(info))
  core.pipelines.useReceiveEvent({
    matcher: 'feed',
    middlewares: ctx => {
      streams.push({ peer: ctx.peer, id: ctx.envelope.streamId, stream: ctx.stream })
    }
  })
  t.after(() => core.dispose())
  const receive = (id, state, data, target = peer) =>
    core.dispatchers.receive.dispatch(target, {
      streamId: id,
      streamState: state,
      ...(state === 'start' ? { event: 'feed' } : {}),
      data
    })
  return { core, peer, frames, streams, errors, invalid, receive }
}

const idleError = error => {
  assert.ok(error instanceof TaskTimeoutError)
  assert.equal(error.code, 'STREAM_IDLE_TIMEOUT')
  return true
}

test('valid proactive stream progress renews its idle deadline', async t => {
  const timer = clock(t)
  const { receive, streams, frames, errors } = setup(t, {
    taskTimeouts: { streamIdleMs: 10 }
  })
  await receive('s', 'start')
  await timer.advance(8)
  await receive('s', 'chunk', 1)
  assert.equal((await streams[0].stream.next()).value, 1)
  await timer.advance(8)
  assert.equal(streams[0].stream.isClosed(), false)
  await timer.advance(3)
  await assert.rejects(streams[0].stream.next(), idleError)
  assert.equal(errors.length, 1)
  assert.equal(errors[0].kind, 'incomingStreams')
  assert.equal(errors[0].error.code, 'STREAM_IDLE_TIMEOUT')
  assert.equal(frames.filter(({ frame }) => frame.kind === 'stream_cancel').length, 1)
  assert.equal(timer.pending, 0)
})

test('filtered chunks do not renew proactive stream idle time', async t => {
  const timer = clock(t)
  const { core, receive, streams } = setup(t, { taskTimeouts: { streamIdleMs: 10 } })
  core.pipelines.useReceiveStream(ctx => {
    if (ctx.state === 'chunk') ctx.stop()
  })
  await receive('s', 'start')
  await timer.advance(8)
  await receive('s', 'chunk', 'filtered')
  await timer.advance(3)
  await assert.rejects(streams[0].stream.next(), idleError)
})

test('duplicate starts and unrelated notifications cannot keep a stream alive', async t => {
  const timer = clock(t)
  const { core, peer, receive, streams, invalid } = setup(t, {
    taskTimeouts: { streamIdleMs: 10 }
  })
  await receive('s', 'start')
  await timer.advance(8)
  await receive('s', 'start')
  await core.dispatchers.receive.dispatch(peer, { event: 'other', data: 'keepalive' })
  await timer.advance(3)
  await assert.rejects(streams[0].stream.next(), idleError)
  assert.equal(invalid[0].reason, 'duplicate_start')
  assert.equal(streams.length, 1)
})

for (const terminal of ['end', 'error', 'return']) {
  test(`proactive stream ${terminal} clears its idle deadline`, async t => {
    const timer = clock(t)
    const { core, peer, receive, streams, errors } = setup(t, {
      taskTimeouts: { streamIdleMs: 10 }
    })
    await receive('s', 'start')
    if (terminal === 'return') await streams[0].stream.return()
    else
      await core.dispatchers.receive.dispatch(peer, {
        streamId: 's',
        streamState: terminal,
        ...(terminal === 'error' ? { error: { code: 'REMOTE', message: 'remote' } } : {})
      })
    await timer.advance(20)
    assert.equal(timer.pending, 0)
    assert.equal(streams[0].stream.isClosed(), true)
    assert.equal(errors.length, 0)
  })
}

test('incoming stream capacity rejects only the new stream and releases on return', async t => {
  const { peer, receive, streams, frames, errors } = setup(t, {
    taskLimits: { incomingStreams: { maxActivePerPeer: 1 } }
  })
  await receive('first', 'start')
  await receive('first', 'start')
  await assert.rejects(receive('second', 'start'), error => {
    assert.ok(error instanceof TaskCapacityError)
    assert.equal(error.kind, 'incomingStreams')
    return true
  })
  await flush()
  assert.equal(peer.state, 'open')
  assert.equal(streams.length, 1)
  assert.equal(errors.length, 1)
  assert.ok(
    frames.some(
      ({ frame }) => frame.kind === 'stream_cancel' && frame.streamId === 'second'
    )
  )
  await receive('first', 'chunk', 'still active')
  assert.equal((await streams[0].stream.next()).value, 'still active')
  await streams[0].stream.return()
  await receive('second', 'start')
  assert.equal(streams.length, 2)
})

test('incoming stream identities and capacity are isolated per peer', async t => {
  const { core, peer, receive, streams } = setup(t, {
    taskLimits: { maxActive: 2, incomingStreams: { maxActivePerPeer: 1 } }
  })
  const other = { id: 'other', state: 'open' },
    third = { id: 'third', state: 'open' }
  await receive('same-id', 'start')
  await receive('same-id', 'start', undefined, other)
  await assert.rejects(
    receive('third-stream', 'start', undefined, third),
    TaskCapacityError
  )
  core.closePeer(peer)
  await receive('third-stream', 'start', undefined, third)
  await receive('same-id', 'chunk', 'other remains active', other)
  assert.equal((await streams[1].stream.next()).value, 'other remains active')
  assert.equal(streams.length, 3)
})

test('stream creation failure releases admission and cancels without replacing its error', async t => {
  const budget = new ActiveTaskBudget({ incomingStreams: { maxActive: 1 } })
  const peer = { id: 'p', state: 'open' }
  const factoryError = new Error('stream factory failed')
  const cancellations = []
  let attempts = 0
  const dispatcher = new ReceiveDispatcher({
    taskBudget: budget,
    pipelines: {
      receiveMiddlewares: [],
      receiveEventMiddlewares: [],
      receiveStreamMiddlewares: []
    },
    schedulers: { inScheduler: new SessionScheduler() },
    invalidStreamObservers: [],
    notifyStreamCancel: (target, streamId) => {
      cancellations.push({ target, streamId })
      throw new Error('cancel callback failed')
    },
    createReceiveStream: target => {
      if (++attempts === 1) throw factoryError
      return new ReceiveStream(target, [])
    }
  })
  t.after(() => dispatcher.dispose())
  await assert.rejects(
    dispatcher.dispatch(peer, { streamId: 'one', streamState: 'start' }),
    error => error === factoryError
  )
  assert.equal(budget.activeCount, 0)
  assert.deepEqual(cancellations, [{ target: peer, streamId: 'one' }])
  await dispatcher.dispatch(peer, { streamId: 'two', streamState: 'start' })
  assert.equal(budget.activeCount, 1)
  await dispatcher.dispatch(peer, { streamId: 'two', streamState: 'end' })
  assert.equal(budget.activeCount, 0)
})

for (const state of ['end', 'error']) {
  test(`late receive ${state} cannot outrun an elapsed idle deadline`, async t => {
    const timer = clock(t)
    const stream = new ReceiveStream({ id: 'late', state: 'open' }, [], {
      idleTimeoutMs: 10
    })
    await stream.push(undefined, 'start')
    timer.jump(10)
    await stream.push(undefined, state, new Error('remote error'))
    await assert.rejects(stream.next(), idleError)
    assert.equal(timer.pending, 0)
  })
}

test('chunk middleware cannot end a receive stream after its idle deadline elapses', async t => {
  const timer = clock(t)
  const stream = new ReceiveStream(
    { id: 'late-hook', state: 'open' },
    [
      ctx => {
        if (ctx.state === 'chunk') {
          timer.jump(10)
          ctx.end()
        }
      }
    ],
    { idleTimeoutMs: 10 }
  )
  await stream.push(undefined, 'start')
  await stream.push('value', 'chunk')
  await assert.rejects(stream.next(), idleError)
  assert.equal(timer.pending, 0)
})

test('idle timeout observers cannot turn the committed receive failure into success', async t => {
  const timer = clock(t)
  const seen = []
  let stream
  stream = new ReceiveStream({ id: 'observer', state: 'open' }, [], {
    idleTimeoutMs: 10,
    onIdleTimeout(error) {
      seen.push(error)
      stream.stop('end')
    }
  })
  await stream.push(undefined, 'start')
  await timer.advance(10)
  await assert.rejects(stream.next(), error => {
    idleError(error)
    assert.equal(error, seen[0])
    return true
  })
  assert.equal(seen.length, 1)
  assert.equal(timer.pending, 0)
})

test('idle expiry frees a stalled receive middleware and prevents late chunks', async t => {
  const timer = clock(t)
  const { core, peer, receive, streams } = setup(t, {
    taskLimits: { incomingStreams: { maxActivePerPeer: 1 } },
    taskTimeouts: { streamIdleMs: 10 }
  })
  const entered = deferred(),
    gate = deferred()
  const delivered = []
  let signal
  core.pipelines.useReceiveStream(async ctx => {
    if (ctx.state === 'chunk' && ctx.data === 'stalled') {
      signal = ctx.signal
      entered.resolve()
      await gate.promise
    }
  })
  await receive('old', 'start')
  streams[0].stream.onData(data => delivered.push(data))
  const blocked = assert.rejects(receive('old', 'chunk', 'stalled'), idleError)
  await entered.promise
  await timer.advance(10)
  await blocked
  assert.equal(signal.aborted, true)
  await core.dispatchers.receive.dispatch(peer, { event: 'notice', data: 'still works' })
  await receive('new', 'start')
  gate.resolve()
  await flush()
  assert.deepEqual(delivered, [])
  assert.equal(streams.length, 2)
  assert.equal(streams[1].stream.isClosed(), false)
})

test('idle deadline starts before asynchronous start middleware can stall', async t => {
  const timer = clock(t)
  const { core, receive, streams } = setup(t, { taskTimeouts: { streamIdleMs: 10 } })
  const entered = deferred(),
    gate = deferred()
  let signal
  core.pipelines.useReceiveStream(async ctx => {
    signal = ctx.signal
    entered.resolve()
    await gate.promise
  })
  const blocked = assert.rejects(receive('s', 'start'), idleError)
  await entered.promise
  await timer.advance(10)
  await blocked
  assert.equal(signal.aborted, true)
  assert.equal(streams[0].stream.isClosed(), true)
  gate.resolve()
  await flush()
})

test('throwing idle observers and close hooks cannot prevent receive cleanup', async t => {
  const timer = clock(t)
  const entered = deferred(),
    gate = deferred(),
    reports = []
  t.mock.method(console, 'error', (...args) => reports.push(args))
  let signal,
    closed = 0
  const stream = new ReceiveStream(
    { id: 'p', state: 'open' },
    [
      async ctx => {
        signal = ctx.signal
        entered.resolve()
        await gate.promise
      }
    ],
    {
      idleTimeoutMs: 10,
      onIdleTimeout() {
        throw new Error('observer failed')
      }
    }
  )
  stream.onClose(() => {
    throw new Error('close callback failed')
  })
  stream.onClose(() => closed++)
  const rejected = assert.rejects(stream.push(undefined, 'start'), idleError)
  await entered.promise
  await timer.advance(10)
  await rejected
  assert.equal(signal.aborted, true)
  assert.equal(closed, 1)
  assert.equal(reports.length, 2)
  assert.equal(timer.pending, 0)
  gate.resolve()
  await flush()
})

test('requested streams count once and send request cancellation on idle expiry', async t => {
  const timer = clock(t)
  const { core, peer, frames } = setup(t, {
    taskLimits: { maxActive: 1 },
    taskTimeouts: { streamIdleMs: 10 }
  })
  const stream = core.requestStream(peer, 'values', null, { timeoutMs: null })
  await flush()
  const request = frames.find(({ frame }) => frame.kind === 'request').frame
  await core.dispatchers.receive.dispatch(peer, {
    kind: 'request_stream',
    requestId: request.requestId,
    streamId: 'requested-result',
    streamState: 'start'
  })
  assert.equal(core.getActiveTaskCount(peer), 1)
  assert.equal(core.getActiveTaskCount(peer, 'outgoingRequests'), 1)
  assert.equal(core.getActiveTaskCount(peer, 'incomingStreams'), 0)
  await timer.advance(10)
  await assert.rejects(stream.next(), idleError)
  assert.equal(core.getActiveTaskCount(peer), 0)
  assert.equal(
    frames.filter(
      ({ frame }) =>
        frame.kind === 'request_cancel' && frame.requestId === request.requestId
    ).length,
    1
  )
})

test('requested stream idle time starts on start rather than request registration', async t => {
  const timer = clock(t)
  const peer = { id: 'requested', state: 'open' }
  const settlements = [],
    errors = []
  const stream = new StreamTask(peer, [], undefined, 10, error => errors.push(error))
  stream.bind((...args) => settlements.push(args))
  t.after(() => stream.cancel())
  await timer.advance(100)
  assert.equal(stream.state, 'waiting')
  await stream.accept({ streamId: 's', streamState: 'start' })
  await timer.advance(8)
  await stream.accept({ streamId: 's', streamState: 'chunk', data: 1 })
  assert.equal((await stream.next()).value, 1)
  await timer.advance(8)
  assert.equal(stream.state, 'open')
  await timer.advance(3)
  await assert.rejects(stream.next(), idleError)
  assert.equal(settlements.length, 1)
  assert.equal(settlements[0][1], true)
  assert.equal(settlements[0][2], true)
  assert.equal(errors.length, 1)
})

test('requested stream timeout still cancels the request when terminal middleware stalls', async t => {
  const timer = clock(t)
  const peer = { id: 'requested', state: 'open' }
  const gate = deferred(),
    entered = deferred(),
    settlements = []
  const stream = new StreamTask(
    peer,
    [
      async ctx => {
        if (ctx.state === 'end') {
          entered.resolve()
          await gate.promise
        }
      }
    ],
    undefined,
    10
  )
  stream.bind((...args) => settlements.push(args))
  await stream.accept({ streamId: 's', streamState: 'start' })
  const closing = stream.accept({ streamId: 's', streamState: 'end' })
  await entered.promise
  await timer.advance(10)
  await closing
  await assert.rejects(stream.next(), idleError)
  assert.equal(settlements.length, 1)
  assert.equal(settlements[0][2], true)
  gate.resolve()
  await flush()
})

test('requested stream normal completion clears idle time and drains buffered chunks', async t => {
  const timer = clock(t)
  const stream = new StreamTask({ id: 'requested', state: 'open' }, [], undefined, 10)
  const settlements = []
  stream.bind((...args) => settlements.push(args))
  await stream.accept({ streamId: 's', streamState: 'start' })
  await stream.accept({ streamId: 's', streamState: 'chunk', data: 1 })
  await stream.accept({ streamId: 's', streamState: 'end' })
  await timer.advance(20)
  assert.equal(stream.state, 'ended')
  assert.deepEqual(await stream.next(), { done: false, value: 1 })
  assert.equal((await stream.next()).done, true)
  assert.equal(settlements.length, 1)
  assert.equal(settlements[0][2], false)
  assert.equal(timer.pending, 0)
})
