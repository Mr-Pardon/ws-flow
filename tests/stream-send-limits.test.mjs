import test from 'node:test'
import assert from 'node:assert/strict'
import {
  RuntimeCore,
  TaskCapacityError,
  TaskTimeoutError
} from '../packages/runtime-core/dist/index.js'

const tick = () => new Promise(resolve => setImmediate(resolve))
const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}
const peer = id => ({ id, state: 'open' })

function setup(t, options = {}, send) {
  const frames = [],
    issues = []
  const core = new RuntimeCore({
    base: { transport: { send: send ?? ((_, frame) => frames.push(frame)) } },
    ...options
  })
  core.useTaskErrorObserver(info => issues.push(info))
  t.after(() => core.dispose())
  return { core, frames, issues }
}

test('outgoing streams share the aggregate task budget with requests', async t => {
  const { core, frames, issues } = setup(t, {
    taskLimits: { maxActive: 2, maxActivePerPeer: 1 }
  })
  const a = peer('a'),
    b = peer('b'),
    c = peer('c')
  const active = core.createSendStream(a)
  assert.throws(
    () => core.createSendStream(a),
    error => error instanceof TaskCapacityError && error.scope === 'peer'
  )
  const pending = core.request(b, 'pending', null, { timeoutMs: null })
  const rejected = assert.rejects(pending)
  assert.throws(
    () => core.createSendStream(c),
    error => error instanceof TaskCapacityError && error.scope === 'global'
  )
  assert.equal(core.getActiveTaskCount(), 2)
  active.cancel()
  const admitted = core.createSendStream(c)
  assert.equal(core.getActiveTaskCount(c, 'outgoingStreams'), 1)
  admitted.cancel()
  core.closePeer(b)
  await rejected
  assert.equal(core.getActiveTaskCount(), 0)
  assert.equal(issues.length, 2)
  assert.equal(frames.filter(frame => frame.streamState).length, 0)
})

test('outgoing stream category caps release on end, cancellation and construction failure', async t => {
  const { core } = setup(t, { taskLimits: { outgoingStreams: { maxActive: 1 } } })
  const a = peer('a'),
    b = peer('b')
  assert.throws(() => core.createSendStream(a, 'x', { idleTimeoutMs: 0 }))
  assert.equal(core.getActiveTaskCount(), 0)
  const first = core.createSendStream(a)
  assert.throws(() => core.createSendStream(b), {
    code: 'CAPACITY_EXCEEDED',
    budget: 'outgoingStreams'
  })
  await first.emit('start')
  await first.emit('end')
  const second = core.createSendStream(b)
  second.cancel()
  const signal = AbortSignal.abort(new Error('already cancelled'))
  const cancelled = core.createSendStream(a, undefined, { signal })
  assert.equal(cancelled.isClosed(), true)
  assert.equal(core.getActiveTaskCount(), 0)
})

test(
  'an allocated but unsent stream expires without an orphan terminal frame',
  { timeout: 3000 },
  async t => {
    const { core, frames, issues } = setup(t, { taskTimeouts: { streamIdleMs: 30 } })
    const stream = core.createSendStream(peer('p'))
    await new Promise(resolve => stream.onClose(resolve))
    await tick()
    assert.ok(stream.error instanceof TaskTimeoutError)
    assert.equal(stream.error.code, 'STREAM_IDLE_TIMEOUT')
    assert.equal(core.getActiveTaskCount(), 0)
    assert.deepEqual(frames, [])
    assert.equal(issues.length, 1)
  }
)

test(
  'stalled proactive generator idles out once and requests source cleanup',
  { timeout: 3000 },
  async t => {
    const { core, frames, issues } = setup(t, { taskTimeouts: { streamIdleMs: 40 } })
    const gate = deferred(),
      entered = deferred(),
      returned = deferred()
    t.after(() => gate.resolve())
    const source = (async function* () {
      try {
        entered.resolve()
        await gate.promise
        yield 'late'
      } finally {
        returned.resolve()
      }
    })()
    const stream = core.createSendStream(peer('p'))
    const running = stream.runGenerator(source)
    const failed = assert.rejects(running, { code: 'STREAM_IDLE_TIMEOUT' })
    await entered.promise
    await failed
    await tick()
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'error']
    )
    assert.equal(frames[1].error.code, 'STREAM_IDLE_TIMEOUT')
    gate.resolve()
    await returned.promise
    assert.equal(core.getActiveTaskCount(), 0)
    assert.equal(issues.length, 1)
    assert.equal(frames.length, 2)
  }
)

test(
  'idle expiry aborts stream middleware and does not submit its late result',
  { timeout: 3000 },
  async t => {
    const { core, frames } = setup(t, { taskTimeouts: { streamIdleMs: 30 } })
    const gate = deferred(),
      entered = deferred()
    t.after(() => gate.resolve())
    let signal
    core.pipelines.useSendStream(async ctx => {
      signal = ctx.signal
      entered.resolve()
      await gate.promise
    })
    const stream = core.createSendStream(peer('p'))
    const emitting = stream.emit('start')
    const failed = assert.rejects(emitting, { code: 'STREAM_IDLE_TIMEOUT' })
    await entered.promise
    await failed
    assert.equal(signal.aborted, true)
    gate.resolve()
    await tick()
    assert.deepEqual(frames, [])
  }
)

test(
  'a terminal already committed to transport is not duplicated by idle expiry',
  { timeout: 3000 },
  async t => {
    const frames = [],
      gate = deferred(),
      entered = deferred()
    t.after(() => gate.resolve())
    const { core } = setup(
      t,
      { taskTimeouts: { streamIdleMs: 35 } },
      async (_, frame) => {
        frames.push(frame)
        if (frame.streamState === 'end') {
          entered.resolve()
          await gate.promise
        }
      }
    )
    const stream = core.createSendStream(peer('p'))
    await stream.emit('start')
    const ending = stream.emit('end')
    const failed = assert.rejects(ending, { code: 'STREAM_IDLE_TIMEOUT' })
    await entered.promise
    await failed
    gate.resolve()
    await tick()
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'end']
    )
    assert.equal(core.getActiveTaskCount(), 0)
  }
)

test('per-stream null override disables the runtime idle deadline', async t => {
  const { core, issues } = setup(t, { taskTimeouts: { streamIdleMs: 10 } })
  const stream = core.createSendStream(peer('p'), 'x', { idleTimeoutMs: null })
  await stream.emit('start')
  await new Promise(resolve => setTimeout(resolve, 25))
  assert.equal(stream.isClosed(), false)
  await stream.emit('end')
  assert.deepEqual(issues, [])
})

test('throwing and rejecting task observers cannot replace capacity failure', async t => {
  const { core } = setup(t, { taskLimits: { maxActive: 1 } })
  const logs = t.mock.method(console, 'error', () => {})
  core.useTaskErrorObserver(() => {
    throw new Error('observer')
  })
  core.useTaskErrorObserver(async () => {
    throw new Error('async observer')
  })
  const stream = core.createSendStream(peer('a'))
  assert.throws(() => core.createSendStream(peer('b')), { code: 'CAPACITY_EXCEEDED' })
  await tick()
  assert.equal(logs.mock.callCount(), 2)
  stream.cancel()
  assert.equal(core.getActiveTaskCount(), 0)
})

for (const state of ['chunk', 'end']) {
  test(`expired producer cannot commit ${state} before timer callbacks run`, async t => {
    let now = 0
    t.mock.method(performance, 'now', () => now)
    const { core, frames } = setup(t, { taskTimeouts: { streamIdleMs: 1000 } })
    const stream = core.createSendStream(peer('p'))
    await stream.emit('start')
    now = 1001
    await assert.rejects(stream.emit(state, 'late'), { code: 'STREAM_IDLE_TIMEOUT' })
    await tick()
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'error']
    )
    assert.equal(frames[1].error.code, 'STREAM_IDLE_TIMEOUT')
    assert.equal(core.getActiveTaskCount(), 0)
  })
}

for (const phase of ['middleware', 'preparation']) {
  test(`deadline is rechecked after ${phase} before committing start`, async t => {
    let now = 0,
      released = 0
    t.mock.method(performance, 'now', () => now)
    const { core, frames } = setup(t, { taskTimeouts: { streamIdleMs: 1000 } })
    if (phase === 'middleware')
      core.pipelines.useSend(() => {
        now = 1001
      })
    else
      core.base.transport.prepare = (_, frame) => {
        now = 1001
        return {
          send: () => frames.push(frame),
          release: () => {
            released++
          }
        }
      }
    const stream = core.createSendStream(peer('p'))
    await assert.rejects(stream.emit('start'), { code: 'STREAM_IDLE_TIMEOUT' })
    await tick()
    assert.deepEqual(frames, [])
    assert.equal(released, phase === 'preparation' ? 1 : 0)
    assert.equal(core.getActiveTaskCount(), 0)
  })
}
