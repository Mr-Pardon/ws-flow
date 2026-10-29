import test from 'node:test'
import assert from 'node:assert/strict'
import {
  RuntimeCore,
  TaskCapacityError,
  TaskTimeoutError
} from '../packages/runtime-core/dist/index.js'
import { SessionScheduler } from '../packages/scheduler/dist/index.js'

const peer = () => ({ id: 'same-id', state: 'open' })
const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}
const tick = () => new Promise(resolve => setImmediate(resolve))
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const make = (send = () => {}, options = {}) =>
  new RuntimeCore({
    base: { transport: { send } },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() },
    ...options
  })
const pair = (t, options = {}) => {
  const ap = peer(),
    bp = peer(),
    frames = []
  const a = make((_, frame) => b.dispatchers.receive.dispatch(bp, frame))
  const b = make((_, frame) => {
    frames.push(frame)
    return a.dispatchers.receive.dispatch(ap, frame)
  }, options)
  t.after(() => {
    a.dispose()
    b.dispose()
  })
  return { a, b, ap, bp, frames }
}

test('outgoing admission counts peers by identity and rejects before transport', async t => {
  const frames = [],
    a = peer(),
    b = peer(),
    runtime = make((target, frame) => frames.push({ target, frame }), {
      taskLimits: { outgoingRequests: { maxActive: 2, maxActivePerPeer: 1 } }
    })
  t.after(() => runtime.dispose())
  const first = runtime.request(a, 'wait', null, { timeoutMs: null }),
    second = runtime.request(b, 'wait', null, { timeoutMs: null })
  const settled = Promise.allSettled([first, second])
  await assert.rejects(
    runtime.request(peer(), 'excess', null),
    error => error instanceof TaskCapacityError && error.scope === 'global'
  )
  assert.equal(runtime.getActiveTaskCount(), 2)
  await tick()
  assert.equal(frames.length, 2)
  await runtime.dispatchers.receive.dispatch(a, {
    kind: 'response',
    requestId: frames[0].frame.requestId,
    data: 42
  })
  assert.equal(await first, 42)
  await assert.rejects(runtime.request(b, 'excess', null), {
    code: 'CAPACITY_EXCEEDED',
    scope: 'peer'
  })
  runtime.closePeer(b)
  assert.equal((await settled)[1].status, 'rejected')
  assert.equal(runtime.getActiveTaskCount(), 0)
})

test('shared global budget counts incoming and outgoing requests together', async t => {
  const frames = [],
    runtime = make((_, frame) => frames.push(frame), {
      taskLimits: { maxActive: 2 }
    }),
    a = peer(),
    b = peer(),
    gate = deferred(),
    abort = new AbortController()
  t.after(() => {
    gate.resolve()
    runtime.dispose()
  })
  runtime.handleRequest('hold', async () => gate.promise)
  await runtime.dispatchers.receive.dispatch(a, {
    kind: 'request',
    requestId: 'incoming',
    event: 'hold'
  })
  const pending = runtime.request(b, 'outgoing', null, {
    timeoutMs: null,
    signal: abort.signal
  })
  const cancelled = assert.rejects(pending, /cancel outgoing/)
  assert.equal(runtime.getActiveTaskCount(), 2)
  await runtime.dispatchers.receive.dispatch(b, {
    kind: 'request',
    requestId: 'excess',
    event: 'hold'
  })
  await tick()
  assert.equal(
    frames.find(frame => frame.requestId === 'excess').error.code,
    'CAPACITY_EXCEEDED'
  )
  assert.equal(runtime.getActiveTaskCount(), 2)
  abort.abort(new Error('cancel outgoing'))
  await cancelled
  assert.equal(runtime.getActiveTaskCount(), 1)
  await runtime.dispatchers.receive.dispatch(a, {
    kind: 'request_cancel',
    requestId: 'incoming'
  })
  assert.equal(runtime.getActiveTaskCount(), 0)
  assert.equal(a.state, 'open')
  assert.equal(b.state, 'open')
})

test('incoming duplicate IDs consume no extra budget and cancellation frees admission', async t => {
  const frames = [],
    p = peer(),
    gate = deferred(),
    signals = [],
    runtime = make((_, frame) => frames.push(frame), {
      taskLimits: { incomingRequests: { maxActivePerPeer: 1 } }
    })
  t.after(() => {
    gate.resolve()
    runtime.dispose()
  })
  runtime.handleRequest('hold', async ctx => {
    signals.push(ctx.signal)
    await gate.promise
  })
  const message = { kind: 'request', requestId: 'same', event: 'hold' }
  await runtime.dispatchers.receive.dispatch(p, message)
  await runtime.dispatchers.receive.dispatch(p, message)
  await runtime.dispatchers.receive.dispatch(p, { ...message, requestId: 'excess' })
  await tick()
  assert.equal(signals.length, 1)
  assert.equal(runtime.getActiveTaskCount(p, 'incomingRequests'), 1)
  assert.equal(frames.length, 1)
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'request_cancel',
    requestId: 'same'
  })
  assert.equal(signals[0].aborted, true)
  await runtime.dispatchers.receive.dispatch(p, message)
  assert.equal(signals.length, 2)
  runtime.closePeer(p)
  assert.equal(signals[1].aborted, true)
  assert.equal(runtime.getActiveTaskCount(), 0)
})

test('requestStream reserves one request slot throughout its response stream', async t => {
  const { a, b, ap, bp } = pair(t, { taskLimits: { maxActive: 1 } }),
    gate = deferred()
  t.after(() => gate.resolve())
  b.handleStream('stream', async function* () {
    yield 1
    await gate.promise
  })
  const task = a.requestStream(ap, 'stream', null, { timeoutMs: null })
  assert.equal((await task.next()).value, 1)
  assert.equal(b.getActiveTaskCount(bp), 1)
  assert.equal(b.getActiveTaskCount(bp, 'incomingRequests'), 1)
  assert.equal(b.getActiveTaskCount(bp, 'outgoingStreams'), 0)
  assert.equal(a.getActiveTaskCount(ap, 'outgoingRequests'), 1)
  task.cancel()
  await tick()
  assert.equal(a.getActiveTaskCount(), 0)
  assert.equal(b.getActiveTaskCount(), 0)
})

test(
  'execution deadline settles a caller with no timeout and ignores late handler results',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, bp, frames } = pair(t, {
        taskLimits: { incomingRequests: { maxActivePerPeer: 1 } },
        taskTimeouts: { incomingRequestMs: 25 }
      }),
      gate = deferred(),
      failures = []
    t.after(() => gate.resolve())
    let signal
    b.useTaskErrorObserver(info => failures.push(info))
    b.handleRequest('hold', async ctx => {
      signal = ctx.signal
      await gate.promise
      return 'late'
    })
    await assert.rejects(a.request(ap, 'hold', null, { timeoutMs: null }), {
      code: 'EXECUTION_TIMEOUT'
    })
    assert.equal(signal.aborted, true)
    assert.equal(signal.reason instanceof TaskTimeoutError, true)
    assert.equal(b.getActiveTaskCount(bp), 0)
    assert.equal(failures.length, 1)
    assert.equal(failures[0].kind, 'incomingRequests')
    b.handleRequest('next', () => 42)
    assert.equal(await a.request(ap, 'next', null, { timeoutMs: null }), 42)
    gate.resolve()
    await tick()
    assert.deepEqual(
      frames.map(frame => frame.kind),
      ['request_error', 'response']
    )
  }
)

test(
  'incoming execution deadline begins after ingress middleware accepts the request',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t, { taskTimeouts: { incomingRequestMs: 20 } }),
      gate = deferred(),
      entered = deferred()
    t.after(() => gate.resolve())
    b.pipelines.useReceive(async (ctx, next) => {
      if (ctx.envelope.kind === 'request') {
        entered.resolve()
        await gate.promise
      }
      await next()
    })
    b.handleRequest('fast', () => 42)
    const pending = a.request(ap, 'fast', null, { timeoutMs: null })
    await entered.promise
    await delay(40)
    assert.equal(b.getActiveTaskCount(), 0)
    gate.resolve()
    assert.equal(await pending, 42)
  }
)

test(
  'a handler returning after its wall-clock deadline cannot outrun the timer callback',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t, { taskTimeouts: { incomingRequestMs: 10 } })
    b.handleRequest('blocking', () => {
      const until = performance.now() + 25
      while (performance.now() < until) {}
      return 'too late'
    })
    await assert.rejects(a.request(ap, 'blocking', null, { timeoutMs: null }), {
      code: 'EXECUTION_TIMEOUT'
    })
    assert.deepEqual(
      frames.map(frame => frame.kind),
      ['request_error']
    )
  }
)

test(
  'late stream handlers return their generator without starting production',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t, { taskTimeouts: { incomingRequestMs: 20 } }),
      gate = deferred(),
      errors = []
    let returned = 0,
      produced = 0
    t.after(() => gate.resolve())
    b.useRequestErrorObserver(error => errors.push(error))
    b.handleStream('late', async () => {
      await gate.promise
      return {
        async next() {
          produced++
          return { done: false, value: 1 }
        },
        async return() {
          returned++
          throw new Error('late cleanup failed')
        },
        [Symbol.asyncIterator]() {
          return this
        }
      }
    })
    const task = a.requestStream(ap, 'late', null, { timeoutMs: null })
    await assert.rejects(task.next(), { code: 'EXECUTION_TIMEOUT' })
    gate.resolve()
    await tick()
    assert.equal(returned, 1)
    assert.equal(produced, 0)
    assert.equal(errors.length, 1)
    assert.match(errors[0].message, /late cleanup failed/)
    assert.deepEqual(
      frames.map(frame => frame.kind),
      ['request_error']
    )
  }
)

test(
  'execution timeout before stream transport starts uses a request error',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t, { taskTimeouts: { incomingRequestMs: 20 } })
    b.handleStream('blocked', async function* () {
      yield 1
    })
    b.pipelines.useSend(async (ctx, next) => {
      if (ctx.envelope.streamState === 'start') {
        await new Promise((_, reject) => {
          ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason), {
            once: true
          })
        })
      }
      await next()
    })
    const task = a.requestStream(ap, 'blocked', null, { timeoutMs: null })
    await assert.rejects(task.next(), { code: 'EXECUTION_TIMEOUT' })
    assert.deepEqual(
      frames.map(frame => frame.kind),
      ['request_error']
    )
    assert.equal(b.getActiveTaskCount(), 0)
  }
)

test(
  'execution timeout after response stream start uses one stream error terminal',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, frames } = pair(t, { taskTimeouts: { incomingRequestMs: 30 } }),
      gate = deferred()
    t.after(() => gate.resolve())
    b.handleStream('stall', async function* () {
      yield 1
      await gate.promise
      yield 'late'
    })
    const task = a.requestStream(ap, 'stall', null, { timeoutMs: null })
    assert.equal((await task.next()).value, 1)
    await assert.rejects(task.next(), { code: 'EXECUTION_TIMEOUT' })
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'chunk', 'error']
    )
    assert.equal(
      frames.every(frame => frame.kind === 'request_stream'),
      true
    )
    assert.equal(b.getActiveTaskCount(), 0)
    gate.resolve()
    await tick()
    assert.equal(frames.length, 3)
  }
)

test(
  'a committed response is not followed by a second terminal when send completion stalls',
  { timeout: 2000 },
  async t => {
    const p = peer(),
      frames = [],
      gate = deferred(),
      expired = deferred(),
      runtime = make(
        async (_, frame) => {
          frames.push(frame)
          if (frame.kind === 'response') await gate.promise
        },
        { taskTimeouts: { incomingRequestMs: 20 } }
      )
    t.after(() => {
      gate.resolve()
      runtime.dispose()
    })
    let signal
    runtime.handleRequest('fast', ctx => {
      signal = ctx.signal
      return 42
    })
    runtime.useTaskErrorObserver(info => expired.resolve(info))
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: 'committed',
      event: 'fast'
    })
    assert.equal((await expired.promise).error.code, 'EXECUTION_TIMEOUT')
    assert.equal(signal.aborted, true)
    assert.equal(runtime.getActiveTaskCount(), 0)
    gate.resolve()
    await tick()
    assert.deepEqual(
      frames.map(frame => frame.kind),
      ['response']
    )
  }
)

test(
  'normal completion, cancellation and peer close clear incoming deadline timers',
  { timeout: 2000 },
  async t => {
    const frames = [],
      failures = [],
      p = peer(),
      gate = deferred(),
      runtime = make((_, frame) => frames.push(frame), {
        taskTimeouts: { incomingRequestMs: 25 }
      })
    t.after(() => {
      gate.resolve()
      runtime.dispose()
    })
    runtime.useTaskErrorObserver(info => failures.push(info))
    runtime.handleRequest('hold', async () => gate.promise)
    runtime.handleRequest('fast', () => 42)
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: 'fast',
      event: 'fast'
    })
    await tick()
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: 'cancel',
      event: 'hold'
    })
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request_cancel',
      requestId: 'cancel'
    })
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: 'close',
      event: 'hold'
    })
    runtime.closePeer(p)
    assert.equal(runtime.getActiveTaskCount(), 0)
    await delay(50)
    assert.equal(failures.length, 0)
    assert.deepEqual(
      frames.map(frame => frame.kind),
      ['response']
    )
  }
)

for (const terminal of ['end', 'error']) {
  test(
    `a committed response stream ${terminal} remains its only terminal after deadline`,
    { timeout: 2000 },
    async t => {
      const p = peer(),
        frames = [],
        gate = deferred(),
        expired = deferred(),
        runtime = make(
          async (_, frame) => {
            frames.push(frame)
            if (frame.streamState === terminal) await gate.promise
          },
          { taskTimeouts: { incomingRequestMs: 20 } }
        )
      t.after(() => {
        gate.resolve()
        runtime.dispose()
      })
      runtime.handleStream('stream', async function* () {
        if (terminal === 'error') throw new Error('generator failed')
      })
      runtime.useTaskErrorObserver(info => expired.resolve(info))
      await runtime.dispatchers.receive.dispatch(p, {
        kind: 'request',
        requestId: 'committed',
        event: 'stream',
        resultMode: 'stream'
      })
      assert.equal((await expired.promise).error.code, 'EXECUTION_TIMEOUT')
      assert.equal(runtime.getActiveTaskCount(), 0)
      gate.resolve()
      await tick()
      assert.deepEqual(
        frames.map(frame => frame.streamState),
        ['start', terminal]
      )
    }
  )
}

test(
  'closing during detached timeout notification does not report expected send cancellation',
  { timeout: 2000 },
  async t => {
    const p = peer(),
      gate = deferred(),
      handler = deferred(),
      sent = deferred(),
      failures = [],
      runtime = make(
        async (_, frame) => {
          if (frame.kind === 'request_error') {
            sent.resolve()
            await gate.promise
          }
        },
        { taskTimeouts: { incomingRequestMs: 20 } }
      )
    t.after(() => {
      gate.resolve()
      handler.resolve()
      runtime.dispose()
    })
    runtime.useRequestErrorObserver(error => failures.push(error))
    runtime.handleRequest('wait', () => handler.promise)
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: 'timeout',
      event: 'wait'
    })
    await sent.promise
    runtime.dispose()
    await tick()
    assert.equal(failures.length, 0)
  }
)

test(
  'stream idle wins once over a later execution deadline while its error send waits',
  { timeout: 2000 },
  async t => {
    const p = peer(),
      frames = [],
      failures = [],
      gate = deferred(),
      entered = deferred(),
      source = deferred(),
      runtime = make((_, frame) => frames.push(frame), {
        taskTimeouts: { incomingRequestMs: 70, streamIdleMs: 20 }
      })
    let signal
    t.after(() => {
      gate.resolve()
      source.resolve()
      runtime.dispose()
    })
    runtime.useTaskErrorObserver(info => failures.push(info.error.code))
    runtime.handleStream('stall', async function* (ctx) {
      signal = ctx.signal
      await source.promise
      yield 'late'
    })
    runtime.pipelines.useSend(async (ctx, next) => {
      if (ctx.envelope.error?.code === 'STREAM_IDLE_TIMEOUT') {
        entered.resolve()
        await gate.promise
      }
      await next()
    })
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: 'idle-first',
      event: 'stall',
      resultMode: 'stream'
    })
    await entered.promise
    assert.equal(signal.aborted, true)
    assert.equal(signal.reason.code, 'STREAM_IDLE_TIMEOUT')
    assert.equal(runtime.getActiveTaskCount(), 0)
    await delay(90)
    assert.deepEqual(failures, ['STREAM_IDLE_TIMEOUT'])
    gate.resolve()
    await tick()
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'error']
    )
    assert.equal(frames[1].error.code, 'STREAM_IDLE_TIMEOUT')
  }
)

for (const result of ['end', 'chunk']) {
  test(
    `response stream cannot commit a late ${result} before the idle timer callback`,
    { timeout: 2000 },
    async t => {
      const { a, b, ap, frames } = pair(t, {
        taskTimeouts: { streamIdleMs: 10 }
      })
      b.handleStream('blocking', async function* () {
        const end = performance.now() + 25
        while (performance.now() < end) {}
        if (result === 'chunk') yield 'late'
      })
      const task = a.requestStream(ap, 'blocking', null, { timeoutMs: null })
      await assert.rejects(task.next(), { code: 'STREAM_IDLE_TIMEOUT' })
      await tick()
      assert.deepEqual(
        frames.map(frame => frame.streamState),
        ['start', 'error']
      )
      assert.equal(b.getActiveTaskCount(), 0)
    }
  )
}
