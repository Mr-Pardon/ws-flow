import test from 'node:test'
import assert from 'node:assert/strict'
import {
  RuntimeCore,
  RequestError,
  genericAdapter,
  wsFlowAdapter
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
const core = (send = () => {}, protocol) =>
  new RuntimeCore({
    base: { transport: { send }, protocol },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
  })
const pair = t => {
  const ap = peer(),
    bp = peer()
  const a = core((_, frame) => b.dispatchers.receive.dispatch(bp, frame))
  const b = core((_, frame) => a.dispatchers.receive.dispatch(ap, frame))
  t.after(() => {
    a.dispose()
    b.dispose()
  })
  return { a, b, ap, bp }
}

for (const mode of ['abort', 'timeout']) {
  test(
    `${mode} propagates to remote handler without waiting for its completion`,
    { timeout: 2000 },
    async t => {
      const { a, b, ap } = pair(t),
        started = deferred(),
        cancelled = deferred(),
        gate = deferred()
      t.after(() => gate.resolve())
      b.handleRequest('wait', async ctx => {
        ctx.signal.addEventListener('abort', () => cancelled.resolve(ctx.signal.reason), {
          once: true
        })
        started.resolve()
        await gate.promise
        return 'late'
      })
      const controller = new AbortController()
      const pending = a.request(ap, 'wait', null, {
        signal: controller.signal,
        timeoutMs: mode === 'timeout' ? 30 : null
      })
      const rejected = assert.rejects(
        pending,
        mode === 'timeout' ? { code: 'TIMEOUT' } : /local reason/
      )
      await started.promise
      if (mode === 'abort') controller.abort(new Error('local reason'))
      await rejected
      assert.equal((await cancelled.promise).code, 'CANCELLED')
      gate.resolve()
      b.handleRequest('next', () => 42)
      assert.equal(await a.request(ap, 'next', null), 42)
    }
  )
}

test('cancellation before transport emits neither request nor cancel', async t => {
  const frames = [],
    runtime = core((_, frame) => frames.push(frame)),
    p = peer(),
    gate = deferred(),
    started = deferred()
  t.after(() => runtime.dispose())
  runtime.pipelines.useSend(async (ctx, next) => {
    if (ctx.envelope.kind === 'request') {
      started.resolve()
      await gate.promise
    }
    await next()
  })
  const controller = new AbortController()
  const rejected = assert.rejects(
    runtime.request(p, 'wait', null, { signal: controller.signal }),
    /cancelled/
  )
  await started.promise
  controller.abort(new Error('cancelled'))
  await rejected
  gate.resolve()
  await runtime.send(p, 'barrier')
  assert.deepEqual(frames, [{ data: 'barrier' }])
})

test('cancel only targets incoming tasks, even when outgoing ID is identical', async t => {
  const frames = [],
    sent = deferred(),
    runtime = core((_, frame) => {
      frames.push(frame)
      sent.resolve(frame.requestId)
    }),
    p = peer()
  t.after(() => runtime.dispose())
  const gate = deferred()
  t.after(() => gate.resolve())
  let signal
  runtime.handleRequest('reverse', async ctx => {
    signal = ctx.signal
    await gate.promise
    return 'late'
  })
  const outgoing = runtime.request(p, 'outbound', null)
  const id = await sent.promise
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'request',
    requestId: id,
    event: 'reverse'
  })
  await runtime.dispatchers.receive.dispatch(p, { kind: 'request_cancel', requestId: id })
  await runtime.dispatchers.receive.dispatch(p, { kind: 'request_cancel', requestId: id })
  assert.equal(signal.aborted, true)
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'response',
    requestId: id,
    data: 42
  })
  assert.equal(await outgoing, 42)
  gate.resolve()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(
    frames.some(frame => frame.kind === 'response'),
    false
  )
})

test('cancellation capability is opt-in and uses its separate wire encoder', async t => {
  for (const supportsCancel of [false, true]) {
    const frames = [],
      sent = deferred(),
      p = peer(),
      controller = new AbortController()
    const protocol = wsFlowAdapter()
    protocol.request = {
      write: frame => ({ token: frame.requestId, op: frame.event }),
      ...(supportsCancel
        ? { cancel: { write: frame => ({ cancelToken: frame.requestId }) } }
        : {})
    }
    const runtime = core((_, frame) => {
      frames.push(frame)
      sent.resolve()
    }, protocol)
    t.after(() => runtime.dispose())
    const rejected = assert.rejects(
      runtime.request(p, 'wait', null, { signal: controller.signal }),
      /cancelled/
    )
    await sent.promise
    controller.abort(new Error('cancelled'))
    await rejected
    await runtime.send(p, 'barrier')
    assert.equal(frames.length, supportsCancel ? 3 : 2)
    if (supportsCancel) assert.deepEqual(frames[1], { cancelToken: frames[0].token })
  }
})

test('late completion of cancelled handler cannot remove a replacement task', async t => {
  const runtime = core(),
    p = peer(),
    gates = [deferred(), deferred()],
    signals = []
  t.after(() => {
    runtime.dispose()
    gates.forEach(gate => gate.resolve())
  })
  runtime.handleRequest('wait', async ctx => {
    const n = signals.length
    signals.push(ctx.signal)
    await gates[n].promise
  })
  const message = { kind: 'request', requestId: 'reused', event: 'wait' }
  await runtime.dispatchers.receive.dispatch(p, message)
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'request_cancel',
    requestId: 'reused'
  })
  await runtime.dispatchers.receive.dispatch(p, message)
  gates[0].resolve()
  await new Promise(resolve => setImmediate(resolve))
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'request_cancel',
    requestId: 'reused'
  })
  assert.equal(signals[1].aborted, true)
})

test('cancel transport failure is observed without replacing local cancellation', async t => {
  const sent = deferred(),
    reported = deferred(),
    controller = new AbortController(),
    p = peer()
  const runtime = core((_, frame) => {
    if (frame.kind === 'request_cancel') throw new Error('cancel delivery failed')
    sent.resolve()
  })
  runtime.useRequestErrorObserver(error => reported.resolve(error))
  t.after(() => runtime.dispose())
  const rejected = assert.rejects(
    runtime.request(p, 'wait', null, { signal: controller.signal }),
    /original cancellation/
  )
  await sent.promise
  controller.abort(new Error('original cancellation'))
  await rejected
  assert.equal((await reported.promise).message, 'cancel delivery failed')
})

test('in-flight cancellation settles locally before transport completes and preserves wire order', async t => {
  const frames = [],
    sent = deferred(),
    gate = deferred(),
    controller = new AbortController(),
    p = peer()
  const runtime = core(async (_, frame) => {
    frames.push(frame)
    if (frame.kind === 'request') {
      sent.resolve()
      await gate.promise
    }
  })
  t.after(() => {
    gate.resolve()
    runtime.dispose()
  })
  const rejected = assert.rejects(
    runtime.request(p, 'wait', null, { signal: controller.signal }),
    /cancelled/
  )
  await sent.promise
  controller.abort(new Error('cancelled'))
  await rejected
  assert.deepEqual(
    frames.map(frame => frame.kind),
    ['request']
  )
  gate.resolve()
  await runtime.send(p, 'barrier')
  assert.deepEqual(
    frames.map(frame => frame.kind),
    ['request', 'request_cancel', undefined]
  )
})

test(
  'concurrent same-event requests resolve independently in reverse order',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t)
    const gates = [deferred(), deferred()]
    b.handleRequest('value', async ctx => {
      await gates[ctx.data].promise
      return ctx.data
    })
    const first = a.request(ap, 'value', 0),
      second = a.request(ap, 'value', 1)
    gates[1].resolve()
    assert.equal(await second, 1)
    gates[0].resolve()
    assert.equal(await first, 0)
  }
)

test(
  'request handler can await a reverse request without blocking reception',
  { timeout: 2000 },
  async t => {
    const { a, b, ap, bp } = pair(t)
    a.handleRequest('double', ctx => ctx.data * 2)
    b.handleRequest('nested', ctx => b.request(bp, 'double', ctx.data))
    assert.equal(await a.request(ap, 'nested', 21), 42)
  }
)

test(
  'undefined, handler errors, missing handler and business envelope-looking results settle',
  { timeout: 2000 },
  async t => {
    const { a, b, ap } = pair(t)
    b.handleRequest('empty', () => undefined)
    b.handleRequest('fail', () => {
      throw new Error('business failed')
    })
    b.handleRequest('object', () => ({
      data: 1,
      event: 'business',
      requestId: 'business'
    }))
    assert.equal(await a.request(ap, 'empty', null), undefined)
    await assert.rejects(
      a.request(ap, 'fail', null),
      error =>
        error instanceof RequestError &&
        error.code === 'HANDLER_ERROR' &&
        error.message === 'business failed'
    )
    await assert.rejects(a.request(ap, 'unknown', null), { code: 'METHOD_NOT_FOUND' })
    assert.deepEqual(await a.request(ap, 'object', null), {
      data: 1,
      event: 'business',
      requestId: 'business'
    })
  }
)

test(
  'response before send completion wins over a later transport failure',
  { timeout: 2000 },
  async t => {
    const p = peer()
    const runtime = core(async (_, frame) => {
      await runtime.dispatchers.receive.dispatch(p, {
        kind: 'response',
        requestId: frame.requestId,
        data: 42
      })
      throw new Error('late transport failure')
    })
    t.after(() => runtime.dispose())
    assert.equal(await runtime.request(p, 'fast', null), 42)
  }
)

test(
  'timeouts include middleware waiting and suppress late sends',
  { timeout: 2000 },
  async t => {
    const frames = [],
      gate = deferred(),
      p = peer()
    const runtime = core((_, frame) => {
      frames.push(frame)
    })
    t.after(() => runtime.dispose())
    runtime.pipelines.useSend(async (ctx, next) => {
      if (ctx.envelope.kind === 'request') await gate.promise
      await next()
    })
    await assert.rejects(runtime.request(p, 'slow', null, { timeoutMs: 20 }), {
      code: 'TIMEOUT'
    })
    gate.resolve()
    await runtime.send(p, 'barrier')
    assert.deepEqual(frames, [{ data: 'barrier' }])
  }
)

test('abort, pre-abort, send failure and close each reject pending requests', async () => {
  const p = peer(),
    runtime = core(),
    controller = new AbortController()
  const pending = runtime.request(p, 'wait', null, {
    signal: controller.signal,
    timeoutMs: null
  })
  const rejected = assert.rejects(pending, /cancelled/)
  controller.abort(new Error('cancelled'))
  await rejected
  await assert.rejects(
    runtime.request(p, 'wait', null, { signal: controller.signal }),
    /cancelled/
  )
  const disconnected = assert.rejects(runtime.request(p, 'wait', null), /closed/)
  runtime.closePeer(p)
  await disconnected
  await assert.rejects(runtime.request(p, 'wait', null), /closed/)
  const failed = core(() => {
    throw new Error('send failed')
  })
  await assert.rejects(failed.request(peer(), 'wait', null), /send failed/)
  runtime.dispose()
  failed.dispose()
})

test(
  'response ownership uses peer identity and late responses never enter subscriptions',
  { timeout: 2000 },
  async t => {
    const frames = [],
      a = peer(),
      b = peer(),
      sent = deferred()
    const runtime = core((_, frame) => {
      frames.push(frame)
      sent.resolve()
    })
    t.after(() => runtime.dispose())
    const subscription = runtime.subscribe(() => true)
    const task = runtime.request(a, 'test', null)
    await sent.promise
    const response = { kind: 'response', requestId: frames[0].requestId, data: 3 }
    await runtime.dispatchers.receive.dispatch(b, { ...response, data: 99 })
    await runtime.dispatchers.receive.dispatch(a, response)
    assert.equal(await task, 3)
    await runtime.dispatchers.receive.dispatch(a, response)
    await runtime.dispatchers.receive.dispatch(a, { data: 'notification' })
    assert.equal((await subscription.next()).value.data, 'notification')
  }
)

test('local handlers override shared handlers and unregister without removing replacements', async t => {
  const { a, b, ap, bp } = pair(t)
  b.handleRequest('value', () => 'shared')
  const off = b.handleRequest('value', () => 'local', bp)
  assert.throws(() => b.handleRequest('value', () => 0, bp), /already registered/)
  assert.equal(await a.request(ap, 'value', null), 'local')
  off()
  assert.equal(await a.request(ap, 'value', null), 'shared')
  b.handleRequest('value', () => 'replacement', bp)
  off()
  assert.equal(await a.request(ap, 'value', null), 'replacement')
})

test('disconnect aborts inbound handler and suppresses its late response', async () => {
  const frames = [],
    runtime = core((_, frame) => {
      frames.push(frame)
    }),
    p = peer(),
    gate = deferred()
  let signal
  runtime.handleRequest('wait', async ctx => {
    signal = ctx.signal
    await gate.promise
    return 1
  })
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'request',
    requestId: 'r',
    event: 'wait'
  })
  runtime.closePeer(p)
  assert.equal(signal.aborted, true)
  gate.resolve()
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(frames, [])
  runtime.dispose()
})

test('active duplicate IDs execute a handler once', async t => {
  const frames = [],
    runtime = core((_, frame) => {
      frames.push(frame)
    }),
    p = peer(),
    gate = deferred()
  t.after(() => runtime.dispose())
  let calls = 0
  runtime.handleRequest('wait', async () => {
    calls++
    await gate.promise
    return 1
  })
  const message = { kind: 'request', requestId: 'r', event: 'wait' }
  await runtime.dispatchers.receive.dispatch(p, message)
  await runtime.dispatchers.receive.dispatch(p, message)
  assert.equal(calls, 1)
  gate.resolve()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(frames.length, 1)
})

test('blocked request and response middleware fail tasks instead of timing out', async t => {
  const { a, b, ap } = pair(t)
  b.handleRequest('ok', () => 1)
  b.pipelines.useReceive(ctx => {
    ctx.stop()
  })
  // The paired transport exposes receive rejection; both routes settle explicitly.
  await assert.rejects(a.request(ap, 'ok', null), /blocked/)
  const c = core(),
    p = peer(),
    sent = deferred()
  t.after(() => c.dispose())
  c.pipelines.useSend(ctx => {
    sent.resolve(ctx.envelope.requestId)
  })
  c.pipelines.useReceive(ctx => {
    ctx.stop()
  })
  const pending = assert.rejects(c.request(p, 'ok', null), /blocked/)
  const id = await sent.promise
  await assert.rejects(
    c.dispatchers.receive.dispatch(p, { kind: 'response', requestId: id, data: 1 }),
    /blocked/
  )
  await pending
})

test('request identity cannot be rewritten by send middleware', async () => {
  const runtime = core()
  runtime.pipelines.useSend(ctx => {
    ctx.envelope.requestId = 'changed'
  })
  await assert.rejects(runtime.request(peer(), 'x', null), /cannot be changed/)
  runtime.dispose()
})

test('generic adapter does not silently enable requests and invalid timeouts reject', async () => {
  const runtime = core(() => assert.fail('must not send'), genericAdapter())
  await assert.rejects(runtime.request(peer(), 'x', null), /does not support requests/)
  runtime.dispose()
  const standard = core()
  for (const timeoutMs of [0, -1, Infinity, 2147483648]) {
    await assert.rejects(standard.request(peer(), 'x', null, { timeoutMs }), /timeoutMs/)
  }
  standard.dispose()
})

test('pending capacity is bounded and disposal settles all pending tasks', async () => {
  const runtime = core(),
    p = peer()
  const tasks = Array.from({ length: 1024 }, () =>
    runtime.request(p, 'x', null, { timeoutMs: null })
  )
  const finished = Promise.allSettled(tasks)
  await assert.rejects(runtime.request(p, 'x', null), { code: 'CAPACITY_EXCEEDED' })
  runtime.dispose()
  assert.equal(
    (await finished).every(result => result.status === 'rejected'),
    true
  )
})

test('custom protocol maps correlation without exposing standard wire fields', async t => {
  const p = peer(),
    wire = []
  const adapter = {
    name: 'external-rpc',
    read: input => ({ kind: 'response', requestId: input.token, data: input.result }),
    write: input => input.data,
    request: {
      write: frame => ({
        token: frame.requestId,
        operation: frame.event,
        payload: frame.data
      })
    }
  }
  const runtime = core(async (_, message) => {
    wire.push(message)
    await runtime.dispatchers.receive.dispatch(p, { token: message.token, result: 42 })
  }, adapter)
  t.after(() => runtime.dispose())
  assert.equal(await runtime.request(p, 'answer', { value: 1 }), 42)
  assert.deepEqual(Object.keys(wire[0]).sort(), ['operation', 'payload', 'token'])
})

test('simultaneous incoming and outgoing tasks may share the exact same ID', async t => {
  const p = peer(),
    frames = [],
    sent = deferred()
  const runtime = core((_, frame) => {
    frames.push(frame)
    if (frame.kind === 'request') sent.resolve(frame.requestId)
  })
  t.after(() => runtime.dispose())
  runtime.handleRequest('reverse', () => 'inbound')
  const task = runtime.request(p, 'outbound', null)
  const id = await sent.promise
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'request',
    requestId: id,
    event: 'reverse'
  })
  await runtime.dispatchers.receive.dispatch(p, {
    kind: 'response',
    requestId: id,
    data: 'outbound'
  })
  assert.equal(await task, 'outbound')
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(frames.find(frame => frame.kind === 'response').data, 'inbound')
})

test('response middleware cannot change correlation or strand the original task', async t => {
  const p = peer(),
    sent = deferred(),
    runtime = core((_, frame) => sent.resolve(frame.requestId))
  t.after(() => runtime.dispose())
  runtime.pipelines.useReceive(ctx => {
    ctx.envelope.requestId = 'other-task'
  })
  const pending = assert.rejects(runtime.request(p, 'x', null), /cannot be changed/)
  const id = await sent.promise
  await assert.rejects(
    runtime.dispatchers.receive.dispatch(p, { kind: 'response', requestId: id, data: 1 }),
    /cannot be changed/
  )
  await pending
})

test('middleware can transform request data but cannot redirect peer ownership', async t => {
  const { a, b, ap } = pair(t)
  b.handleRequest('value', ctx => ctx.data)
  b.pipelines.useReceive(ctx => {
    ctx.envelope.data *= 2
  })
  assert.equal(await a.request(ap, 'value', 21), 42)
  a.pipelines.useSend(ctx => {
    ctx.peer = peer()
  })
  await assert.rejects(a.request(ap, 'value', 21), /peer cannot be changed/)
})

test('incoming capacity rejects excess work without invoking extra handlers', async () => {
  const frames = [],
    runtime = core((_, frame) => {
      frames.push(frame)
    }),
    p = peer(),
    gate = deferred()
  let calls = 0
  runtime.handleRequest('wait', async () => {
    calls++
    await gate.promise
  })
  for (let i = 0; i < 1025; i++) {
    await runtime.dispatchers.receive.dispatch(p, {
      kind: 'request',
      requestId: String(i),
      event: 'wait'
    })
  }
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(calls, 1024)
  assert.equal(frames.length, 1)
  assert.equal(frames[0].error.code, 'CAPACITY_EXCEEDED')
  runtime.dispose()
  gate.resolve()
})
