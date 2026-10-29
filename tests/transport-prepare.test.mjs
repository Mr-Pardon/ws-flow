import test from 'node:test'
import assert from 'node:assert/strict'
import { RuntimeCore, TransportSendQueue } from '../packages/runtime-core/dist/index.js'
import { SessionScheduler } from '../packages/scheduler/dist/index.js'

const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

const setup = (t, prepare, protocol) => {
  const peer = { id: 'prepared-peer', state: 'open' }
  const core = new RuntimeCore({
    base: {
      ...(protocol ? { protocol } : {}),
      transport: {
        send() {
          assert.fail('prepared transports must not use the legacy send entry')
        },
        prepare
      }
    },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
  })
  t.after(() => core.dispose())
  return { core, peer }
}

test('transport preparation receives the final adapted message exactly once', async t => {
  const order = []
  const protocol = {
    name: 'custom-wire',
    read: data => ({ data }),
    write(message) {
      order.push('adapt')
      return { operation: message.event, payload: message.data }
    }
  }
  const { core, peer } = setup(
    t,
    (target, message, signal) => {
      assert.equal(target, peer)
      assert.equal(signal.aborted, false)
      assert.deepEqual(message, { operation: 'notice', payload: 'transformed' })
      order.push('prepare')
      return {
        send() {
          order.push('send')
        },
        release() {
          order.push('release')
        }
      }
    },
    protocol
  )
  core.pipelines.useSend(async (ctx, next) => {
    order.push('middleware')
    ctx.envelope.data = 'transformed'
    await next()
  })
  await core.dispatchers.send.dispatch(
    peer,
    { event: 'notice', data: 'original' },
    undefined,
    () => order.push('commit')
  )
  assert.deepEqual(order, ['middleware', 'adapt', 'prepare', 'commit', 'send', 'release'])
})

test('aborting gated preparation rejects promptly and never commits the late result', async t => {
  const entered = deferred(),
    gate = deferred(),
    released = deferred(),
    controller = new AbortController()
  const reason = new Error('cancelled while preparing')
  let committed = 0,
    sent = 0,
    releases = 0
  const { core, peer } = setup(t, async (_, __, signal) => {
    entered.resolve(signal)
    await gate.promise
    return {
      send() {
        sent++
      },
      release() {
        releases++
        released.resolve()
      }
    }
  })
  const rejected = assert.rejects(
    core.dispatchers.send.dispatch(
      peer,
      { data: 1 },
      { signal: controller.signal },
      () => {
        committed++
      }
    ),
    error => error === reason
  )
  const signal = await entered.promise
  controller.abort(reason)
  await rejected
  assert.equal(signal.aborted, true)
  assert.equal(committed, 0)
  assert.equal(sent, 0)
  gate.resolve()
  await released.promise
  assert.equal(releases, 1)
  assert.equal(committed, 0)
  assert.equal(sent, 0)
})

for (const operation of ['closePeer', 'dispose']) {
  test(`${operation} releases a late preparation without sending`, async t => {
    const entered = deferred(),
      gate = deferred(),
      released = deferred()
    let sent = 0,
      committed = 0,
      releases = 0
    const { core, peer } = setup(t, async () => {
      entered.resolve()
      await gate.promise
      return {
        send() {
          sent++
        },
        release() {
          releases++
          released.resolve()
        }
      }
    })
    const rejected = assert.rejects(
      core.dispatchers.send.dispatch(peer, { data: 1 }, undefined, () => {
        committed++
      }),
      /closed/
    )
    await entered.promise
    if (operation === 'closePeer') core.closePeer(peer)
    else core.dispose()
    await rejected
    gate.resolve()
    await released.promise
    assert.equal(sent, 0)
    assert.equal(committed, 0)
    assert.equal(releases, 1)
  })
}

test('a synchronous abort inside preparation still releases its returned handle', async t => {
  const controller = new AbortController()
  const released = deferred()
  const reason = new Error('preparation cancelled')
  let sends = 0,
    commits = 0
  const { core, peer } = setup(t, () => {
    controller.abort(reason)
    return {
      send() {
        sends++
      },
      release() {
        released.resolve()
      }
    }
  })
  await assert.rejects(
    core.dispatchers.send.dispatch(
      peer,
      { data: 1 },
      { signal: controller.signal },
      () => {
        commits++
      }
    ),
    error => error === reason
  )
  await released.promise
  assert.equal(sends, 0)
  assert.equal(commits, 0)
})

for (const asynchronous of [false, true]) {
  test(`prepared ${asynchronous ? 'async' : 'sync'} send failure releases exactly once`, async t => {
    const reason = new Error('write failed')
    let releases = 0,
      commits = 0
    const fail = () => {
      throw reason
    }
    const { core, peer } = setup(t, () => ({
      send: asynchronous ? async () => fail() : fail,
      release() {
        releases++
      }
    }))
    await assert.rejects(
      core.dispatchers.send.dispatch(peer, { data: 1 }, undefined, () => {
        commits++
      }),
      error => error === reason
    )
    assert.equal(releases, 1)
    assert.equal(commits, 1)
  })
}

test('release failure cannot replace the original prepared send failure', async t => {
  const sendError = new Error('socket write failed')
  const releaseError = new Error('custom release failed')
  const reported = []
  t.mock.method(console, 'error', (...args) => reported.push(args))
  let releases = 0
  const { core, peer } = setup(t, () => ({
    send() {
      throw sendError
    },
    release() {
      releases++
      throw releaseError
    }
  }))
  await assert.rejects(core.send(peer, 1), error => error === sendError)
  assert.equal(releases, 1)
  assert.equal(reported.length, 1)
  assert.equal(reported[0][1], releaseError)
})

test('preparation rejection preserves its error without starting transport', async t => {
  const reason = new Error('admission rejected')
  let commits = 0
  const { core, peer } = setup(t, () => {
    throw reason
  })
  await assert.rejects(
    core.dispatchers.send.dispatch(peer, { data: 1 }, undefined, () => {
      commits++
    }),
    error => error === reason
  )
  assert.equal(commits, 0)
})

test('request cancelled during preparation emits neither request nor remote cancel', async t => {
  const entered = deferred(),
    gate = deferred(),
    released = deferred(),
    controller = new AbortController()
  const frames = [],
    preparations = []
  const reason = new Error('request cancelled before admission')
  const { core, peer } = setup(t, async (_, message) => {
    preparations.push(message)
    if (message.kind === 'request') {
      entered.resolve()
      await gate.promise
    }
    return {
      send() {
        frames.push(message)
      },
      release() {
        if (message.kind === 'request') released.resolve()
      }
    }
  })
  const rejected = assert.rejects(
    core.request(peer, 'work', null, { timeoutMs: null, signal: controller.signal }),
    error => error === reason
  )
  await entered.promise
  controller.abort(reason)
  await rejected
  gate.resolve()
  await released.promise
  await core.send(peer, 'barrier')
  assert.deepEqual(
    preparations.map(frame => frame.kind),
    ['request', undefined]
  )
  assert.deepEqual(
    frames.map(frame => frame.data),
    ['barrier']
  )
})

test('proactive stream cancelled before prepared start emits no orphan terminal', async t => {
  const entered = deferred(),
    gate = deferred(),
    released = deferred()
  const frames = [],
    preparations = []
  const reason = new Error('stream cancelled before admission')
  const { core, peer } = setup(t, async (_, message) => {
    preparations.push(message)
    if (message.streamState === 'start') {
      entered.resolve()
      await gate.promise
    }
    return {
      send() {
        frames.push(message)
      },
      release() {
        if (message.streamState === 'start') released.resolve()
      }
    }
  })
  const stream = core.createSendStream(peer, 'values')
  const rejected = assert.rejects(stream.emit('start'), error => error === reason)
  await entered.promise
  stream.cancel(reason)
  await rejected
  gate.resolve()
  await released.promise
  await core.send(peer, 'barrier')
  assert.deepEqual(
    preparations.map(frame => frame.streamState),
    ['start', undefined]
  )
  assert.deepEqual(
    frames.map(frame => frame.data),
    ['barrier']
  )
})

test('response stream rejected before preparation commits uses a request error', async t => {
  const response = deferred()
  const frames = []
  const reason = new Error('response stream admission failed')
  let preparations = 0,
    returns = 0
  const { core, peer } = setup(t, (_, message) => {
    preparations++
    if (message.kind === 'request_stream') throw reason
    return {
      send() {
        frames.push(message)
        response.resolve()
      },
      release() {}
    }
  })
  core.handleStream('values', () => ({
    [Symbol.asyncIterator]() {
      return this
    },
    async next() {
      assert.fail('source must not be read before start is admitted')
    },
    async return() {
      returns++
      return { done: true }
    }
  }))
  await core.dispatchers.receive.dispatch(peer, {
    kind: 'request',
    requestId: 'inbound-request',
    resultMode: 'stream',
    event: 'values'
  })
  await response.promise
  await core.send(peer, 'barrier')
  assert.equal(preparations, 3)
  assert.equal(returns, 1)
  assert.equal(frames.length, 2)
  assert.equal(frames[0].kind, 'request_error')
  assert.equal(frames[0].requestId, 'inbound-request')
  assert.equal(frames[0].error.message, reason.message)
  assert.equal(frames[0].streamState, undefined)
})

test('legacy transports still send when preparation is absent', async t => {
  const frames = []
  const peer = { id: 'legacy-peer', state: 'open' }
  const core = new RuntimeCore({
    base: { transport: { send: (_, message) => frames.push(message) } },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
  })
  t.after(() => core.dispose())
  await core.emit(peer, 'notice', 1)
  assert.equal(frames.length, 1)
  assert.equal(frames[0].event, 'notice')
  assert.equal(frames[0].data, 1)
})

test('transport queue holds admission until the prepared send is released', async () => {
  const queue = new TransportSendQueue()
  const peer = { id: 'same-socket' }
  const order = []
  const first = await queue.prepare(peer, undefined, async () => ({
    send() {
      order.push('send-first')
    },
    release() {
      order.push('release-first')
    }
  }))
  const secondPending = queue.prepare(peer, undefined, async () => {
    order.push('prepare-second')
    return { send() {}, release() {} }
  })
  await first.send()
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(order, ['send-first'])
  first.release()
  first.release()
  const second = await secondPending
  second.release()
  assert.deepEqual(order, ['send-first', 'release-first', 'prepare-second'])
  assert.throws(() => first.send(), /released/)
})

test('transport queue removes cancelled pending factories and permits later work', async () => {
  const queue = new TransportSendQueue()
  const peer = { id: 'same-socket' }
  const controller = new AbortController()
  const reason = new Error('cancel pending transport')
  const first = await queue.prepare(peer, undefined, async () => ({
    send() {},
    release() {}
  }))
  const rejected = assert.rejects(
    queue.prepare(peer, controller.signal, async () => {
      assert.fail('cancelled pending factory must not execute')
    }),
    error => error === reason
  )
  controller.abort(reason)
  await rejected
  first.release()
  const next = await queue.prepare(peer, undefined, async () => ({
    send() {},
    release() {}
  }))
  next.release()
})

test('transport queue releases a cancelled running factory that returns late', async () => {
  const queue = new TransportSendQueue()
  const peer = { id: 'same-socket' }
  const controller = new AbortController()
  const entered = deferred(),
    gate = deferred(),
    released = deferred()
  const reason = new Error('cancel running preparation')
  let releases = 0
  const rejected = assert.rejects(
    queue.prepare(peer, controller.signal, async () => {
      entered.resolve()
      await gate.promise
      return {
        send() {
          assert.fail('cancelled preparation must not be sent')
        },
        release() {
          releases++
          released.resolve()
        }
      }
    }),
    error => error === reason
  )
  await entered.promise
  controller.abort(reason)
  await rejected
  gate.resolve()
  await released.promise
  const next = await queue.prepare(peer, undefined, async () => ({
    send() {},
    release() {}
  }))
  next.release()
  assert.equal(releases, 1)
})

test('transport queue does not free a prepared slot before its owner releases it', async () => {
  const queue = new TransportSendQueue()
  const peer = { id: 'same-socket' }
  const controller = new AbortController()
  const reason = new Error('cancel prepared operation')
  let nextStarted = false
  const first = await queue.prepare(peer, controller.signal, async () => ({
    send() {
      assert.fail('send must check cancellation before committing')
    },
    release() {}
  }))
  const nextPending = queue.prepare(peer, undefined, async () => {
    nextStarted = true
    return { send() {}, release() {} }
  })
  controller.abort(reason)
  assert.throws(
    () => first.send(),
    error => error === reason
  )
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(nextStarted, false)
  first.release()
  const next = await nextPending
  next.release()
  assert.equal(nextStarted, true)
})

test('transport queue advances after factory rejection or a throwing release', async () => {
  const queue = new TransportSendQueue()
  const peer = { id: 'same-socket' }
  const reason = new Error('factory failed')
  await assert.rejects(
    queue.prepare(peer, undefined, async () => {
      throw reason
    }),
    error => error === reason
  )
  const first = await queue.prepare(peer, undefined, async () => ({
    send() {},
    release() {
      throw reason
    }
  }))
  const nextPending = queue.prepare(peer, undefined, async () => ({
    send() {},
    release() {}
  }))
  assert.throws(
    () => first.release(),
    error => error === reason
  )
  first.release()
  const next = await nextPending
  next.release()
})

test('transport queue isolates distinct connection identities with the same display id', async () => {
  const queue = new TransportSendQueue()
  const first = await queue.prepare({ id: 'reused' }, undefined, async () => ({
    send() {},
    release() {}
  }))
  const second = await queue.prepare({ id: 'reused' }, undefined, async () => ({
    send() {},
    release() {}
  }))
  second.release()
  first.release()
})
