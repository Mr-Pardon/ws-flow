import test from 'node:test'
import assert from 'node:assert/strict'
import { RuntimeCore } from '../packages/runtime-core/dist/index.js'
import { SessionScheduler } from '../packages/scheduler/dist/index.js'

const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}
const bounded = async operation => {
  let timer
  try {
    return await Promise.race([
      operation,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('operation blocked')), 500)
      })
    ])
  } finally {
    clearTimeout(timer)
  }
}
const setup = send => {
  const frames = [],
    peer = { id: 'p', state: 'open' }
  const core = new RuntimeCore({
    base: {
      transport: {
        send:
          send ??
          ((_, message) => {
            frames.push(message)
          })
      }
    },
    schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
  })
  return { core, peer, frames }
}

test('disconnect settles a generator waiting on user code and eventually executes finally', async () => {
  const { core, peer, frames } = setup()
  const entered = deferred(),
    release = deferred(),
    cleaned = deferred()
  const stream = core.createSendStream(peer, 'values')
  const reason = new Error('connection lost')
  const task = stream.runGenerator(
    (async function* () {
      try {
        entered.resolve()
        await release.promise
        yield 1
      } finally {
        cleaned.resolve()
      }
    })()
  )
  const rejected = assert.rejects(task, error => error === reason)
  await entered.promise
  core.closePeer(peer, reason)
  await bounded(rejected)
  assert.equal(stream.isClosed(), true)
  release.resolve()
  await bounded(cleaned.promise)
  assert.deepEqual(
    frames.map(frame => frame.streamState),
    ['start']
  )
})

test('failed start requests generator return even when no item was read', async () => {
  const reason = new Error('transport failed')
  const { core, peer } = setup(() => {
    throw reason
  })
  let reads = 0,
    returns = 0
  const gen = {
    next: async () => {
      reads++
      return { done: false, value: 1 }
    },
    return: async () => {
      returns++
      return { done: true }
    }
  }
  const stream = core.createSendStream(peer)
  await assert.rejects(stream.runGenerator(gen), error => error === reason)
  assert.equal(reads, 0)
  assert.equal(returns, 1)
  assert.equal(stream.isClosed(), true)
  core.dispose()
  assert.equal(returns, 1)
})

test('generator failure sends one error frame and preserves the source error', async () => {
  const { core, peer, frames } = setup()
  const reason = new Error('source failed')
  const stream = core.createSendStream(peer)
  await assert.rejects(
    stream.runGenerator(
      (async function* () {
        yield 1
        throw reason
      })()
    ),
    error => error === reason
  )
  assert.deepEqual(
    frames.map(frame => frame.streamState),
    ['start', 'chunk', 'error']
  )
  assert.deepEqual(frames[2].error, { code: 'STREAM_ERROR', message: reason.message })
})

test('error-frame transport failure does not replace generator failure', async () => {
  const source = new Error('source'),
    transport = new Error('transport')
  const { core, peer } = setup((_, frame) => {
    if (frame.streamState === 'error') throw transport
  })
  await assert.rejects(
    core.createSendStream(peer).runGenerator(
      (async function* () {
        throw source
      })()
    ),
    error => error === source
  )
})

test('aborted stream cannot send after asynchronous send middleware resumes', async () => {
  const { core, peer, frames } = setup()
  const entered = deferred(),
    release = deferred(),
    controller = new AbortController()
  const reason = new Error('cancel requested')
  core.pipelines.useSend(async (_, next) => {
    entered.resolve()
    await release.promise
    await next()
  })
  const stream = core.createSendStream(peer, undefined, { signal: controller.signal })
  const pending = assert.rejects(
    stream.runGenerator(
      (async function* () {
        yield 1
      })()
    ),
    error => error === reason
  )
  await entered.promise
  controller.abort(reason)
  await bounded(pending)
  release.resolve()
  // Queue a later send as a barrier: the cancelled start must finish first.
  await core.send(peer, 'barrier')
  assert.deepEqual(
    frames.map(frame => frame.data),
    ['barrier']
  )
})

test('cancelling one stream does not close another stream or its peer', async () => {
  const { core, peer, frames } = setup()
  const a = core.createSendStream(peer),
    b = core.createSendStream(peer)
  await a.emit('start')
  a.cancel()
  await assert.rejects(a.emit('chunk', 1), /cancelled/)
  await b.runGenerator(
    (async function* () {
      yield 2
    })()
  )
  assert.deepEqual(
    frames.map(frame => frame.streamState),
    ['start', 'error', 'start', 'chunk', 'end']
  )
  assert.equal(b.isClosed(), true)
})

test('dispose cancels streams on every peer and pre-aborted streams never send', async () => {
  const { core, peer, frames } = setup()
  const a = core.createSendStream(peer)
  const b = core.createSendStream({ id: 'other', state: 'open' })
  const controller = new AbortController()
  controller.abort(new Error('already cancelled'))
  const c = core.createSendStream(peer, undefined, { signal: controller.signal })
  await assert.rejects(
    c.runGenerator(
      (async function* () {
        yield 1
      })()
    ),
    /already cancelled/
  )
  core.dispose()
  assert.equal(a.isClosed(), true)
  assert.equal(b.isClosed(), true)
  assert.deepEqual(frames, [])
})

test('stream middleware end closes the source early with exactly one terminal frame', async () => {
  const { core, peer, frames } = setup()
  let finalized = 0
  core.pipelines.useSendStream(async (ctx, next) => {
    if (ctx.state === 'chunk') ctx.end()
    await next()
  })
  await core.createSendStream(peer).runGenerator(
    (async function* () {
      try {
        yield 1
        yield 2
      } finally {
        finalized++
      }
    })()
  )
  assert.equal(finalized, 1)
  assert.deepEqual(
    frames.map(frame => frame.streamState),
    ['start', 'end']
  )
})

test('invalid frame order fails locally and closes the stream', async () => {
  const { core, peer, frames } = setup()
  const stream = core.createSendStream(peer)
  await assert.rejects(stream.emit('chunk', 1), /must start/)
  assert.equal(stream.isClosed(), true)
  assert.deepEqual(frames, [])
})

test('cancellation settles an in-flight send and suppresses queued chunks', async () => {
  const entered = deferred(),
    release = deferred(),
    frames = []
  const { core, peer } = setup(async (_, frame) => {
    frames.push(frame)
    entered.resolve()
    await release.promise
  })
  const stream = core.createSendStream(peer)
  const start = assert.rejects(stream.emit('start'), /cancelled/)
  await entered.promise
  const chunk = assert.rejects(stream.emit('chunk', 1), /cancelled/)
  stream.cancel()
  await bounded(Promise.all([start, chunk]))
  release.resolve()
  await core.send(peer, 'barrier')
  assert.equal(frames.filter(frame => frame.streamState === 'chunk').length, 0)
  core.dispose()
})
