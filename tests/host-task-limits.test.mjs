import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { setTimeout as delay } from 'node:timers/promises'
import { WSClient } from '../packages/client/dist/index.js'
import { WSServer } from '../packages/server/dist/index.js'
import {
  TaskCapacityError,
  TaskTimeoutError
} from '../packages/runtime-core/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer } = require('ws')

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

async function pair(t, serverOptions = {}, clientOptions = {}) {
  const raw = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(raw, 'listening')
  const server = WSServer.takeover(raw, serverOptions)
  const client = new WSClient({ protocol: 'ws-flow', ...clientOptions })
  const serverIssues = [],
    clientIssues = []
  server.onIssue(issue => serverIssues.push(issue))
  client.onIssue(error => clientIssues.push(error))
  t.after(async () => {
    client.dispose()
    server.close()
    for (const socket of raw.clients) socket.terminate()
    await new Promise(resolve => raw.close(resolve))
  })
  await client.connect(`ws://127.0.0.1:${raw.address().port}`)
  return { server, client, session: [...server.sessions][0], serverIssues, clientIssues }
}

async function collect(stream) {
  const values = []
  for await (const value of stream) values.push(value)
  return values
}

function captureFrames(socket) {
  const frames = []
  socket.on('message', raw => frames.push(JSON.parse(raw.toString())))
  return frames
}

test('both hosts validate task limits and execution and idle timeouts at construction', () => {
  const invalid = [
    { taskLimits: null },
    { taskLimits: [] },
    { taskLimits: { maxActive: 0 } },
    { taskLimits: { maxActivePerPeer: 1.5 } },
    { taskLimits: { incomingRequests: { maxActive: Infinity } } },
    { taskLimits: { outgoingRequests: { maxActivePerPeer: -1 } } },
    { taskLimits: { incomingStreams: null } },
    { taskLimits: { outgoingStreams: { maxActive: '1' } } },
    { taskTimeouts: null },
    { taskTimeouts: { incomingRequestMs: 0 } },
    { taskTimeouts: { incomingRequestMs: 2147483648 } },
    { taskTimeouts: { streamIdleMs: NaN } },
    { taskTimeouts: { streamIdleMs: 0.5 } }
  ]
  for (const options of invalid) {
    assert.throws(() => new WSClient(options))
    assert.throws(() => new WSServer(options))
  }
  const options = { taskTimeouts: { incomingRequestMs: null, streamIdleMs: null } }
  new WSClient(options).dispose()
  new WSServer(options).close()
})

test(
  'server incoming request capacity rejects only excess work and recovers on cancellation',
  { timeout: 5000 },
  async t => {
    const { server, client, session, serverIssues } = await pair(t, {
      taskLimits: { incomingRequests: { maxActivePerPeer: 1 } }
    })
    const entered = deferred(),
      cancelled = deferred()
    let handled = 0
    server.handleRequest('hold', async ctx => {
      handled++
      entered.resolve()
      await new Promise(resolve =>
        ctx.signal.addEventListener(
          'abort',
          () => {
            cancelled.resolve()
            resolve()
          },
          { once: true }
        )
      )
      return 'late'
    })
    server.handleRequest('ping', () => 'ready')
    const controller = new AbortController()
    const held = assert.rejects(
      client.request('hold', null, {
        timeoutMs: null,
        signal: controller.signal
      }),
      /caller cancelled/
    )
    await entered.promise
    await assert.rejects(client.request('hold', null, { timeoutMs: null }), {
      code: 'CAPACITY_EXCEEDED'
    })
    assert.equal(handled, 1)
    assert.equal(client.state, 'open')
    assert.equal(session.state, 'open')
    assert.equal(serverIssues.length, 1)
    assert.equal(serverIssues[0].type, 'resource_error')
    assert.equal(serverIssues[0].taskKind, 'incomingRequests')
    assert.equal(serverIssues[0].direction, 'receive')
    assert.ok(serverIssues[0].error instanceof TaskCapacityError)
    controller.abort(new Error('caller cancelled'))
    await Promise.all([held, cancelled.promise])
    assert.equal(await client.request('ping', null, { timeoutMs: null }), 'ready')
    assert.equal(serverIssues.length, 1)
  }
)

for (const receiver of ['server', 'client']) {
  test(
    `${receiver} execution deadline aborts handler and sends one terminal to no-timeout caller`,
    { timeout: 5000 },
    async t => {
      const options = { taskTimeouts: { incomingRequestMs: 40 } }
      const { server, client, session, serverIssues, clientIssues } = await pair(
        t,
        receiver === 'server' ? options : {},
        receiver === 'client' ? options : {}
      )
      const host = receiver === 'server' ? server : client
      const caller = receiver === 'server' ? client : session
      const frames = captureFrames(
        receiver === 'server' ? client.getWsInstance() : session.ws
      )
      const entered = deferred(),
        gate = deferred(),
        aborted = deferred()
      let requestId, signal
      t.after(() => gate.resolve())
      host.handleRequest('slow', async ctx => {
        requestId = ctx.requestId
        signal = ctx.signal
        signal.addEventListener('abort', () => aborted.resolve(signal.reason), {
          once: true
        })
        entered.resolve()
        await gate.promise
        return 'late result'
      })
      host.handleRequest('ping', () => 'ready')
      const failed = assert.rejects(caller.request('slow', null, { timeoutMs: null }), {
        code: 'EXECUTION_TIMEOUT'
      })
      await entered.promise
      await failed
      const reason = await aborted.promise
      assert.ok(reason instanceof TaskTimeoutError)
      assert.equal(signal.aborted, true)
      gate.resolve()
      await new Promise(resolve => setImmediate(resolve))
      assert.equal(await caller.request('ping', null, { timeoutMs: null }), 'ready')
      assert.equal(
        frames.filter(
          frame =>
            frame.requestId === requestId &&
            ['response', 'request_error'].includes(frame.kind)
        ).length,
        1
      )
      assert.equal(client.state, 'open')
      assert.equal(session.state, 'open')
      if (receiver === 'server') {
        assert.equal(serverIssues.length, 1)
        assert.equal(serverIssues[0].type, 'resource_error')
        assert.equal(serverIssues[0].taskKind, 'incomingRequests')
        assert.ok(serverIssues[0].error instanceof TaskTimeoutError)
      } else {
        assert.equal(clientIssues.length, 1)
        assert.ok(clientIssues[0] instanceof TaskTimeoutError)
      }
    }
  )
}

for (const receiver of ['server', 'client']) {
  test(
    `${receiver} incoming stream cap cancels only the rejected producer and permits reuse`,
    { timeout: 5000 },
    async t => {
      const options = { taskLimits: { incomingStreams: { maxActivePerPeer: 1 } } }
      const { server, client, session, serverIssues, clientIssues } = await pair(
        t,
        receiver === 'server' ? options : {},
        receiver === 'client' ? options : {}
      )
      const host = receiver === 'server' ? server : client
      const producer = receiver === 'server' ? client : session
      const frames = captureFrames(
        receiver === 'server' ? client.getWsInstance() : session.ws
      )
      const firstRouted = deferred(),
        firstGate = deferred(),
        secondGate = deferred()
      const firstConsumed = deferred(),
        thirdConsumed = deferred()
      let routed = 0
      t.after(() => {
        firstGate.resolve()
        secondGate.resolve()
      })
      host.onStream('feed', async ctx => {
        const index = ++routed
        if (index === 1) firstRouted.resolve()
        const values = await collect(ctx.stream)
        if (index === 1) firstConsumed.resolve(values)
        else thirdConsumed.resolve(values)
      })
      const first = producer.stream('feed', async function* () {
        await firstGate.promise
        yield 'first'
      })
      await firstRouted.promise
      await assert.rejects(
        producer.stream('feed', async function* () {
          await secondGate.promise
          yield 'rejected'
        }),
        /Remote cancelled stream/
      )
      assert.equal(routed, 1)
      assert.equal(frames.filter(frame => frame.kind === 'stream_cancel').length, 1)
      assert.equal(client.state, 'open')
      assert.equal(session.state, 'open')
      secondGate.resolve()
      firstGate.resolve()
      await first
      assert.deepEqual(await firstConsumed.promise, ['first'])
      await producer.stream('feed', async function* () {
        yield 'third'
      })
      assert.deepEqual(await thirdConsumed.promise, ['third'])
      assert.equal(routed, 2)
      if (receiver === 'server') {
        assert.equal(serverIssues.length, 1)
        assert.equal(serverIssues[0].type, 'resource_error')
        assert.equal(serverIssues[0].taskKind, 'incomingStreams')
        assert.ok(serverIssues[0].error instanceof TaskCapacityError)
      } else {
        assert.equal(clientIssues.length, 1)
        assert.ok(clientIssues[0] instanceof TaskCapacityError)
      }
    }
  )
}

for (const receiver of ['server', 'client']) {
  test(
    `${receiver} requested stream idle expiry cancels the remote request and reports once`,
    { timeout: 5000 },
    async t => {
      const options = { taskTimeouts: { streamIdleMs: 40 } }
      const { server, client, session, serverIssues, clientIssues } = await pair(
        t,
        receiver === 'server' ? options : {},
        receiver === 'client' ? options : {}
      )
      const host = receiver === 'server' ? client : server
      const caller = receiver === 'server' ? session : client
      const frames = captureFrames(
        receiver === 'server' ? client.getWsInstance() : session.ws
      )
      const gate = deferred(),
        aborted = deferred()
      t.after(() => gate.resolve())
      host.handleStream('idle', async function* (ctx) {
        ctx.signal.addEventListener('abort', () => aborted.resolve(), { once: true })
        yield 1
        await gate.promise
        yield 2
      })
      host.handleRequest('ping', () => 'ready')
      const task = caller.requestStream('idle', null, { timeoutMs: null })
      assert.equal((await task.next()).value, 1)
      await assert.rejects(task.next(), { code: 'STREAM_IDLE_TIMEOUT' })
      await aborted.promise
      assert.equal(task.state, 'failed')
      assert.equal(frames.filter(frame => frame.kind === 'request_cancel').length, 1)
      gate.resolve()
      assert.equal(await caller.request('ping', null, { timeoutMs: null }), 'ready')
      assert.equal(client.state, 'open')
      assert.equal(session.state, 'open')
      if (receiver === 'server') {
        assert.equal(serverIssues.length, 1)
        assert.equal(serverIssues[0].type, 'resource_error')
        assert.equal(serverIssues[0].taskKind, 'outgoingRequests')
        assert.ok(serverIssues[0].error instanceof TaskTimeoutError)
      } else {
        assert.equal(clientIssues.length, 1)
        assert.ok(clientIssues[0] instanceof TaskTimeoutError)
      }
    }
  )
}

test(
  'server session forwards null idle overrides for outgoing and requested streams',
  { timeout: 5000 },
  async t => {
    const { client, session, serverIssues } = await pair(t, {
      taskTimeouts: { streamIdleMs: 20 }
    })
    const received = deferred()
    client.onStream('slow', async ctx => received.resolve(await collect(ctx.stream)))
    await session.stream(
      'slow',
      async function* () {
        await delay(65)
        yield 'proactive'
      },
      { idleTimeoutMs: null }
    )
    assert.deepEqual(await received.promise, ['proactive'])
    client.handleStream('slow-result', async function* () {
      await delay(65)
      yield 'requested'
    })
    assert.deepEqual(
      await collect(
        session.requestStream('slow-result', null, {
          timeoutMs: null,
          idleTimeoutMs: null
        })
      ),
      ['requested']
    )
    assert.equal(serverIssues.length, 0)
    assert.equal(session.state, 'open')
  }
)
