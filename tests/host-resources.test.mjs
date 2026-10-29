import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSClient } from '../packages/client/dist/index.js'
import { WSServer } from '../packages/server/dist/index.js'
import {
  ImmediateScheduler,
  QueueCapacityError
} from '../packages/scheduler/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer, WebSocket } = require('ws')

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

async function listen(t) {
  const raw = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(raw, 'listening')
  t.after(async () => {
    for (const socket of raw.clients) socket.terminate()
    await new Promise(resolve => raw.close(resolve))
  })
  return { raw, url: `ws://127.0.0.1:${raw.address().port}` }
}

test('host queue options validate capacity and reject conflicting server schedulers', () => {
  for (const value of [0, -1, NaN, Infinity, 1.5]) {
    assert.throws(() => new WSClient({ queueLimits: { receive: { maxQueued: value } } }))
    assert.throws(
      () => new WSClient({ queueLimits: { send: { maxQueuedPerPeer: value } } })
    )
    assert.throws(() => new WSServer({ queueLimits: { receive: { maxQueued: value } } }))
    assert.throws(
      () => new WSServer({ queueLimits: { send: { maxQueuedPerPeer: value } } })
    )
  }
  assert.throws(
    () =>
      new WSServer({
        schedulers: { in: new ImmediateScheduler() },
        queueLimits: { receive: {} }
      }),
    /queueLimits.receive.*schedulers.in/
  )
  assert.throws(
    () =>
      new WSServer({
        schedulers: { out: new ImmediateScheduler() },
        queueLimits: { send: {} }
      }),
    /queueLimits.send.*schedulers.out/
  )
  const server = new WSServer({
    schedulers: { in: new ImmediateScheduler() },
    queueLimits: { send: { maxQueued: 1 } }
  })
  server.close()
})

test(
  'client bounds frames before async decoding, closes overload and safely reconnects',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const entered = deferred(),
      gate = deferred(),
      issue = deferred(),
      decoded = []
    const client = new WSClient({
      queueLimits: { receive: { maxQueuedPerPeer: 1 } },
      reconnect: { delayMs: 1, maxAttempts: 1 },
      codec: {
        encode: JSON.stringify,
        decode: async value => {
          decoded.push(value)
          if (value === 'first') {
            entered.resolve()
            await gate.promise
          }
          return value
        }
      }
    })
    t.after(() => {
      gate.resolve()
      client.dispose()
    })
    client.onIssue(error => issue.resolve(error))
    const connected = once(raw, 'connection')
    await client.connect(url)
    const [remote] = await connected,
      oldPeer = client.peer
    remote.send('first')
    await entered.promise
    const nextConnection = once(raw, 'connection'),
      reconnected = deferred()
    client.onState(state => {
      if (state === 'open') reconnected.resolve()
    })
    remote.send('queued')
    remote.send('overflow')
    const error = await issue.promise
    assert.ok(error instanceof QueueCapacityError)
    assert.equal(error.limit, 1)
    assert.equal(error.peerId, oldPeer.id)
    assert.equal(oldPeer.state, 'closed')
    assert.notEqual(oldPeer.socket.readyState, 1)
    const [newRemote] = await nextConnection
    await reconnected.promise
    assert.notEqual(client.peer, oldPeer)
    const messages = client.subscribe(() => true)
    newRemote.send('new')
    assert.equal((await messages.next()).value.raw, 'new')
    gate.resolve()
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(decoded, ['first', 'new'])
  }
)

test(
  'client detach removes queued frames without waiting for an active codec',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const entered = deferred(),
      gate = deferred(),
      decoded = []
    const client = new WSClient({
      queueLimits: { receive: { maxQueued: 1 } },
      codec: {
        encode: JSON.stringify,
        decode: async value => {
          decoded.push(value)
          if (value === 'first') {
            entered.resolve()
            await gate.promise
          }
          return value
        }
      }
    })
    t.after(() => {
      gate.resolve()
      client.dispose()
    })
    const connected = once(raw, 'connection')
    await client.connect(url)
    const [remote] = await connected,
      socket = client.getWsInstance()
    remote.send('first')
    await entered.promise
    const enqueued = once(socket, 'message')
    remote.send('queued')
    await enqueued
    client.detach()
    assert.equal(socket.readyState, 1)
    client.attach({ socket, send: data => socket.send(data) })
    const messages = client.subscribe(() => true)
    remote.send('new')
    assert.equal((await messages.next()).value.raw, 'new')
    gate.resolve()
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(decoded, ['first', 'new'])
  }
)

test(
  'server inbound overload immediately detaches only the affected peer and removes queued work',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw, {
      queueLimits: { receive: { maxQueuedPerPeer: 1 } }
    })
    const entered = deferred(),
      gate = deferred(),
      issue = deferred(),
      healthy = deferred(),
      seen = []
    const clients = [new WebSocket(url), new WebSocket(url)]
    t.after(() => {
      gate.resolve()
      clients.forEach(client => client.terminate())
      server.close()
    })
    server.onIssue('resource_error', value => issue.resolve(value))
    server.useReceive(async (ctx, next) => {
      seen.push(ctx.envelope.data)
      if (ctx.envelope.data === 'first') {
        entered.resolve(ctx.session)
        await gate.promise
      }
      await next()
    })
    server.on('healthy', value => healthy.resolve(value))
    await Promise.all(clients.map(client => once(client, 'open')))
    clients[0].send(JSON.stringify({ data: 'first' }))
    const peer = await entered.promise
    clients[0].send(JSON.stringify({ data: 'queued' }))
    clients[0].send(JSON.stringify({ data: 'overflow' }))
    const overflow = await issue.promise
    assert.equal(overflow.session, peer)
    assert.equal(overflow.direction, 'receive')
    assert.ok(overflow.error instanceof QueueCapacityError)
    assert.equal(peer.state, 'closed')
    assert.equal(server.sessions.has(peer), false)
    assert.equal(server.schedulers.inScheduler.queuedCount, 0)
    clients[1].send(JSON.stringify({ event: 'healthy', data: 'other-peer' }))
    assert.equal(await healthy.promise, 'other-peer')
    gate.resolve()
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(seen, ['first', 'other-peer'])
  }
)

test(
  'client ordinary outbound overload rejects locally and aborted queued sends release capacity',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const client = new WSClient({
      protocol: 'ws-flow',
      queueLimits: { send: { maxQueued: 1 } }
    })
    const entered = deferred(),
      gate = deferred(),
      issues = [],
      received = []
    t.after(() => {
      gate.resolve()
      client.dispose()
    })
    client.onIssue(error => issues.push(error))
    client.useSend(async (ctx, next) => {
      if (ctx.envelope.data === 'first') {
        entered.resolve()
        await gate.promise
      }
      await next()
    })
    const connected = once(raw, 'connection')
    await client.connect(url)
    const [remote] = await connected,
      delivered = deferred()
    remote.on('message', raw => {
      received.push(JSON.parse(raw.toString()).data)
      if (received.length === 2) delivered.resolve()
    })
    const first = client.send('first')
    await entered.promise
    const controller = new AbortController()
    const cancelled = assert.rejects(
      client.send('queued', { signal: controller.signal }),
      /cancel queued/
    )
    await assert.rejects(client.send('overflow'), QueueCapacityError)
    assert.equal(client.state, 'open')
    assert.equal(issues.filter(error => error instanceof QueueCapacityError).length, 1)
    controller.abort(new Error('cancel queued'))
    await cancelled
    const last = client.send('last')
    gate.resolve()
    await Promise.all([first, last, delivered.promise])
    assert.deepEqual(received, ['first', 'last'])
  }
)

test(
  'server ordinary outbound overload is observable without closing the session',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw, { queueLimits: { send: { maxQueued: 1 } } })
    const client = new WSClient({ protocol: 'ws-flow' })
    const entered = deferred(),
      gate = deferred(),
      issue = deferred()
    t.after(() => {
      gate.resolve()
      client.dispose()
      server.close()
    })
    server.onIssue('resource_error', value => issue.resolve(value))
    server.useSend(async (ctx, next) => {
      if (ctx.envelope.data === 'first') {
        entered.resolve()
        await gate.promise
      }
      await next()
    })
    await client.connect(url)
    const peer = [...server.sessions][0],
      received = client.subscribe(() => true)
    const first = peer.send('first')
    await entered.promise
    const queued = peer.send('queued')
    await assert.rejects(peer.send('overflow'), QueueCapacityError)
    const overflow = await issue.promise
    assert.equal(overflow.direction, 'send')
    assert.equal(overflow.session, peer)
    assert.equal(peer.state, 'open')
    gate.resolve()
    await Promise.all([first, queued])
    assert.equal((await received.next()).value.data, 'first')
    assert.equal((await received.next()).value.data, 'queued')
  }
)

test(
  'server task outbound overload closes the socket and settles remote requests with no timeout',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw, {
      queueLimits: { send: { maxQueuedPerPeer: 1 } }
    })
    const client = new WSClient({ protocol: 'ws-flow' })
    const entered = deferred(),
      waiting = deferred(),
      gate = deferred(),
      issues = []
    t.after(() => {
      gate.resolve()
      client.dispose()
      server.close()
    })
    client.onIssue(() => {})
    server.onIssue(value => issues.push(value))
    server.handleRequest('never', async ctx => {
      waiting.resolve()
      await new Promise(resolve =>
        ctx.signal.addEventListener('abort', resolve, { once: true })
      )
    })
    server.useSend(async (ctx, next) => {
      if (ctx.envelope.data === 'first') {
        entered.resolve()
        await gate.promise
      }
      await next()
    })
    await client.connect(url)
    const peer = [...server.sessions][0]
    const remoteRequest = assert.rejects(
      client.request('never', null, { timeoutMs: null }),
      /closed/
    )
    await waiting.promise
    const running = assert.rejects(peer.send('first'))
    await entered.promise
    const queued = assert.rejects(peer.send('queued'))
    await assert.rejects(peer.request('overflow', null), QueueCapacityError)
    await Promise.all([running, queued, remoteRequest])
    assert.equal(peer.state, 'closed')
    assert.equal(server.sessions.has(peer), false)
    assert.equal(client.state, 'closed')
    assert.equal(issues.filter(issue => issue.type === 'resource_error').length, 1)
    assert.equal(server.schedulers.outScheduler.queuedCount, 0)
  }
)
