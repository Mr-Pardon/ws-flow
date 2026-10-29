import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSClient, genericAdapter } from '../packages/client/dist/index.js'
import { WSServer } from '../packages/server/dist/index.js'
const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer, WebSocket } = require('ws')
const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}
const collect = async task => {
  const values = []
  for await (const value of task) values.push(value)
  return values
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

test(
  'generic client subscribes to independent raw messages and sends no envelope',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      client = new WSClient()
    t.after(() => client.dispose())
    const connected = once(raw, 'connection')
    const subscription = client.subscribe(value => value.type === 'progress')
    await client.connect(url)
    const [remote] = await connected
    remote.send(
      JSON.stringify({
        type: 'progress',
        requestId: 'business',
        kind: 'business',
        value: 42
      })
    )
    assert.equal((await subscription.next()).value.raw.value, 42)
    const incoming = once(remote, 'message')
    await client.send({ op: 'next' })
    assert.deepEqual(JSON.parse((await incoming)[0].toString()), { op: 'next' })
    await assert.rejects(client.request('unsupported', null), /does not support requests/)
    client.close()
    assert.equal((await subscription.next()).done, true)
  }
)

test(
  'new client and server share bidirectional requests, streaming and middleware',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw),
      client = new WSClient({ protocol: 'ws-flow' })
    t.after(() => {
      client.dispose()
      server.close()
    })
    server.handleRequest('double', ctx => ctx.data * 2)
    server.handleStream('range', async function* () {
      yield 1
      yield 2
    })
    client.handleRequest('status', () => 'ready')
    client.handleStream('reverse', async function* () {
      yield 3
      yield 4
    })
    client.useReceiveStream(async (ctx, next) => {
      if (ctx.state === 'chunk') ctx.data *= 10
      await next()
    })
    await client.connect(url)
    assert.equal(await client.request('double', 21), 42)
    assert.deepEqual(await collect(client.requestStream('range', null)), [10, 20])
    const session = [...server.sessions][0]
    assert.equal(await session.request('status', null), 'ready')
    assert.deepEqual(await collect(session.requestStream('reverse', null)), [3, 4])
  }
)

test(
  'client event callback may await a reverse request without blocking reception',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw),
      client = new WSClient({ protocol: 'ws-flow' }),
      done = deferred()
    t.after(() => {
      client.dispose()
      server.close()
    })
    server.handleRequest('answer', () => 42)
    client.on('notice', async () => done.resolve(await client.request('answer', null)))
    await client.connect(url)
    await [...server.sessions][0].emit('notice', {})
    assert.equal(await done.promise, 42)
  }
)

test(
  'proactive stream early break remotely cancels production and requests return',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw),
      client = new WSClient({ protocol: 'ws-flow' }),
      gate = deferred(),
      cleaned = deferred()
    t.after(() => {
      gate.resolve()
      client.dispose()
      server.close()
    })
    client.onStream('feed', async ctx => {
      for await (const value of ctx.stream) {
        assert.equal(value, 1)
        break
      }
    })
    await client.connect(url)
    const sending = [...server.sessions][0].stream('feed', async function* () {
      try {
        yield 1
        await gate.promise
        yield 2
      } finally {
        cleaned.resolve()
      }
    })
    await assert.rejects(sending, /Remote cancelled stream/)
    gate.resolve()
    await cleaned.promise
  }
)

test(
  'producer cancellation sends a terminal error to a waiting proactive consumer',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw),
      client = new WSClient({ protocol: 'ws-flow' }),
      entered = deferred(),
      failed = deferred(),
      gate = deferred()
    t.after(() => {
      gate.resolve()
      client.dispose()
      server.close()
    })
    client.onStream('feed', async ctx => {
      entered.resolve()
      try {
        await ctx.stream.next()
      } catch (error) {
        failed.resolve(error)
      }
    })
    await client.connect(url)
    const controller = new AbortController()
    const rejected = assert.rejects(
      [...server.sessions][0].stream(
        'feed',
        async function* () {
          await gate.promise
          yield 1
        },
        { signal: controller.signal }
      ),
      /producer cancelled/
    )
    await entered.promise
    controller.abort(new Error('producer cancelled'))
    await rejected
    assert.equal((await failed.promise).message, 'producer cancelled')
  }
)

test(
  'requestAll fixes target membership and returns partial results in target order',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw),
      gate = deferred()
    const clients = [
      new WSClient({ protocol: 'ws-flow' }),
      new WSClient({ protocol: 'ws-flow' }),
      new WSClient({ protocol: 'ws-flow' })
    ]
    t.after(() => {
      gate.resolve()
      clients.forEach(client => client.dispose())
      server.close()
    })
    clients[0].handleRequest('value', async () => {
      await gate.promise
      return 42
    })
    clients[1].handleRequest('value', () => {
      throw new Error('partial failure')
    })
    clients[2].handleRequest('value', () => 'must be excluded')
    for (const client of clients) await client.connect(url)
    const sessions = [...server.sessions]
    const task = server.all().except(sessions[2].id).requestAll('value', null)
    const late = new WSClient({ protocol: 'ws-flow' })
    clients.push(late)
    await late.connect(url)
    gate.resolve()
    const results = await task
    assert.equal(results.length, 2)
    assert.equal(results[0].session, sessions[0])
    assert.equal(results[0].value, 42)
    assert.equal(results[1].status, 'rejected')
    assert.match(results[1].reason.message, /partial failure/)
    assert.deepEqual(await server.to(() => false).requestAll('value', null), [])
  }
)

test(
  'takeover detach leaves the raw socket usable and removes client listeners',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      socket = new WebSocket(url)
    t.after(() => socket.terminate())
    const connected = once(raw, 'connection')
    await once(socket, 'open')
    const count = socket.listenerCount('message')
    const client = WSClient.takeover({ socket, send: data => socket.send(data) })
    client.detach()
    assert.equal(socket.listenerCount('message'), count)
    assert.equal(socket.readyState, 1)
    const [remote] = await connected,
      received = once(remote, 'message')
    socket.send('still usable')
    assert.equal((await received)[0].toString(), 'still usable')
    client.dispose()
  }
)

test(
  'reconnect creates a new peer, ends old tasks and does not replay commands',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      client = new WSClient({
        protocol: 'ws-flow',
        reconnect: { delayMs: 5, maxAttempts: 2 }
      })
    t.after(() => client.dispose())
    client.onIssue(() => {})
    const connected = once(raw, 'connection')
    await client.connect(url)
    const firstPeer = client.peer,
      [remote] = await connected
    const subscription = client.subscribe(() => true)
    const requestSent = once(remote, 'message')
    const rejected = assert.rejects(client.request('wait', null), /closed/)
    await requestSent
    const reconnected = deferred()
    client.onState(state => {
      if (state === 'open') reconnected.resolve()
    })
    remote.terminate()
    await rejected
    await reconnected.promise
    assert.notEqual(client.peer, firstPeer)
    assert.equal(firstPeer.state, 'closed')
    assert.equal(subscription.closed, true)
    client.close()
    assert.equal(client.state, 'closed')
  }
)

test('closing while a factory is pending rejects connect and closes its late socket', async () => {
  const gate = deferred()
  let closes = 0
  const socket = {
    readyState: 1,
    binaryType: '',
    send() {},
    close() {
      closes++
    },
    addEventListener() {},
    removeEventListener() {}
  }
  const client = new WSClient({ socketFactory: () => gate.promise })
  const rejected = assert.rejects(client.connect('ws://unused'), /closed/)
  client.close()
  await rejected
  gate.resolve({ socket, send() {} })
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(closes, 1)
  assert.equal(client.getWsInstance(), null)
  client.dispose()
})

test(
  'custom codec and event extractor support a non-ws-flow text service',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      client = new WSClient({
        protocol: genericAdapter({
          event: raw => raw.split(':')[0],
          writeEvent: (event, data) => `${event}:${data}`
        }),
        codec: { encode: value => value, decode: value => value }
      })
    t.after(() => client.dispose())
    const message = deferred(),
      connected = once(raw, 'connection')
    client.on('progress', data => message.resolve(data))
    await client.connect(url)
    const [remote] = await connected
    remote.send('progress:42')
    assert.equal(await message.promise, 'progress:42')
    const received = once(remote, 'message')
    await client.emit('resume', 42)
    assert.equal((await received)[0].toString(), 'resume:42')
  }
)

test(
  'server subscriptions are peer scoped and callbacks can await reverse requests',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw)
    const clients = [
      new WSClient({ protocol: 'ws-flow' }),
      new WSClient({ protocol: 'ws-flow' })
    ]
    t.after(() => {
      clients.forEach(client => client.dispose())
      server.close()
    })
    clients[0].handleRequest('answer', () => 42)
    for (const client of clients) await client.connect(url)
    const [first] = [...server.sessions],
      done = deferred(),
      sessionDone = deferred()
    const scoped = first.subscribe(() => true)
    server.on('ask', async (_, message) =>
      done.resolve(await message.peer.request('answer', null))
    )
    first.on('ask', data => sessionDone.resolve(data))
    await clients[1].emit('unrelated', 'other peer')
    await clients[0].emit('ask', 'first peer')
    assert.equal((await scoped.next()).value.data, 'first peer')
    assert.equal(await done.promise, 42)
    assert.equal(await sessionDone.promise, 'first peer')
    const closed = once(clients[0].getWsInstance(), 'close')
    clients[0].close()
    await closed
    // Use the server's disconnect notification to avoid client/server close-event races.
    if (!scoped.closed) await new Promise(resolve => server.onDisconnect(resolve))
    assert.equal(scoped.closed, true)
  }
)

test(
  'server onStream consumes a proactive client stream and reports callback failure',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw),
      client = new WSClient({ protocol: 'ws-flow' })
    t.after(() => {
      client.dispose()
      server.close()
    })
    const consumed = deferred(),
      issue = deferred()
    server.onStream('upload', async ctx => consumed.resolve(await collect(ctx.stream)))
    server.onIssue('subscription_error', value => issue.resolve(value))
    server.on('broken', () => {
      throw new Error('subscriber failed')
    })
    await client.connect(url)
    await client.stream('upload', async function* () {
      yield 1
      yield 2
    })
    assert.deepEqual(await consumed.promise, [1, 2])
    await client.emit('broken', null)
    assert.equal((await issue.promise).error.message, 'subscriber failed')
  }
)

test(
  'connect deadline covers a pending factory and disposes late connections',
  { timeout: 5000 },
  async () => {
    const gate = deferred()
    let closes = 0
    const client = new WSClient({
      connectTimeoutMs: 10,
      socketFactory: () => gate.promise
    })
    try {
      await assert.rejects(client.connect('ws://unused'), /timed out/)
      gate.resolve({
        socket: {
          readyState: 1,
          close() {
            closes++
          },
          addEventListener() {},
          removeEventListener() {}
        },
        send() {}
      })
      await new Promise(resolve => setImmediate(resolve))
      assert.equal(closes, 1)
      assert.equal(client.state, 'closed')
    } finally {
      client.dispose()
    }
  }
)

test(
  'manual close cancels a scheduled retry and never creates another socket',
  { timeout: 5000 },
  async () => {
    let factories = 0
    const client = new WSClient({
      reconnect: { delayMs: 10, maxAttempts: 3 },
      socketFactory() {
        factories++
        throw new Error('failed handshake')
      }
    })
    try {
      await assert.rejects(client.connect('ws://unused'), /failed handshake/)
      assert.equal(client.state, 'reconnecting')
      client.close()
      await new Promise(resolve => setTimeout(resolve, 30))
      assert.equal(factories, 1)
      assert.equal(client.state, 'closed')
    } finally {
      client.dispose()
    }
  }
)

test(
  'async decode from an old connection cannot enter a new connection subscription',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      decoding = deferred(),
      gate = deferred()
    const client = new WSClient({
      codec: {
        encode: JSON.stringify,
        decode: async value => {
          if (value === 'old') {
            decoding.resolve()
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
    const [remote] = await connected
    remote.send('old')
    await decoding.promise
    client.close()
    const connectedAgain = once(raw, 'connection')
    await client.connect(url)
    const subscription = client.subscribe(() => true),
      [nextRemote] = await connectedAgain
    gate.resolve()
    nextRemote.send('new')
    assert.equal((await subscription.next()).value.raw, 'new')
  }
)
