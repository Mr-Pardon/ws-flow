import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSServer } from '../packages/server/dist/index.js'
import { WSClient } from '../packages/client/dist/index.js'
import { TransportLimitError } from '../packages/runtime-core/dist/index.js'
import { ImmediateScheduler } from '../packages/scheduler/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer, WebSocket } = require('ws')

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

async function setup(t, options) {
  const raw = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(raw, 'listening')
  const server = WSServer.takeover(raw, options)
  const issues = []
  server.onIssue(issue => issues.push(issue))
  t.after(async () => {
    server.close()
    for (const socket of raw.clients) socket.terminate()
    await new Promise(resolve => raw.close(resolve))
  })
  return { server, issues, url: `ws://127.0.0.1:${raw.address().port}` }
}

async function connect(t, server, url) {
  const previous = new Set(server.sessions)
  const socket = new WebSocket(url)
  t.after(() => socket.terminate())
  await once(socket, 'open')
  const peer = [...server.sessions].find(value => !previous.has(value))
  return { socket, peer }
}

const code = expected => error => {
  assert.ok(error instanceof TransportLimitError)
  assert.equal(error.code, expected)
  return true
}

test('server validates byte and water mark configuration before attaching a socket', () => {
  for (const field of ['maxMessageBytes', 'maxPendingBytes', 'maxPendingBytesPerPeer']) {
    for (const value of [0, -1, 1.5, Infinity, NaN]) {
      assert.throws(
        () => new WSServer({ transportLimits: { receive: { [field]: value } } })
      )
      assert.throws(() => new WSServer({ transportLimits: { send: { [field]: value } } }))
    }
  }
  for (const send of [
    { highWaterMark: 0 },
    { highWaterMark: 2, lowWaterMark: 3 },
    { highWaterMark: 2, waitTimeoutMs: 0 },
    { highWaterMark: 2, pollIntervalMs: -1 },
    { lowWaterMark: 1 }
  ])
    assert.throws(() => new WSServer({ transportLimits: { send } }))
})

for (const raw of ['你好', Buffer.from([1, 2, 3, 4, 5, 6])]) {
  test(
    `server rejects oversized ${typeof raw === 'string' ? 'UTF-8' : 'binary'} frames before decoding`,
    { timeout: 5000 },
    async t => {
      let decoded = 0
      const { server, url, issues } = await setup(t, {
        transportLimits: { receive: { maxMessageBytes: 5 } },
        message: {
          decode: () => {
            decoded++
            return { data: true }
          }
        }
      })
      const { socket, peer } = await connect(t, server, url)
      const closed = once(socket, 'close')
      socket.send(raw)
      await closed
      assert.equal(decoded, 0)
      assert.equal(peer.state, 'closed')
      assert.equal(server.sessions.size, 0)
      assert.equal(issues.length, 1)
      assert.equal(issues[0].type, 'resource_error')
      assert.equal(issues[0].direction, 'receive')
      assert.equal(issues[0].error.code, 'MESSAGE_TOO_LARGE')
    }
  )
}

test(
  'server inbound byte overflow rejects an outstanding request with the original resource error',
  { timeout: 5000 },
  async t => {
    const { server, url } = await setup(t, {
      transportLimits: { receive: { maxMessageBytes: 5 } }
    })
    const reported = deferred()
    server.onIssue('resource_error', issue => reported.resolve(issue))
    const { socket, peer } = await connect(t, server, url)
    const sent = once(socket, 'message')
    const outcome = peer.request('waiting', null, { timeoutMs: null }).then(
      () => assert.fail('The outstanding request should reject'),
      error => error
    )
    await sent
    const closed = once(socket, 'close')
    socket.send('oversized')
    const [error, issue] = await Promise.all([outcome, reported.promise])
    assert.ok(error instanceof TransportLimitError)
    assert.equal(error.code, 'MESSAGE_TOO_LARGE')
    assert.equal(error, issue.error)
    assert.equal(issue.direction, 'receive')
    assert.equal(issue.session, peer)
    assert.equal(peer.state, 'closed')
    await closed
  }
)

test(
  'server pending receive byte cap includes running work and closes only the overflowing peer',
  { timeout: 5000 },
  async t => {
    const gate = deferred(),
      entered = deferred(),
      healthy = deferred(),
      decoded = []
    t.after(() => gate.resolve())
    const { server, url, issues } = await setup(t, {
      transportLimits: { receive: { maxPendingBytesPerPeer: 3 } },
      message: {
        decode: value => {
          decoded.push(value.toString())
          return { data: value.toString() }
        }
      }
    })
    server.useReceive(async (ctx, next) => {
      if (ctx.envelope.data === 'aa') {
        entered.resolve()
        await gate.promise
      }
      if (ctx.envelope.data === 'ok') healthy.resolve()
      await next()
    })
    const first = await connect(t, server, url),
      second = await connect(t, server, url)
    first.socket.send('aa')
    await entered.promise
    const closed = once(first.socket, 'close')
    first.socket.send('bb')
    await closed
    assert.equal(first.peer.state, 'closed')
    assert.equal(second.peer.state, 'open')
    second.socket.send('ok')
    await healthy.promise
    assert.deepEqual(decoded, ['aa', 'ok'])
    assert.equal(issues[0].error.code, 'PENDING_BYTES_EXCEEDED')
  }
)

test(
  'server global pending receive bytes are released on detach despite a suspended middleware',
  { timeout: 5000 },
  async t => {
    const gate = deferred(),
      entered = deferred(),
      healthy = deferred()
    t.after(() => gate.resolve())
    const { server, url } = await setup(t, {
      transportLimits: { receive: { maxPendingBytes: 2 } },
      message: { decode: value => ({ data: value.toString() }) }
    })
    server.useReceive(async (ctx, next) => {
      if (ctx.envelope.data === 'aa') {
        entered.resolve()
        await gate.promise
      }
      if (ctx.envelope.data === 'ok') healthy.resolve()
      await next()
    })
    const first = await connect(t, server, url),
      second = await connect(t, server, url)
    first.socket.send('aa')
    await entered.promise
    const closed = once(second.socket, 'close')
    second.socket.send('b')
    await closed
    assert.equal(first.peer.state, 'open')
    server.detach(first.peer)
    const third = await connect(t, server, url)
    third.socket.send('ok')
    await healthy.promise
    assert.equal(third.peer.state, 'open')
  }
)

test(
  'server releases receive bytes after decoding errors and completed dispatch',
  { timeout: 5000 },
  async t => {
    const bad = deferred(),
      good = deferred()
    const { server, url, issues } = await setup(t, {
      transportLimits: { receive: { maxPendingBytes: 2 } },
      message: {
        decode: value => {
          if (value.toString() === 'no') throw new Error('bad codec')
          return { event: 'good', data: value.toString() }
        }
      }
    })
    server.onIssue('decode_error', () => bad.resolve())
    server.on('good', value => good.resolve(value))
    const { socket, peer } = await connect(t, server, url)
    socket.send('no')
    await bad.promise
    socket.send('ok')
    assert.equal(await good.promise, 'ok')
    assert.equal(peer.state, 'open')
    assert.equal(issues.filter(issue => issue.type === 'resource_error').length, 0)
  }
)

test(
  'server outgoing byte cap measures encoded payload once and preserves ordinary sends after overflow',
  { timeout: 5000 },
  async t => {
    let encoded = 0
    const { server, url, issues } = await setup(t, {
      transportLimits: { send: { maxMessageBytes: 5 } },
      message: {
        encode: envelope => {
          encoded++
          return envelope.data
        }
      }
    })
    const { socket, peer } = await connect(t, server, url)
    await assert.rejects(peer.send('你好'), code('MESSAGE_TOO_LARGE'))
    assert.equal(peer.state, 'open')
    const delivered = once(socket, 'message')
    await peer.send(Buffer.from([1, 2, 3, 4, 5]))
    assert.deepEqual((await delivered)[0], Buffer.from([1, 2, 3, 4, 5]))
    assert.equal(encoded, 2)
    assert.equal(issues.length, 1)
    assert.equal(issues[0].type, 'resource_error')
    assert.equal(issues[0].direction, 'send')
  }
)

test(
  'server oversized response closes the socket and settles a remote request without a timeout',
  { timeout: 5000 },
  async t => {
    const { server, url, issues } = await setup(t, {
      transportLimits: { send: { maxMessageBytes: 64 } }
    })
    server.handleRequest('huge', () => 'x'.repeat(100))
    const client = new WSClient({ protocol: 'ws-flow' })
    client.onIssue(() => {})
    t.after(() => client.dispose())
    await client.connect(url)
    await assert.rejects(client.request('huge', null, { timeoutMs: null }), /closed/)
    assert.equal(server.sessions.size, 0)
    assert.equal(issues.length, 1)
    assert.equal(issues[0].type, 'resource_error')
    assert.equal(issues[0].error.code, 'MESSAGE_TOO_LARGE')
  }
)

test(
  'server waits for its low water mark and encodes a waiting send only once',
  { timeout: 5000 },
  async t => {
    const encoded = deferred()
    let count = 0,
      buffered = 10
    const { server, url } = await setup(t, {
      transportLimits: { send: { highWaterMark: 8, lowWaterMark: 2, pollIntervalMs: 1 } },
      message: {
        encode: envelope => {
          count++
          encoded.resolve()
          return envelope.data
        }
      }
    })
    const { socket, peer } = await connect(t, server, url)
    Object.defineProperty(peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => buffered
    })
    const delivered = once(socket, 'message')
    let settled = false
    const sent = peer.send('ok').then(() => {
      settled = true
    })
    await encoded.promise
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.equal(settled, false)
    buffered = 4
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.equal(settled, false)
    buffered = 2
    await sent
    assert.equal((await delivered)[0].toString(), 'ok')
    assert.equal(count, 1)
  }
)

test(
  'server send buffer timeout rejects a notification without closing the session',
  { timeout: 5000 },
  async t => {
    const { server, url, issues } = await setup(t, {
      transportLimits: {
        send: { highWaterMark: 8, waitTimeoutMs: 10, pollIntervalMs: 1 }
      },
      message: { encode: envelope => envelope.data }
    })
    const { socket, peer } = await connect(t, server, url)
    let buffered = 10
    Object.defineProperty(peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => buffered
    })
    await assert.rejects(peer.send('ok'), code('SEND_BUFFER_TIMEOUT'))
    assert.equal(peer.state, 'open')
    buffered = 0
    const delivered = once(socket, 'message')
    await peer.send('ok')
    await delivered
    assert.equal(issues.length, 1)
    assert.equal(issues[0].type, 'resource_error')
  }
)

test(
  'server aborts a staged send before transport and releases bytes for the next send',
  { timeout: 5000 },
  async t => {
    const entered = deferred()
    const { server, url, issues } = await setup(t, {
      transportLimits: {
        send: { highWaterMark: 8, maxPendingBytes: 2, pollIntervalMs: 1 }
      },
      message: {
        encode: envelope => {
          entered.resolve()
          return envelope.data
        }
      }
    })
    const { socket, peer } = await connect(t, server, url)
    let buffered = 10
    Object.defineProperty(peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => buffered
    })
    const controller = new AbortController()
    const aborted = assert.rejects(
      peer.send('no', { signal: controller.signal }),
      /stop staged/
    )
    await entered.promise
    controller.abort(new Error('stop staged'))
    await aborted
    buffered = 0
    const delivered = once(socket, 'message')
    await peer.send('ok')
    assert.equal((await delivered)[0].toString(), 'ok')
    assert.equal(issues.length, 0)
  }
)

test(
  'server closes a peer waiting on its send buffer without waiting for the configured deadline',
  { timeout: 5000 },
  async t => {
    const entered = deferred()
    const { server, url } = await setup(t, {
      transportLimits: { send: { highWaterMark: 8, waitTimeoutMs: 60000 } },
      message: {
        encode: envelope => {
          entered.resolve()
          return envelope.data
        }
      }
    })
    const { peer } = await connect(t, server, url)
    Object.defineProperty(peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => 10
    })
    const rejected = assert.rejects(peer.send('ok'), /closed/)
    await entered.promise
    server.detach(peer)
    await rejected
  }
)

test(
  'server global send staging cap rejects another peer and releases capacity on detach',
  { timeout: 5000 },
  async t => {
    const entered = deferred()
    const { server, url, issues } = await setup(t, {
      transportLimits: {
        send: { highWaterMark: 8, maxPendingBytes: 3, pollIntervalMs: 1 }
      },
      message: {
        encode: envelope => {
          if (envelope.data === 'aa') entered.resolve()
          return envelope.data
        }
      }
    })
    const first = await connect(t, server, url),
      second = await connect(t, server, url)
    Object.defineProperty(first.peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => 10
    })
    const rejected = assert.rejects(first.peer.send('aa'), /closed/)
    await entered.promise
    await assert.rejects(second.peer.send('bb'), code('PENDING_BYTES_EXCEEDED'))
    assert.equal(second.peer.state, 'open')
    server.detach(first.peer)
    await rejected
    const delivered = once(second.socket, 'message')
    await second.peer.send('ok')
    assert.equal((await delivered)[0].toString(), 'ok')
    assert.equal(issues.filter(issue => issue.type === 'resource_error').length, 1)
  }
)

test(
  'server per-peer send staging cap works with a custom concurrent scheduler',
  { timeout: 5000 },
  async t => {
    const entered = deferred()
    const { server, url } = await setup(t, {
      schedulers: { out: new ImmediateScheduler() },
      transportLimits: {
        send: { highWaterMark: 8, maxPendingBytesPerPeer: 3, pollIntervalMs: 1 }
      },
      message: {
        encode: envelope => {
          if (envelope.data === 'aa') entered.resolve()
          return envelope.data
        }
      }
    })
    const { peer } = await connect(t, server, url)
    let buffered = 10
    Object.defineProperty(peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => buffered
    })
    const first = peer.send('aa')
    await entered.promise
    await assert.rejects(peer.send('bb'), code('PENDING_BYTES_EXCEEDED'))
    buffered = 0
    await first
    await peer.send('ok')
    assert.equal(peer.state, 'open')
  }
)

test(
  'server serializes watermark admission through the send callback with concurrent scheduling',
  { timeout: 5000 },
  async t => {
    const committed = deferred(),
      callback = deferred(),
      delivered = deferred()
    const { server, url } = await setup(t, {
      schedulers: { out: new ImmediateScheduler() },
      transportLimits: { send: { highWaterMark: 6, lowWaterMark: 0, pollIntervalMs: 1 } },
      message: { encode: envelope => envelope.data }
    })
    const { peer, socket } = await connect(t, server, url)
    let buffered = 0,
      sends = 0
    const received = []
    socket.on('message', raw => {
      received.push(raw.toString())
      if (received.length === 2) delivered.resolve()
    })
    Object.defineProperty(peer.ws, 'bufferedAmount', {
      configurable: true,
      get: () => buffered
    })
    const original = peer.ws.send.bind(peer.ws)
    peer.ws.send = (raw, done) => {
      sends++
      buffered += Buffer.byteLength(raw)
      original(raw, async error => {
        if (raw === 'aaaa') {
          committed.resolve()
          await callback.promise
        }
        done(error)
      })
    }
    t.after(() => callback.resolve())
    const first = peer.send('aaaa'),
      second = peer.send('bbbb')
    await committed.promise
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.equal(sends, 1)
    callback.resolve()
    await first
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.equal(sends, 1)
    buffered = 0
    await second
    await delivered.promise
    assert.equal(sends, 2)
    assert.deepEqual(received, ['aaaa', 'bbbb'])
  }
)
