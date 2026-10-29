import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSServer } from '../packages/server/dist/index.js'
import { WSClient } from '../packages/client/dist/index.js'
import { WSClient as BrowserWSClient } from '../packages/client/dist/browser.js'
import { RuntimeCore, genericAdapter } from '../packages/runtime-core/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer, WebSocket } = require('ws')

test(
  'generic runtime exchanges a custom wire format with an independent server',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const connected = once(raw, 'connection')
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    const [remote] = await connected
    const core = new RuntimeCore({
      base: {
        protocol: genericAdapter(),
        transport: {
          send: (_, message) =>
            new Promise((resolve, reject) => {
              socket.send(JSON.stringify(message), error =>
                error ? reject(error) : resolve()
              )
            })
        }
      }
    })
    const peer = { id: 'external', state: 'open' }
    const subscription = core.subscribe(raw => raw.type === 'progress', { peer })
    t.after(() => subscription.cancel())
    const incoming = new Promise((resolve, reject) => {
      subscription.next().then(result => resolve(result.value.raw), reject)
      socket.on('message', raw =>
        core.dispatchers.receive.dispatch(peer, JSON.parse(raw.toString())).catch(reject)
      )
    })
    remote.send(JSON.stringify({ type: 'progress', job: 42, percent: 60 }))
    assert.deepEqual(await incoming, { type: 'progress', job: 42, percent: 60 })
    const outgoing = once(remote, 'message')
    await core.send(peer, { action: 'continue', job: 42 }, { priority: 9 })
    assert.deepEqual(JSON.parse((await outgoing)[0].toString()), {
      action: 'continue',
      job: 42
    })
  }
)

test(
  'server sends plain envelopes and preserves middleware context assignments',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    server.useSend(async (ctx, next) => {
      ctx.envelope = { ...ctx.envelope, data: { wrapped: ctx.envelope.data } }
      await next()
    })
    server.useSendStream(async (ctx, next) => {
      if (ctx.state === 'chunk') ctx.data *= 2
      await next()
    })
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    const session = [...server.sessions][0]
    const plain = once(socket, 'message')
    await session.send(42)
    const message = JSON.parse((await plain)[0].toString())
    assert.equal(typeof message.id, 'string')
    assert.deepEqual(message.data, { wrapped: 42 })
    const frames = []
    const ended = new Promise(resolve =>
      socket.on('message', raw => {
        const frame = JSON.parse(raw.toString())
        frames.push(frame)
        if (frame.streamState === 'end') resolve()
      })
    )
    await session.stream('numbers', async function* () {
      yield 3
    })
    await ended
    assert.deepEqual(frames.find(frame => frame.streamState === 'chunk').data, {
      wrapped: 6
    })
  }
)

async function listen(t) {
  const raw = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  t.after(async () => {
    for (const socket of raw.clients) socket.terminate()
    await new Promise(resolve => raw.close(resolve))
  })
  await once(raw, 'listening')
  return { raw, url: `ws://127.0.0.1:${raw.address().port}` }
}

test(
  'requested streams work in both directions over JSON WebSockets',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t),
      server = WSServer.takeover(raw)
    server.handleStream('numbers', async function* (ctx) {
      yield ctx.data
      yield ctx.data + 1
    })
    server.handleStream('error', async function* () {
      throw new Error('remote stream failed')
    })
    const socket = new WebSocket(url)
    t.after(() => {
      socket.terminate()
      server.close()
    })
    await once(socket, 'open')
    const peer = { id: 'server', state: 'open' }
    const runtime = new RuntimeCore({
      base: {
        transport: {
          send: (_, frame) => {
            socket.send(JSON.stringify(frame))
          }
        }
      }
    })
    t.after(() => runtime.dispose())
    const errors = []
    socket.on('message', raw => {
      void runtime.dispatchers.receive
        .dispatch(peer, JSON.parse(raw.toString()))
        .catch(error => errors.push(error))
    })
    runtime.handleStream('client-numbers', async function* () {
      yield 10
      yield 20
    })
    const collect = async task => {
      const values = []
      for await (const item of task) values.push(item)
      return values
    }
    assert.deepEqual(await collect(runtime.requestStream(peer, 'numbers', 1)), [1, 2])
    assert.deepEqual(
      await collect([...server.sessions][0].requestStream('client-numbers', null)),
      [10, 20]
    )
    await assert.rejects(
      collect(runtime.requestStream(peer, 'error', null)),
      /remote stream failed/
    )
    assert.deepEqual(errors, [])
  }
)

test(
  'request cancellation travels both directions over real JSON sockets',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    const socket = new WebSocket(url)
    let release, started, aborted
    const gate = new Promise(resolve => {
      release = resolve
    })
    const entered = new Promise(resolve => {
      started = resolve
    })
    const cancelled = new Promise(resolve => {
      aborted = resolve
    })
    t.after(() => {
      release()
      socket.terminate()
      server.close()
    })
    server.handleRequest('wait', async ctx => {
      ctx.signal.addEventListener('abort', () => aborted(ctx.signal.reason), {
        once: true
      })
      started()
      await gate
      return 'late result'
    })
    await once(socket, 'open')
    const received = []
    socket.on('message', raw => received.push(JSON.parse(raw.toString())))
    socket.send(
      JSON.stringify({ kind: 'request', requestId: 'client-request', event: 'wait' })
    )
    await entered
    socket.send(JSON.stringify({ kind: 'request_cancel', requestId: 'client-request' }))
    assert.equal((await cancelled).code, 'CANCELLED')
    release()
    const session = [...server.sessions][0],
      controller = new AbortController()
    const requestFrame = once(socket, 'message')
    const rejected = assert.rejects(
      session.request('remote-wait', null, { signal: controller.signal }),
      /stop remote/
    )
    const request = JSON.parse((await requestFrame)[0].toString())
    assert.equal(request.kind, 'request')
    const cancelFrame = once(socket, 'message')
    controller.abort(new Error('stop remote'))
    await rejected
    assert.deepEqual(JSON.parse((await cancelFrame)[0].toString()), {
      kind: 'request_cancel',
      requestId: request.requestId
    })
    assert.equal(
      received.some(frame => frame.requestId === 'client-request'),
      false
    )
  }
)

test(
  'server initiates requests and serves reverse requests over JSON sockets',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    server.handleRequest('nested', ctx => ctx.session.request('double', ctx.data))
    server.handleRequest('empty', () => undefined)
    server.handleRequest('failure', () => {
      throw new Error('readable failure')
    })
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    const peer = { id: 'server', state: 'open' }
    const client = new RuntimeCore({
      base: {
        transport: {
          send: (_, message) => {
            socket.send(JSON.stringify(message))
          }
        }
      }
    })
    t.after(() => {
      client.dispose()
      server.close()
    })
    const receivedErrors = []
    socket.on('message', raw => {
      void client.dispatchers.receive
        .dispatch(peer, JSON.parse(raw.toString()))
        .catch(error => receivedErrors.push(error))
    })
    client.handleRequest('double', ctx => ctx.data * 2)
    const session = [...server.sessions][0]
    assert.equal(await session.request('double', 5), 10)
    assert.equal(await client.request(peer, 'nested', 21), 42)
    assert.equal(await client.request(peer, 'empty', null), undefined)
    await assert.rejects(client.request(peer, 'failure', null), {
      code: 'HANDLER_ERROR',
      message: 'readable failure'
    })
    const off = session.handleRequest('empty', () => 'local')
    assert.equal(await client.request(peer, 'empty', null), 'local')
    off()
    assert.equal(await client.request(peer, 'empty', null), undefined)
    assert.deepEqual(receivedErrors, [])
  }
)

test(
  'server request rejects when its client disconnects without responding',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    const socket = new WebSocket(url)
    t.after(() => {
      socket.terminate()
      server.close()
    })
    await once(socket, 'open')
    const received = once(socket, 'message')
    const rejected = assert.rejects(
      [...server.sessions][0].request('wait', null),
      /closed/
    )
    await received
    socket.close()
    await rejected
  }
)

for (const mode of ['abort', 'disconnect']) {
  test(
    `server sending stream settles on ${mode} while its source is waiting`,
    { timeout: 5000 },
    async t => {
      const { raw, url } = await listen(t)
      const server = WSServer.takeover(raw)
      const socket = new WebSocket(url)
      t.after(() => socket.terminate())
      await once(socket, 'open')
      const session = [...server.sessions][0]
      const controller = new AbortController()
      let release, begin, clean
      const gate = new Promise(resolve => {
        release = resolve
      })
      const started = new Promise(resolve => {
        begin = resolve
      })
      const cleaned = new Promise(resolve => {
        clean = resolve
      })
      t.after(() => release())
      const task = session.stream(
        'values',
        async function* () {
          try {
            begin()
            await gate
            yield 1
          } finally {
            clean()
          }
        },
        { signal: controller.signal }
      )
      const rejected = assert.rejects(
        task,
        mode === 'abort' ? /user cancelled/ : /closed/
      )
      await started
      if (mode === 'abort') controller.abort(new Error('user cancelled'))
      else socket.close()
      await rejected
      release()
      await cleaned
      if (mode === 'abort') {
        const response = new Promise(resolve => {
          const receive = raw => {
            const message = JSON.parse(raw.toString())
            if (message.event === 'still-open') {
              socket.off('message', receive)
              resolve(message)
            }
          }
          socket.on('message', receive)
        })
        await session.emit('still-open', true)
        assert.equal((await response).data, true)
      }
      server.close()
    }
  )
}

test(
  'server consumes a complete incoming stream using for-await over real sockets',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    server.route('sum', async ctx => {
      let sum = 0
      for await (const value of ctx.stream) sum += value
      await ctx.session.emit('result', sum)
    })
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    const result = once(socket, 'message')
    for (const [streamState, data] of [['start'], ['chunk', 2], ['chunk', 3], ['end']]) {
      socket.send(JSON.stringify({ event: 'sum', streamId: 's', streamState, data }))
    }
    assert.equal(JSON.parse((await result)[0].toString()).data, 5)
    server.close()
  }
)

test(
  'disconnect rejects pending stream reads and removes session rooms',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    let begin, finish
    const started = new Promise(resolve => {
      begin = resolve
    })
    const ended = new Promise(resolve => {
      finish = resolve
    })
    server.route('wait', async ctx => {
      ctx.session.join('room')
      begin()
      try {
        await ctx.stream.next()
      } catch (error) {
        finish(error)
      }
    })
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    const session = [...server.sessions][0]
    socket.send(JSON.stringify({ event: 'wait', streamId: 's', streamState: 'start' }))
    await started
    socket.close()
    assert.match((await ended).message, /closed/)
    assert.equal(session.state, 'closed')
    assert.equal(server.sessions.size, 0)
    assert.equal(server.rooms.size, 0)
    server.close()
  }
)

test(
  'closing a takeover removes its listener while the external server stays usable',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const external = () => {}
    raw.on('connection', external)
    const server = WSServer.takeover(raw)
    server.close()
    server.close()
    assert.deepEqual(raw.listeners('connection'), [external])
    const connected = once(raw, 'connection')
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    const [remote] = await connected
    assert.throws(() => server.attach(remote), /closed/)
    assert.equal(server.sessions.size, 0)
  }
)

test(
  'built server routes events and emits responses over a real socket',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    const issues = []
    server.onIssue(issue => issues.push(issue.type))
    server.handle(
      'echo',
      ctx => ({ event: 'reply', data: ctx.envelope.data }),
      'envelope'
    )
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')

    const response = once(socket, 'message')
    socket.send(JSON.stringify({ event: 'echo', data: { value: 42 } }))
    assert.deepEqual(JSON.parse((await response)[0].toString()), {
      event: 'reply',
      data: { value: 42 }
    })

    const session = [...server.sessions][0]
    const notification = once(socket, 'message')
    await session.emit('notice', { ok: true })
    const message = JSON.parse((await notification)[0].toString())
    assert.equal(message.event, 'notice')
    assert.deepEqual(message.data, { ok: true })
    assert.deepEqual(issues, [])
  }
)

test(
  'built server sends ordered stream frames and survives malformed JSON',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const server = WSServer.takeover(raw)
    let reportDecode
    const decoded = new Promise(resolve => {
      reportDecode = resolve
    })
    server.onIssue('decode_error', () => reportDecode())
    const socket = new WebSocket(url)
    t.after(() => socket.terminate())
    await once(socket, 'open')
    socket.send('{')
    await decoded

    const frames = []
    const ended = new Promise(resolve =>
      socket.on('message', raw => {
        const frame = JSON.parse(raw.toString())
        frames.push(frame)
        if (frame.streamState === 'end') resolve()
      })
    )
    await [...server.sessions][0].stream('chunks', async function* () {
      yield 1
      yield 2
    })
    await ended
    assert.deepEqual(
      frames.map(frame => frame.streamState),
      ['start', 'chunk', 'chunk', 'end']
    )
    assert.equal(new Set(frames.map(frame => frame.streamId)).size, 1)
    assert.deepEqual(
      frames.filter(frame => frame.streamState === 'chunk').map(frame => frame.data),
      [1, 2]
    )
  }
)

test(
  'client selects ws in Node and exchanges messages with an independent server',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const client = new WSClient()
    t.after(() => client.close())
    const connected = once(raw, 'connection')
    await client.connect(url)
    assert.ok(client.getWsInstance() instanceof WebSocket)
    const [remote] = await connected
    const received = once(remote, 'message')
    await client.send({ action: 'ping', value: 42 })
    assert.deepEqual(JSON.parse((await received)[0].toString()), {
      action: 'ping',
      value: 42
    })
  }
)

test(
  'browser entry uses the native constructor (simulated browser globals)',
  { timeout: 5000 },
  async t => {
    const { raw, url } = await listen(t)
    const originalWindow = Object.getOwnPropertyDescriptor(globalThis, 'window')
    const originalSocket = Object.getOwnPropertyDescriptor(globalThis, 'WebSocket')
    const NativeSocket = globalThis.WebSocket
    assert.equal(
      typeof NativeSocket,
      'function',
      'This test requires Node 22+ native WebSocket'
    )
    let creations = 0
    class BrowserSocket extends NativeSocket {
      constructor(...args) {
        super(...args)
        creations++
      }
    }
    Object.defineProperty(globalThis, 'window', { configurable: true, value: {} })
    Object.defineProperty(globalThis, 'WebSocket', {
      configurable: true,
      value: BrowserSocket
    })
    t.after(() => {
      if (originalWindow) Object.defineProperty(globalThis, 'window', originalWindow)
      else delete globalThis.window
      Object.defineProperty(globalThis, 'WebSocket', originalSocket)
    })
    const client = new BrowserWSClient()
    t.after(() => client.close())
    const connected = once(raw, 'connection')
    await client.connect(url)
    assert.equal(creations, 1)
    assert.ok(client.getWsInstance() instanceof BrowserSocket)
    assert.equal(client.getWsInstance() instanceof WebSocket, false)
    const [remote] = await connected
    const received = once(remote, 'message')
    await client.send({ native: true })
    assert.deepEqual(JSON.parse((await received)[0].toString()), { native: true })
  }
)
