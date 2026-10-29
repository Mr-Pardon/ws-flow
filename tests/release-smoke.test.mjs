import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSClient } from '../packages/client/dist/index.js'
import { WSServer } from '../packages/server/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer } = require('ws')

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

async function collect(stream) {
  const values = []
  for await (const value of stream) values.push(value)
  return values
}

test(
  'bounded hosts reuse one client across five duplex request, stream and disconnect cycles',
  { timeout: 10000 },
  async t => {
    const limits = {
      queueLimits: {
        receive: { maxQueued: 32, maxQueuedPerPeer: 16 },
        send: { maxQueued: 32, maxQueuedPerPeer: 16 }
      },
      transportLimits: {
        receive: { maxMessageBytes: 4096, maxPendingBytes: 16384 },
        send: {
          maxMessageBytes: 4096,
          maxPendingBytes: 16384,
          highWaterMark: 4096,
          lowWaterMark: 1024
        }
      },
      taskLimits: { maxActive: 2, maxActivePerPeer: 2 },
      taskTimeouts: { incomingRequestMs: 2000, streamIdleMs: 2000 }
    }
    const raw = new WebSocketServer({ host: '127.0.0.1', port: 0 })
    await once(raw, 'listening')
    const server = WSServer.takeover(raw, limits)
    const client = new WSClient({ protocol: 'ws-flow', ...limits })
    const serverIssues = [],
      clientIssues = [],
      held = new Map(),
      peerIds = new Set(),
      sessionIds = new Set()
    let disconnected

    t.after(async () => {
      client.dispose()
      server.close()
      for (const socket of raw.clients) socket.terminate()
      await new Promise(resolve => raw.close(resolve))
    })
    server.onIssue(issue => serverIssues.push(issue))
    client.onIssue(error => clientIssues.push(error))
    server.onDisconnect(session => disconnected.resolve(session))
    server.handleRequest('echo', ctx => ({ from: 'server', value: ctx.data }))
    client.handleRequest('echo', ctx => ({ from: 'client', value: ctx.data }))
    for (const host of [server, client]) {
      host.handleStream('numbers', async function* (ctx) {
        yield ctx.data
        yield ctx.data + 1
      })
      host.handleRequest('hold', ctx => {
        const gate = held.get(ctx.data)
        return new Promise(resolve => {
          ctx.signal.addEventListener(
            'abort',
            () => {
              gate.aborted.resolve(ctx.signal.reason)
              resolve('late result')
            },
            { once: true }
          )
          gate.entered.resolve()
        })
      })
    }

    for (let round = 0; round < 5; round++) {
      disconnected = deferred()
      await client.connect(`ws://127.0.0.1:${raw.address().port}`)
      assert.equal(server.sessions.size, 1)
      const session = [...server.sessions][0],
        peer = client.peer
      assert.equal(peerIds.has(peer.id), false)
      assert.equal(sessionIds.has(session.id), false)
      peerIds.add(peer.id)
      sessionIds.add(session.id)
      session.join('release-smoke')
      session.join(`round-${round}`)
      assert.equal(server.rooms.size, 2)
      assert.equal(session.rooms.size, 2)

      assert.deepEqual(
        await Promise.all([
          client.request('echo', round),
          session.request('echo', round)
        ]),
        [
          { from: 'server', value: round },
          { from: 'client', value: round }
        ]
      )
      assert.deepEqual(
        await Promise.all([
          collect(client.requestStream('numbers', round)),
          collect(session.requestStream('numbers', round))
        ]),
        [
          [round, round + 1],
          [round, round + 1]
        ]
      )

      for (const mode of ['cancel', 'disconnect']) {
        const controller = new AbortController()
        const gates = [client, session].map((caller, index) => {
          const key = `${round}-${mode}-${index}`
          const gate = { entered: deferred(), aborted: deferred() }
          held.set(key, gate)
          return {
            key,
            ...gate,
            settled: assert.rejects(
              caller.request('hold', key, {
                timeoutMs: null,
                signal: controller.signal
              }),
              mode === 'cancel' ? /caller cancelled/ : /closed/i
            )
          }
        })
        // Both hosts now hold one incoming and one outgoing request, filling all slots.
        await Promise.all(gates.map(gate => gate.entered.promise))
        if (mode === 'cancel') {
          controller.abort(new Error('caller cancelled'))
        } else {
          const clientSubscription = client.subscribe(() => false),
            sessionSubscription = session.subscribe(() => false),
            socketClosed = once(client.getWsInstance(), 'close')
          client.close()
          await Promise.all([socketClosed, disconnected.promise])
          assert.equal(clientSubscription.closed, true)
          assert.equal(sessionSubscription.closed, true)
          assert.equal(peer.state, 'closed')
          assert.equal(session.state, 'closed')
          assert.equal(session.rooms.size, 0)
          assert.equal(server.sessions.size, 0)
          assert.equal(server.rooms.size, 0)
          assert.equal(server.schedulers.inScheduler.queuedCount, 0)
          assert.equal(server.schedulers.outScheduler.queuedCount, 0)
          assert.equal(client.state, 'closed')
        }
        await Promise.all(gates.flatMap(gate => [gate.settled, gate.aborted.promise]))
        for (const gate of gates) held.delete(gate.key)
      }
      assert.deepEqual(serverIssues, [])
      assert.deepEqual(clientIssues, [])
    }
    assert.equal(held.size, 0)
    assert.equal(peerIds.size, 5)
    assert.equal(sessionIds.size, 5)
  }
)
