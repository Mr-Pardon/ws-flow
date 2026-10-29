// Run after pnpm run build: node examples/cancellable-task.mjs
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { setTimeout as delay } from 'node:timers/promises'
import { WSClient } from '../packages/client/dist/index.js'
import { WSServer } from '../packages/server/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer } = require('ws')
const socketServer = new WebSocketServer({ host: '127.0.0.1', port: 0 })
await once(socketServer, 'listening')
const server = WSServer.takeover(socketServer)
const client = new WSClient({ protocol: 'ws-flow' })
const stopped = Promise.withResolvers()
let cancelled = false

server.handleStream('report', async function* (ctx) {
  try {
    for (let percent = 20; percent <= 100; percent += 20) {
      // Pass the request signal to real work that supports cancellation.
      await delay(100, undefined, { signal: ctx.signal })
      yield { job: ctx.data.job, percent }
    }
  } catch (error) {
    if (!ctx.signal.aborted) throw error
    cancelled = true
  } finally {
    console.log(`Server work stopped; cancelled: ${cancelled}`)
    stopped.resolve()
  }
})
server.handleRequest('ping', () => 'pong')

const verification = new AbortController()
try {
  await client.connect(`ws://127.0.0.1:${socketServer.address().port}`)
  const values = []

  for await (const progress of client.requestStream('report', { job: 'report-1' })) {
    values.push(progress.percent)
    console.log(`Report progress: ${progress.percent}%`)
    if (progress.percent >= 40) break
  }

  // break ends local consumption first. Wait separately to verify remote cleanup.
  await Promise.race([
    stopped.promise,
    delay(5000, undefined, { signal: verification.signal }).then(() => {
      throw new Error('Server did not finish cancellation in time')
    })
  ])

  assert.deepEqual(values, [20, 40])
  assert.equal(cancelled, true)
  assert.equal(await client.request('ping', null), 'pong')
  console.log('The stream was cancelled; the connection still answers requests.')
} finally {
  verification.abort()
  client.dispose()
  server.close()
  // takeover leaves ownership of the external WebSocketServer with its caller.
  for (const socket of socketServer.clients) socket.terminate()
  await new Promise(resolve => socketServer.close(resolve))
}
