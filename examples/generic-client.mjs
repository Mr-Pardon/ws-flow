// Run after pnpm run build: node examples/generic-client.mjs
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSClient } from '../packages/client/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer } = require('ws')
const socketServer = new WebSocketServer({ host: '127.0.0.1', port: 0 })
await once(socketServer, 'listening')

// This service uses its own JSON messages, without a ws-flow Server or envelope.
socketServer.on('connection', socket => {
  socket.on('message', data => {
    const command = JSON.parse(data.toString())
    if (command.action !== 'watch') return

    for (const message of [
      { type: 'heartbeat', online: true },
      { type: 'progress', job: command.job, percent: 25 },
      { type: 'log', text: 'Still working' },
      { type: 'progress', job: command.job, percent: 100 }
    ]) {
      socket.send(JSON.stringify(message))
    }
  })
})

const client = new WSClient()
const progress = client.subscribe(
  raw =>
    typeof raw === 'object' &&
    raw !== null &&
    raw.type === 'progress' &&
    raw.job === 'report-1',
  { signal: AbortSignal.timeout(5000) }
)

try {
  await client.connect(`ws://127.0.0.1:${socketServer.address().port}`)
  await client.send({ action: 'watch', job: 'report-1' })

  const values = []
  for await (const message of progress) {
    values.push(message.raw.percent)
    console.log(`Report progress: ${message.raw.percent}%`)
    if (message.raw.percent === 100) break
  }

  assert.deepEqual(values, [25, 100])
  assert.equal(progress.closed, true)
  console.log('Only the matching events were consumed; subscription released.')
} finally {
  client.dispose()
  for (const socket of socketServer.clients) socket.terminate()
  await new Promise(resolve => socketServer.close(resolve))
}
