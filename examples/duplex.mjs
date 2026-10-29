// Run from the repository: pnpm run example:duplex
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { WSServer } from '../packages/server/dist/index.js'
import { WSClient } from '../packages/client/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer } = require('ws')
const socketServer = new WebSocketServer({ host: '127.0.0.1', port: 0 })
await once(socketServer, 'listening')
const server = WSServer.takeover(socketServer)
const client = new WSClient({ protocol: 'ws-flow' })

server.handleRequest('double', ctx => ctx.data * 2)
server.handleStream('numbers', async function* (ctx) {
  for (let n = 0; n < ctx.data; n++) {
    if (ctx.signal.aborted) return
    yield n
  }
})
client.handleRequest('status', () => ({ ready: true }))

try {
  await client.connect(`ws://127.0.0.1:${socketServer.address().port}`)
  console.log('Client requests Server:', await client.request('double', 21))
  const values = []
  for await (const value of client.requestStream('numbers', 3)) values.push(value)
  console.log('Streaming response:', values)
  const results = await server.all().requestAll('status', null)
  console.log(
    'Server requests Clients:',
    results.map(({ session, ...result }) => ({ session: session.id, ...result }))
  )
} finally {
  client.dispose()
  server.close()
  // takeover does not own the external server.
  for (const socket of socketServer.clients) socket.terminate()
  await new Promise(resolve => socketServer.close(resolve))
}
