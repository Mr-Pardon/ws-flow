import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { spawnSync } from 'node:child_process'
import { WSServer } from '../packages/server/dist/index.js'

const require = createRequire(new URL('../packages/server/package.json', import.meta.url))
const { WebSocketServer, WebSocket } = require('ws')

async function listen(t) {
  const raw = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  t.after(async () => {
    for (const socket of raw.clients) socket.terminate()
    await new Promise(resolve => raw.close(resolve))
  })
  await once(raw, 'listening')
  return raw
}

test(
  'owned server reports an occupied port through onIssue',
  { timeout: 5000 },
  async t => {
    const occupied = await listen(t)
    const server = WSServer.create({
      host: '127.0.0.1',
      port: occupied.address().port
    })
    t.after(() => server.close())
    const allIssues = []
    server.onIssue(issue => allIssues.push(issue))
    const issue = await new Promise(resolve => server.onIssue('server_error', resolve))

    assert.equal(issue.error.code, 'EADDRINUSE')
    assert.equal(issue.session, undefined)
    assert.ok(Number.isFinite(issue.timestamp))
    assert.deepEqual(allIssues, [issue])
    assert.equal(server.sessions.size, 0)
  }
)

test('closing during startup consumes queued listener errors without crashing', async t => {
  const occupied = await listen(t)
  const entry = import.meta.resolve('../packages/server/dist/index.js')
  const source = `
    import { WSServer } from ${JSON.stringify(entry)};
    const server = WSServer.create({
      host: '127.0.0.1',
      port: ${occupied.address().port}
    });
    server.onIssue(() => { throw new Error('Observers already disposed'); });
    process.nextTick(() => {
      server.close();
      server.close();
    });
    setTimeout(() => console.log('closed'), 100);
  `
  const result = spawnSync(process.execPath, ['--input-type=module', '-e', source], {
    encoding: 'utf8',
    timeout: 5000
  })

  assert.ifError(result.error)
  assert.equal(result.status, 0, result.stderr)
  assert.equal(result.stdout.trim(), 'closed')
  assert.equal(result.stderr, '')
})

test('takeover observes server errors and removes only its own listeners', async t => {
  const raw = await listen(t)
  const externalErrors = []
  const externalError = error => externalErrors.push(error)
  const externalConnection = () => {}
  raw.on('error', externalError)
  raw.on('connection', externalConnection)
  const server = WSServer.takeover(raw)
  t.after(() => server.close())
  const failed = new Promise(resolve => server.onIssue('server_error', resolve))
  const first = new Error('External listener failed')
  raw.emit('error', first)
  assert.equal((await failed).error, first)

  server.close()
  assert.deepEqual(raw.listeners('error'), [externalError])
  assert.deepEqual(raw.listeners('connection'), [externalConnection])
  const second = new Error('Still owned by the application')
  raw.emit('error', second)
  assert.deepEqual(externalErrors, [first, second])
  assert.ok(raw.address())
})

test('takeover ignores connections already closed by an application listener', async t => {
  const raw = await listen(t)
  raw.on('connection', socket => socket.close(1008, 'Rejected by application'))
  const server = WSServer.takeover(raw)
  t.after(() => server.close())
  let connected = 0
  server.onConnect(() => connected++)
  const socket = new WebSocket(`ws://127.0.0.1:${raw.address().port}`)
  t.after(() => socket.terminate())
  const [code] = await once(socket, 'close')

  assert.equal(code, 1008)
  assert.equal(connected, 0)
  assert.equal(server.sessions.size, 0)
})

test('attach rejects non-open sockets without creating sessions or rooms', async t => {
  const raw = await listen(t)
  const server = new WSServer()
  t.after(() => server.close())
  let connected = 0
  server.onConnect(session => {
    connected++
    session.join('unexpected')
  })
  const socket = new WebSocket(`ws://127.0.0.1:${raw.address().port}`)
  t.after(() => socket.terminate())

  assert.equal(socket.readyState, WebSocket.CONNECTING)
  assert.throws(() => server.attach(socket), /open WebSocket/)
  await once(socket, 'open')
  const closed = once(socket, 'close')
  socket.close()
  assert.equal(socket.readyState, WebSocket.CLOSING)
  assert.throws(() => server.attach(socket), /open WebSocket/)
  await closed
  assert.equal(socket.readyState, WebSocket.CLOSED)
  assert.throws(() => server.attach(socket), /open WebSocket/)
  assert.equal(connected, 0)
  assert.equal(server.sessions.size, 0)
  assert.equal(server.rooms.size, 0)
})

test('close releases values captured by server observers while the host stays alive', () => {
  const entry = import.meta.resolve('../packages/server/dist/index.js')
  const source = `
    import assert from 'node:assert/strict';
    import { setImmediate as nextTurn } from 'node:timers/promises';
    import { WSServer } from ${JSON.stringify(entry)};
    const server = new WSServer();
    function register(registerHandler) {
      const resource = {};
      registerHandler(() => { Object.keys(resource); });
      return new WeakRef(resource);
    }
    const resources = [
      register(fn => server.onConnect(fn)),
      register(fn => server.onDisconnect(fn)),
      register(fn => server.onIssue(fn)),
      register(fn => server.onIssue('server_error', fn))
    ];
    await nextTurn();
    global.gc();
    assert.ok(resources.every(ref => ref.deref()), 'registered observers retain resources');
    server.close();
    for (let attempt = 0; attempt < 10; attempt++) {
      await nextTurn();
      global.gc();
      if (resources.every(ref => !ref.deref())) break;
    }
    assert.ok(resources.every(ref => !ref.deref()), 'closed host releases resources');
    assert.equal(server.sessions.size, 0);
  `
  const result = spawnSync(
    process.execPath,
    ['--expose-gc', '--input-type=module', '-e', source],
    {
      encoding: 'utf8',
      timeout: 5000
    }
  )
  assert.ifError(result.error)
  assert.equal(result.status, 0, result.stderr)
})
