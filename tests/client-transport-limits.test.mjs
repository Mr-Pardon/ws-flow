import test from 'node:test'
import assert from 'node:assert/strict'
import { WSClient } from '../packages/client/dist/browser.js'
import { TransportLimitError } from '../packages/runtime-core/dist/index.js'

const identityCodec = { encode: value => value, decode: value => value }
const tick = () => new Promise(resolve => setImmediate(resolve))
const deferred = () => {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

class Socket extends EventTarget {
  readyState = 1
  binaryType = 'arraybuffer'
  bufferedAmount = 0
  sent = []
  closeCode

  send(data) {
    this.sent.push(data)
  }

  receive(data) {
    this.dispatchEvent(Object.assign(new Event('message'), { data }))
  }

  close(code) {
    this.closeCode = code
    this.readyState = 3
    this.dispatchEvent(new Event('close'))
  }
}

function attach(t, options = {}, send) {
  const socket = new Socket()
  const client = WSClient.takeover(
    { socket, send: send ?? (data => socket.send(data)) },
    {
      codec: identityCodec,
      ...options
    }
  )
  const issues = []
  client.onIssue(error => issues.push(error))
  t.after(() => client.dispose())
  return { client, socket, issues }
}

test('client validates byte and watermark limits before connecting', () => {
  for (const value of [0, -1, NaN, Infinity, 1.5, null]) {
    assert.throws(
      () => new WSClient({ transportLimits: { receive: { maxMessageBytes: value } } })
    )
    assert.throws(
      () => new WSClient({ transportLimits: { send: { maxPendingBytes: value } } })
    )
  }
  assert.throws(() => new WSClient({ transportLimits: { send: { lowWaterMark: 1 } } }))
  assert.throws(
    () =>
      new WSClient({ transportLimits: { send: { highWaterMark: 10, lowWaterMark: 10 } } })
  )
})

test('client rejects oversized UTF-8 input before decoding and closes once', async t => {
  let decoded = 0
  const { client, socket, issues } = attach(t, {
    codec: {
      encode: value => value,
      decode: value => {
        decoded++
        return value
      }
    },
    transportLimits: { receive: { maxMessageBytes: 5 } }
  })
  socket.receive('中文')
  await tick()
  assert.equal(decoded, 0)
  assert.equal(client.state, 'closed')
  assert.equal(socket.closeCode, 4009)
  assert.equal(issues.length, 1)
  assert.ok(issues[0] instanceof TransportLimitError)
  assert.equal(issues[0].code, 'MESSAGE_TOO_LARGE')
  assert.equal(issues[0].actual, 6)
})

test('client counts only the received binary slice and releases after dispatch', async t => {
  const { client, socket, issues } = attach(t, {
    transportLimits: { receive: { maxMessageBytes: 2, maxPendingBytes: 2 } }
  })
  const messages = client.subscribe(() => true)
  const backing = new Uint8Array(100),
    slice = backing.subarray(40, 42)
  socket.receive(slice)
  assert.deepEqual((await messages.next()).value.raw, slice)
  await tick()
  socket.receive(slice)
  assert.deepEqual((await messages.next()).value.raw, slice)
  assert.deepEqual(issues, [])
})

test('client receive byte budget includes active decoding and drops old queued frames', async t => {
  const gate = deferred(),
    entered = deferred(),
    decoded = []
  t.after(() => gate.resolve())
  const { client, socket, issues } = attach(t, {
    transportLimits: { receive: { maxPendingBytesPerPeer: 8 } },
    codec: {
      encode: value => value,
      decode: async value => {
        decoded.push(value)
        if (value === 'aaaa') {
          entered.resolve()
          await gate.promise
        }
        return value
      }
    }
  })
  socket.receive('aaaa')
  await entered.promise
  socket.receive('bbbb')
  socket.receive('c')
  await tick()
  assert.equal(issues.length, 1)
  assert.equal(issues[0].code, 'PENDING_BYTES_EXCEEDED')
  assert.equal(issues[0].scope, 'peer')
  assert.equal(socket.closeCode, 4009)
  const next = new Socket()
  client.attach({ socket: next, send: data => next.send(data) })
  const messages = client.subscribe(() => true)
  next.receive('new')
  assert.equal((await messages.next()).value.raw, 'new')
  gate.resolve()
  await tick()
  assert.deepEqual(decoded, ['aaaa', 'new'])
})

test('client measures encoded output once, rejects oversized notifications and stays usable', async t => {
  let encoded = 0
  const { client, socket, issues } = attach(t, {
    transportLimits: { send: { maxMessageBytes: 3, maxPendingBytes: 3 } },
    codec: {
      encode: value => {
        encoded++
        return value
      },
      decode: value => value
    }
  })
  await assert.rejects(client.send('中文'), { code: 'MESSAGE_TOO_LARGE', actual: 6 })
  await client.send('中')
  await client.send(new Uint8Array(100).subarray(40, 43))
  assert.equal(encoded, 3)
  assert.equal(client.state, 'open')
  assert.equal(socket.sent.length, 2)
  assert.equal(issues.length, 1)
})

test('client releases outgoing bytes when its transport rejects', async t => {
  const failed = new Error('write failed')
  let calls = 0
  const { client } = attach(
    t,
    {
      transportLimits: { send: { maxPendingBytes: 3 } }
    },
    async () => {
      if (++calls === 1) throw failed
    }
  )
  await assert.rejects(client.send('abc'), error => error === failed)
  await client.send('abc')
  assert.equal(calls, 2)
})

test(
  'client cancels watermark waiting without sending and can reuse the reservation',
  { timeout: 3000 },
  async t => {
    const encoded = deferred(),
      controller = new AbortController()
    const { client, socket, issues } = attach(t, {
      transportLimits: {
        send: {
          highWaterMark: 10,
          lowWaterMark: 2,
          pollIntervalMs: 1,
          maxPendingBytes: 3
        }
      },
      codec: {
        encode: value => {
          encoded.resolve()
          return value
        },
        decode: value => value
      }
    })
    socket.bufferedAmount = 9
    const sending = client.send('abc', { signal: controller.signal })
    const canceled = assert.rejects(sending, /cancel waiting/)
    await encoded.promise
    controller.abort(new Error('cancel waiting'))
    await canceled
    socket.bufferedAmount = 0
    await client.send('abc')
    assert.deepEqual(socket.sent, ['abc'])
    assert.deepEqual(issues, [])
  }
)

test(
  'client send buffer timeout releases bytes and rejects only the notification',
  { timeout: 3000 },
  async t => {
    const { client, socket, issues } = attach(t, {
      transportLimits: {
        send: {
          highWaterMark: 10,
          waitTimeoutMs: 15,
          pollIntervalMs: 1,
          maxPendingBytes: 3
        }
      }
    })
    socket.bufferedAmount = 9
    await assert.rejects(client.send('abc'), { code: 'SEND_BUFFER_TIMEOUT' })
    assert.equal(client.state, 'open')
    socket.bufferedAmount = 0
    await client.send('abc')
    assert.deepEqual(socket.sent, ['abc'])
    assert.equal(issues.length, 1)
  }
)

test('client requires bufferedAmount only when a send watermark is configured', async t => {
  const { client, socket } = attach(t, {
    transportLimits: { send: { highWaterMark: 10 } }
  })
  delete socket.bufferedAmount
  await assert.rejects(client.send('a'), { code: 'INVALID_BUFFERED_AMOUNT' })
  const compatible = attach(t)
  delete compatible.socket.bufferedAmount
  await compatible.client.send('a')
  assert.deepEqual(compatible.socket.sent, ['a'])
})

test('client detaching during encoding never sends or retains bytes on a replacement connection', async t => {
  let client,
    detached = false
  const setup = attach(t, {
    transportLimits: { send: { maxPendingBytes: 3 } },
    codec: {
      encode: value => {
        if (!detached) {
          detached = true
          client.detach()
        }
        return value
      },
      decode: value => value
    }
  })
  client = setup.client
  await assert.rejects(client.send('old'), /detached|closed/)
  const replacement = new Socket()
  client.attach({ socket: replacement, send: data => replacement.send(data) })
  await client.send('new')
  assert.deepEqual(setup.socket.sent, [])
  assert.deepEqual(replacement.sent, ['new'])
})

test(
  'client reconnecting while waiting cannot retarget an old outgoing message',
  { timeout: 3000 },
  async t => {
    const encoded = deferred()
    const { client, socket } = attach(t, {
      transportLimits: {
        send: { highWaterMark: 10, maxPendingBytes: 3, pollIntervalMs: 1 }
      },
      codec: {
        encode: value => {
          encoded.resolve()
          return value
        },
        decode: value => value
      }
    })
    socket.bufferedAmount = 10
    const sending = client.send('old')
    const rejected = assert.rejects(sending, /detached|closed/)
    await encoded.promise
    client.detach()
    const replacement = new Socket()
    client.attach({ socket: replacement, send: data => replacement.send(data) })
    await rejected
    await client.send('new')
    assert.deepEqual(socket.sent, [])
    assert.deepEqual(replacement.sent, ['new'])
  }
)

test('client rejects an oversized request fatally without an orphan cancel frame', async t => {
  const { client, socket, issues } = attach(t, {
    protocol: 'ws-flow',
    codec: { encode: JSON.stringify, decode: JSON.parse },
    transportLimits: { send: { maxMessageBytes: 1 } }
  })
  await assert.rejects(client.request('run', null, { timeoutMs: null }), {
    code: 'MESSAGE_TOO_LARGE'
  })
  await tick()
  assert.equal(client.state, 'closed')
  assert.equal(socket.closeCode, 4009)
  assert.deepEqual(socket.sent, [])
  assert.equal(issues.length, 1)
})
