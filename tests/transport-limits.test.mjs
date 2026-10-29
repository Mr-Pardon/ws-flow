import test from 'node:test'
import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import { setTimeout as delay } from 'node:timers/promises'
import {
  ByteBudget,
  TransportLimitError,
  validateSendLimits,
  waitForWritable,
  wireByteLength
} from '../packages/runtime-core/dist/index.js'

test('wire byte sizes match UTF-8 including paired and unpaired surrogates', () => {
  for (const value of [
    '',
    'abc',
    '你好',
    'é𝄞🙂',
    '\ud800',
    '\udc00',
    '\ud800x',
    '\ud800\ud800\udc00'
  ]) {
    assert.equal(wireByteLength(value), Buffer.byteLength(value, 'utf8'))
  }
})

test('wire byte sizes respect buffer view offsets and fragmented messages', () => {
  const buffer = new ArrayBuffer(128)
  assert.equal(wireByteLength(buffer), 128)
  assert.equal(wireByteLength(new Uint16Array(buffer, 8, 3)), 6)
  assert.equal(wireByteLength(new DataView(buffer, 20, 7)), 7)
  assert.equal(wireByteLength(Buffer.from('hello').subarray(1, 3)), 2)
  assert.equal(wireByteLength([Buffer.from('你'), new Uint8Array(buffer, 20, 4)]), 7)
  assert.equal(wireByteLength(new Blob(['你', '🙂'])), 7)
  assert.equal(wireByteLength([]), 0)
})

test('byte counting rejects values that are not supported wire data', () => {
  for (const value of [
    null,
    undefined,
    {},
    1,
    ['text'],
    [new ArrayBuffer(1)],
    [[Buffer.alloc(1)]]
  ]) {
    assert.throws(() => wireByteLength(value), {
      name: 'TransportLimitError',
      code: 'UNSUPPORTED_WIRE_DATA'
    })
  }
})

test('byte budget validates limits and snapshots options', () => {
  for (const limits of [null, [], 1, '1']) assert.throws(() => new ByteBudget(limits))
  for (const field of ['maxMessageBytes', 'maxPendingBytes', 'maxPendingBytesPerPeer']) {
    for (const value of [null, 0, -1, 0.5, Infinity, NaN, 2 ** 53, '1']) {
      assert.throws(() => new ByteBudget({ [field]: value }), RangeError)
    }
  }
  assert.equal(new ByteBudget().enabled, false)
  const options = { maxMessageBytes: 2 }
  const budget = new ByteBudget(options)
  options.maxMessageBytes = 100
  assert.equal(budget.enabled, true)
  assert.throws(() => budget.reserve({}, 3), { code: 'MESSAGE_TOO_LARGE' })
  assert.equal(budget.pendingBytes, 0)
})

test('message byte rejection is atomic and exposes structured diagnostic fields', () => {
  const budget = new ByteBudget({ maxMessageBytes: 3 })
  const peer = {}
  assert.throws(
    () => budget.reserve(peer, 4),
    error => {
      assert.ok(error instanceof TransportLimitError)
      assert.equal(error.code, 'MESSAGE_TOO_LARGE')
      assert.equal(error.scope, 'message')
      assert.equal(error.limit, 3)
      assert.equal(error.actual, 4)
      return true
    }
  )
  assert.equal(budget.pendingBytes, 0)
  assert.equal(budget.getPendingBytes(peer), 0)
  const release = budget.reserve(peer, 3)
  release()
  release()
  assert.equal(budget.pendingBytes, 0)
})

test('global pending bytes count all reservations and recover on release', () => {
  const budget = new ByteBudget({ maxPendingBytes: 7 })
  const a = {},
    b = {}
  const releaseA = budget.reserve(a, 4)
  const releaseB = budget.reserve(b, 3)
  assert.throws(() => budget.reserve(b, 1), {
    code: 'PENDING_BYTES_EXCEEDED',
    scope: 'global',
    limit: 7,
    actual: 8
  })
  assert.equal(budget.pendingBytes, 7)
  assert.equal(budget.getPendingBytes(a), 4)
  assert.equal(budget.getPendingBytes(b), 3)
  releaseA()
  const replacement = budget.reserve(b, 4)
  releaseB()
  replacement()
  assert.equal(budget.pendingBytes, 0)
})

test('per-peer budgets isolate distinct objects with reusable connection IDs', () => {
  const budget = new ByteBudget({ maxPendingBytesPerPeer: 4 })
  const a = { id: 'same' },
    b = { id: 'same' }
  const releaseA = budget.reserve(a, 4)
  const releaseB = budget.reserve(b, 4)
  assert.throws(() => budget.reserve(a, 1), {
    code: 'PENDING_BYTES_EXCEEDED',
    scope: 'peer',
    limit: 4,
    actual: 5
  })
  assert.equal(budget.pendingBytes, 8)
  releaseA()
  releaseB()
  assert.equal(budget.pendingBytes, 0)
})

test('closing a peer invalidates old releases without touching new reservations', () => {
  const budget = new ByteBudget({ maxPendingBytes: 10 })
  const peer = {},
    other = {}
  const stale = budget.reserve(peer, 4)
  const releaseOther = budget.reserve(other, 3)
  budget.closePeer(peer)
  budget.closePeer(peer)
  const current = budget.reserve(peer, 6)
  stale()
  assert.equal(budget.pendingBytes, 9)
  assert.equal(budget.getPendingBytes(peer), 6)
  current()
  releaseOther()
  assert.equal(budget.pendingBytes, 0)
})

test('clearing all budgets invalidates stale releases and supports reuse', () => {
  const budget = new ByteBudget()
  const peer = {}
  const stale = budget.reserve(peer, 7)
  budget.clear()
  const current = budget.reserve(peer, 2)
  stale()
  assert.equal(budget.pendingBytes, 2)
  current()
  assert.equal(budget.pendingBytes, 0)
  const empty = budget.reserve(peer, 0)
  empty()
  assert.equal(budget.getPendingBytes(peer), 0)
})

test('reservation sizes and global accounting stay within safe integer precision', () => {
  const budget = new ByteBudget()
  for (const bytes of [null, undefined, -1, 0.5, Infinity, NaN, 2 ** 53]) {
    assert.throws(() => budget.reserve({}, bytes), RangeError)
  }
  const release = budget.reserve({}, Number.MAX_SAFE_INTEGER)
  assert.throws(() => budget.reserve({}, 1), { code: 'PENDING_BYTES_EXCEEDED' })
  assert.equal(budget.pendingBytes, Number.MAX_SAFE_INTEGER)
  release()
  assert.equal(budget.pendingBytes, 0)
})

test('send water mark validation supplies defaults without mutating options', () => {
  const options = { maxMessageBytes: 20, highWaterMark: 9 }
  assert.deepEqual(validateSendLimits(options), {
    ...options,
    lowWaterMark: 4,
    waitTimeoutMs: 10000,
    pollIntervalMs: 10
  })
  assert.deepEqual(options, { maxMessageBytes: 20, highWaterMark: 9 })
  assert.deepEqual(validateSendLimits(), {})
  assert.deepEqual(validateSendLimits({ highWaterMark: 1, lowWaterMark: 0 }), {
    highWaterMark: 1,
    lowWaterMark: 0,
    waitTimeoutMs: 10000,
    pollIntervalMs: 10
  })
})

test('send water marks reject ambiguous or invalid configurations immediately', () => {
  for (const options of [null, [], '1']) assert.throws(() => validateSendLimits(options))
  for (const options of [
    { lowWaterMark: 0 },
    { waitTimeoutMs: 5 },
    { pollIntervalMs: 5 },
    { highWaterMark: null },
    { highWaterMark: 0 },
    { highWaterMark: Infinity },
    { highWaterMark: 2, lowWaterMark: null },
    { highWaterMark: 2, lowWaterMark: 2 },
    { highWaterMark: 2, lowWaterMark: -1 },
    { highWaterMark: 2, lowWaterMark: 0.5 },
    { highWaterMark: 2, waitTimeoutMs: 0 },
    { highWaterMark: 2, waitTimeoutMs: null },
    { highWaterMark: 2, waitTimeoutMs: 2 ** 31 },
    { highWaterMark: 2, pollIntervalMs: NaN },
    { highWaterMark: 2, pollIntervalMs: 0.5 },
    { highWaterMark: 2, pollIntervalMs: 2 ** 31 }
  ])
    assert.throws(() => validateSendLimits(options), RangeError)
})

test('disabled water marks do not require bufferedAmount but still honor abort', async () => {
  await waitForWritable({}, 0, {})
  const controller = new AbortController()
  const reason = new Error('cancelled')
  controller.abort(reason)
  await assert.rejects(
    waitForWritable({}, 1, {}, controller.signal),
    error => error === reason
  )
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('water marks allow immediate sends up to the high mark and reject oversized frames', async () => {
  const controller = new AbortController()
  const socket = { readyState: 1, bufferedAmount: 7 }
  await waitForWritable(socket, 3, { highWaterMark: 10 }, controller.signal)
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  await assert.rejects(waitForWritable(socket, 11, { highWaterMark: 10 }), {
    code: 'MESSAGE_TOO_LARGE',
    scope: 'message',
    limit: 10,
    actual: 11
  })
})

test('water marks reject missing or invalid socket buffer measurements', async () => {
  for (const bufferedAmount of [undefined, null, -1, NaN, Infinity, '0']) {
    const controller = new AbortController()
    await assert.rejects(
      waitForWritable(
        { readyState: 1, bufferedAmount },
        1,
        { highWaterMark: 10 },
        controller.signal
      ),
      { code: 'INVALID_BUFFERED_AMOUNT' }
    )
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  }
})

test('a blocked send waits for low mark and enough space for its complete frame', async () => {
  const socket = { readyState: 1, bufferedAmount: 10 }
  const controller = new AbortController()
  let settled = false
  const pending = waitForWritable(
    socket,
    8,
    {
      highWaterMark: 10,
      lowWaterMark: 3,
      pollIntervalMs: 2,
      waitTimeoutMs: 5000
    },
    controller.signal
  ).then(() => {
    settled = true
  })
  socket.bufferedAmount = 3
  await delay(10)
  assert.equal(settled, false)
  socket.bufferedAmount = 2
  await pending
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('hysteresis prevents resume above the low mark even with room below the high mark', async () => {
  const socket = { readyState: 1, bufferedAmount: 10 }
  let settled = false
  const pending = waitForWritable(socket, 2, {
    highWaterMark: 10,
    lowWaterMark: 3,
    pollIntervalMs: 2,
    waitTimeoutMs: 5000
  }).then(() => {
    settled = true
  })
  socket.bufferedAmount = 6
  await delay(10)
  assert.equal(settled, false)
  socket.bufferedAmount = 3
  await pending
})

test('water mark timeout releases timers and abort listeners', async () => {
  let reads = 0
  const socket = {
    readyState: 1,
    get bufferedAmount() {
      reads++
      return 10
    }
  }
  const controller = new AbortController()
  await assert.rejects(
    waitForWritable(
      socket,
      1,
      {
        highWaterMark: 10,
        pollIntervalMs: 1000,
        waitTimeoutMs: 10
      },
      controller.signal
    ),
    { code: 'SEND_BUFFER_TIMEOUT' }
  )
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  const completedReads = reads
  await delay(15)
  assert.equal(reads, completedReads)
})

test('aborting a blocked send preserves the reason and removes all later polling', async () => {
  let reads = 0
  const socket = {
    readyState: 1,
    get bufferedAmount() {
      reads++
      return 10
    }
  }
  const controller = new AbortController()
  const pending = waitForWritable(
    socket,
    1,
    {
      highWaterMark: 10,
      pollIntervalMs: 2,
      waitTimeoutMs: 1000
    },
    controller.signal
  )
  const reason = new Error('caller stopped')
  controller.abort(reason)
  await assert.rejects(pending, error => error === reason)
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  const completedReads = reads
  await delay(10)
  assert.equal(reads, completedReads)
})

test('a drain observed after the deadline cannot revive an expired send', async t => {
  let now = 0
  t.mock.method(performance, 'now', () => now)
  const socket = { readyState: 1, bufferedAmount: 10 }
  const pending = waitForWritable(socket, 1, {
    highWaterMark: 10,
    pollIntervalMs: 2,
    waitTimeoutMs: 10
  })
  now = 11
  socket.bufferedAmount = 0
  await assert.rejects(pending, { code: 'SEND_BUFFER_TIMEOUT' })
})

test('socket close while waiting rejects and clears cancellation listeners', async () => {
  const socket = { readyState: 1, bufferedAmount: 10 }
  const controller = new AbortController()
  const pending = waitForWritable(
    socket,
    1,
    {
      highWaterMark: 10,
      pollIntervalMs: 2,
      waitTimeoutMs: 1000
    },
    controller.signal
  )
  socket.readyState = 3
  await assert.rejects(pending, /WebSocket is not open/)
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  await assert.rejects(waitForWritable(socket, 1, { highWaterMark: 10 }), /not open/)
})
