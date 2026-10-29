import test from 'node:test'
import assert from 'node:assert/strict'
import {
  ActiveTaskBudget,
  IdleDeadline,
  TaskCapacityError,
  TaskTimeoutError,
  validateTaskTimeouts
} from '../packages/runtime-core/dist/index.js'

const kinds = [
  'incomingRequests',
  'outgoingRequests',
  'incomingStreams',
  'outgoingStreams'
]

function fakeClock(t) {
  let now = 0
  let nextId = 0
  const pending = new Map()
  const callbacks = []
  t.mock.method(performance, 'now', () => now)
  t.mock.method(globalThis, 'setTimeout', (callback, waitMs) => {
    const id = ++nextId
    pending.set(id, { callback, at: now + waitMs })
    callbacks.push(callback)
    return id
  })
  t.mock.method(globalThis, 'clearTimeout', id => pending.delete(id))
  return {
    pending,
    callbacks,
    advance(ms) {
      now += ms
    },
    runDue() {
      for (const [id, timer] of [...pending]) {
        if (timer.at > now) continue
        pending.delete(id)
        timer.callback()
      }
    }
  }
}

test('active task limits reject invalid fields and snapshot nested options', () => {
  for (const limits of [null, [], 1, '1'])
    assert.throws(() => new ActiveTaskBudget(limits), TypeError)
  for (const kind of kinds) {
    for (const value of [null, [], 1, '1'])
      assert.throws(() => new ActiveTaskBudget({ [kind]: value }), TypeError)
  }
  for (const field of ['maxActive', 'maxActivePerPeer']) {
    for (const value of [null, 0, -1, 0.5, Infinity, NaN, 2 ** 53, '1']) {
      assert.throws(() => new ActiveTaskBudget({ [field]: value }), RangeError)
      for (const kind of kinds) {
        assert.throws(
          () => new ActiveTaskBudget({ [kind]: { [field]: value } }),
          RangeError
        )
      }
    }
  }
  const limits = { outgoingStreams: { maxActive: 1 } }
  const budget = new ActiveTaskBudget(limits)
  limits.outgoingStreams.maxActive = 100
  budget.reserve({}, 'outgoingStreams')
  assert.throws(() => budget.reserve({}, 'outgoingStreams'), TaskCapacityError)
  assert.equal(budget.activeCount, 1)
})

test('default request capacities remain isolated by peer and direction', () => {
  const budget = new ActiveTaskBudget()
  const a = {},
    b = {}
  for (const kind of ['incomingRequests', 'outgoingRequests']) {
    for (let index = 0; index < 1024; index++) budget.reserve(a, kind)
    assert.throws(() => budget.reserve(a, kind), {
      name: 'TaskCapacityError',
      code: 'CAPACITY_EXCEEDED',
      kind,
      scope: 'peer',
      budget: kind,
      limit: 1024,
      actual: 1025
    })
    budget.reserve(b, kind)
    assert.equal(budget.getActiveCount(a, kind), 1024)
    assert.equal(budget.getActiveCount(undefined, kind), 1025)
  }
  for (const kind of ['incomingStreams', 'outgoingStreams']) {
    for (let index = 0; index < 1025; index++) budget.reserve(a, kind)
    assert.equal(budget.getActiveCount(a, kind), 1025)
  }
  assert.equal(budget.activeCount, 4100)
  budget.clear()
  assert.equal(budget.activeCount, 0)
})

test('explicit request capacities override defaults but omitted fields retain them', () => {
  const peer = {}
  const budget = new ActiveTaskBudget({
    incomingRequests: { maxActivePerPeer: 1025 },
    outgoingRequests: { maxActivePerPeer: undefined }
  })
  for (let index = 0; index < 1025; index++) budget.reserve(peer, 'incomingRequests')
  for (let index = 0; index < 1024; index++) budget.reserve(peer, 'outgoingRequests')
  assert.throws(() => budget.reserve(peer, 'incomingRequests'), { limit: 1025 })
  assert.throws(() => budget.reserve(peer, 'outgoingRequests'), { limit: 1024 })
})

test('shared global capacity counts mixed task kinds across peers', () => {
  const budget = new ActiveTaskBudget({ maxActive: 2 })
  const a = {},
    b = {}
  const release = budget.reserve(a, 'incomingRequests')
  budget.reserve(b, 'outgoingStreams')
  assert.throws(() => budget.reserve(b, 'incomingStreams'), {
    code: 'CAPACITY_EXCEEDED',
    kind: 'incomingStreams',
    budget: 'all',
    scope: 'global',
    limit: 2,
    actual: 3
  })
  assert.equal(budget.getActiveCount(b), 1)
  release()
  budget.reserve(b, 'incomingStreams')
  assert.equal(budget.activeCount, 2)
  assert.equal(budget.getActiveCount(a), 0)
})

test('shared peer capacity covers every kind while allowing other peers', () => {
  const budget = new ActiveTaskBudget({ maxActivePerPeer: 2 })
  const a = {},
    b = {}
  budget.reserve(a, 'incomingRequests')
  budget.reserve(a, 'outgoingRequests')
  assert.throws(() => budget.reserve(a, 'outgoingStreams'), {
    scope: 'peer',
    budget: 'all',
    actual: 3
  })
  budget.reserve(b, 'incomingStreams')
  assert.equal(budget.getActiveCount(a), 2)
  assert.equal(budget.activeCount, 3)
})

test('each category enforces its global and peer capacities independently', () => {
  for (const kind of kinds) {
    const budget = new ActiveTaskBudget({ [kind]: { maxActive: 2, maxActivePerPeer: 1 } })
    const a = {},
      b = {},
      c = {}
    budget.reserve(a, kind)
    assert.throws(() => budget.reserve(a, kind), {
      budget: kind,
      scope: 'peer',
      actual: 2,
      limit: 1
    })
    budget.reserve(b, kind)
    assert.throws(() => budget.reserve(c, kind), {
      budget: kind,
      scope: 'global',
      actual: 3,
      limit: 2
    })
    const otherKind = kinds.find(value => value !== kind)
    budget.reserve(c, otherKind)
    assert.equal(budget.activeCount, 3)
    assert.equal(budget.getActiveCount(undefined, kind), 2)
  }
})

test('category rejection never consumes shared capacity and releases are idempotent', () => {
  const budget = new ActiveTaskBudget({
    maxActive: 3,
    maxActivePerPeer: 2,
    incomingStreams: { maxActive: 1 }
  })
  const a = {},
    b = {}
  const release = budget.reserve(a, 'incomingStreams')
  assert.throws(() => budget.reserve(b, 'incomingStreams'), TaskCapacityError)
  assert.equal(budget.activeCount, 1)
  assert.equal(budget.getActiveCount(b), 0)
  budget.reserve(b, 'incomingRequests')
  budget.reserve(b, 'outgoingRequests')
  release()
  release()
  assert.equal(budget.activeCount, 2)
  budget.reserve(a, 'incomingStreams')
  assert.equal(budget.activeCount, 3)
})

test('closePeer releases only that peer and stale releases cannot alter reused peer', () => {
  const budget = new ActiveTaskBudget({ maxActive: 5 })
  const a = {},
    b = {}
  const stale = kinds.map(kind => budget.reserve(a, kind))
  const releaseB = budget.reserve(b, 'incomingRequests')
  budget.closePeer(a)
  budget.closePeer(a)
  assert.equal(budget.activeCount, 1)
  assert.equal(budget.getActiveCount(a), 0)
  const releaseNew = budget.reserve(a, 'outgoingStreams')
  stale.forEach(release => release())
  assert.equal(budget.getActiveCount(a), 1)
  assert.equal(budget.getActiveCount(undefined, 'outgoingStreams'), 1)
  releaseNew()
  releaseB()
  assert.equal(budget.activeCount, 0)
})

test('clear releases every counter and isolates late releases from new reservations', () => {
  const budget = new ActiveTaskBudget({ maxActive: 4 })
  const peer = {}
  const stale = kinds.map(kind => budget.reserve(peer, kind))
  budget.clear()
  budget.clear()
  for (const kind of kinds) assert.equal(budget.getActiveCount(undefined, kind), 0)
  budget.reserve(peer, 'incomingRequests')
  for (const release of stale) release()
  assert.equal(budget.activeCount, 1)
  assert.equal(budget.getActiveCount(peer), 1)
})

test('invalid runtime reservation inputs do not alter counters', () => {
  const budget = new ActiveTaskBudget()
  assert.throws(() => budget.reserve({}, 'unknown'), TypeError)
  for (const peer of [null, undefined, 0, 'peer'])
    assert.throws(() => budget.reserve(peer, 'incomingStreams'), TypeError)
  assert.throws(() => budget.getActiveCount(undefined, 'unknown'), TypeError)
  assert.equal(budget.activeCount, 0)
})

test('task timeout options default to disabled and snapshot valid millisecond values', () => {
  assert.deepEqual(validateTaskTimeouts(), {
    incomingRequestMs: null,
    streamIdleMs: null
  })
  assert.deepEqual(validateTaskTimeouts({ incomingRequestMs: null }), {
    incomingRequestMs: null,
    streamIdleMs: null
  })
  const options = { incomingRequestMs: 1, streamIdleMs: 2147483647 }
  const normalized = validateTaskTimeouts(options)
  options.incomingRequestMs = 9
  assert.deepEqual(normalized, { incomingRequestMs: 1, streamIdleMs: 2147483647 })
  for (const value of [null, [], 0, '1'])
    assert.throws(() => validateTaskTimeouts(value), TypeError)
  for (const field of ['incomingRequestMs', 'streamIdleMs']) {
    for (const value of [0, -1, 0.5, NaN, Infinity, 2147483648, '1'])
      assert.throws(() => validateTaskTimeouts({ [field]: value }), RangeError)
  }
})

test('task timeout errors expose stable machine-readable diagnostics', () => {
  for (const code of ['EXECUTION_TIMEOUT', 'STREAM_IDLE_TIMEOUT']) {
    const error = new TaskTimeoutError(code, 250)
    assert.ok(error instanceof Error)
    assert.equal(error.name, 'TaskTimeoutError')
    assert.equal(error.code, code)
    assert.equal(error.timeoutMs, 250)
    assert.match(error.message, /250 ms/)
  }
})

test('idle deadline stays unarmed until first touch and disabled deadlines never arm', t => {
  const clock = fakeClock(t)
  let expired = 0
  const deadline = new IdleDeadline(10, () => expired++)
  assert.equal(clock.pending.size, 0)
  clock.advance(100)
  clock.runDue()
  assert.equal(expired, 0)
  deadline.touch()
  assert.equal(clock.pending.size, 1)
  for (const value of [undefined, null]) {
    const disabled = new IdleDeadline(value, () => expired++)
    disabled.touch()
    disabled.touch()
    disabled.clear()
  }
  assert.equal(clock.pending.size, 1)
  deadline.clear()
  assert.equal(clock.pending.size, 0)
})

test('idle deadline renews and ignores callbacks belonging to older timers', t => {
  const clock = fakeClock(t)
  let expired = 0
  const deadline = new IdleDeadline(10, () => expired++)
  deadline.touch()
  const stale = clock.callbacks[0]
  clock.advance(5)
  deadline.touch()
  assert.equal(clock.pending.size, 1)
  clock.advance(5)
  stale()
  clock.runDue()
  assert.equal(expired, 0)
  clock.advance(5)
  clock.runDue()
  assert.equal(expired, 1)
  assert.equal(clock.pending.size, 0)
  deadline.touch()
  stale()
  assert.equal(expired, 1)
  assert.equal(clock.pending.size, 0)
})

test('touch after elapsed time expires synchronously instead of reviving a task', t => {
  const clock = fakeClock(t)
  let expired = 0
  let deadline
  deadline = new IdleDeadline(10, () => {
    expired++
    deadline.touch()
  })
  deadline.touch()
  clock.advance(10)
  deadline.touch()
  assert.equal(expired, 1)
  assert.equal(clock.pending.size, 0)
  clock.callbacks[0]()
  deadline.touch()
  assert.equal(expired, 1)
})

test('idle checks observe the current deadline without extending it', t => {
  const clock = fakeClock(t)
  let expired = 0
  const deadline = new IdleDeadline(10, () => expired++)
  deadline.check()
  assert.equal(clock.pending.size, 0)
  deadline.touch()
  const timer = clock.callbacks[0]
  clock.advance(9)
  deadline.check()
  assert.equal(expired, 0)
  assert.equal(clock.callbacks.length, 1)
  clock.advance(1)
  deadline.check()
  assert.equal(expired, 1)
  assert.equal(clock.pending.size, 0)
  deadline.check()
  deadline.touch()
  timer()
  assert.equal(expired, 1)
  assert.equal(clock.pending.size, 0)
})

test('idle checks remain inert after clear and for disabled deadlines', t => {
  const clock = fakeClock(t)
  let expired = 0
  const cleared = new IdleDeadline(10, () => expired++)
  cleared.touch()
  cleared.clear()
  const disabled = [undefined, null].map(
    value => new IdleDeadline(value, () => expired++)
  )
  for (const deadline of disabled) deadline.touch()
  clock.advance(100)
  for (const deadline of [cleared, ...disabled]) {
    deadline.check()
    deadline.touch()
    deadline.check()
  }
  clock.runDue()
  assert.equal(expired, 0)
  assert.equal(clock.pending.size, 0)
})

test('early timer callbacks rearm against the original monotonic deadline', t => {
  const clock = fakeClock(t)
  let expired = 0
  const deadline = new IdleDeadline(10, () => expired++)
  deadline.touch()
  clock.advance(4.5)
  clock.callbacks[0]()
  assert.equal(expired, 0)
  assert.equal(clock.pending.size, 1)
  clock.advance(6)
  clock.runDue()
  assert.equal(expired, 1)
  assert.equal(clock.pending.size, 0)
})

test('clear is terminal before and after first touch and cancels pending callbacks', t => {
  const clock = fakeClock(t)
  let expired = 0
  const unopened = new IdleDeadline(10, () => expired++)
  unopened.clear()
  unopened.touch()
  assert.equal(clock.pending.size, 0)
  const running = new IdleDeadline(10, () => expired++)
  running.touch()
  const stale = clock.callbacks[0]
  running.clear()
  running.clear()
  running.touch()
  clock.advance(100)
  stale()
  clock.runDue()
  assert.equal(expired, 0)
  assert.equal(clock.pending.size, 0)
})

test('idle deadline validates timeout values before installing timers', () => {
  for (const value of [0, -1, 0.5, Infinity, NaN, 2147483648, '1'])
    assert.throws(() => new IdleDeadline(value, () => {}), RangeError)
})
