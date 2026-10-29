import test from 'node:test'
import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import {
  ImmediateScheduler,
  SessionScheduler,
  KeyedSerialScheduler,
  PriorityScheduler,
  ConcurrencyScheduler,
  ComposableScheduler,
  QueueCapacityError
} from '../packages/scheduler/dist/index.js'

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

const factories = [
  ['keyed', limits => new KeyedSerialScheduler(() => 'shared', limits)],
  ['session', limits => new SessionScheduler(limits)],
  ['priority', limits => new PriorityScheduler(limits)],
  ['concurrency', limits => new ConcurrencyScheduler(1, limits)]
]

function blockPeers(scheduler, name, peers) {
  const gate = deferred()
  const running = (name === 'session' ? peers : [peers[0]]).map(peerId =>
    scheduler.schedule({ peerId, run: () => gate.promise })
  )
  return {
    release: async () => {
      gate.resolve()
      await Promise.all(running)
    }
  }
}

test('queue limits require positive safe integers and reject malformed options', () => {
  for (const [, create] of factories) {
    for (const field of ['maxQueued', 'maxQueuedPerPeer']) {
      for (const limit of [0, -1, 0.5, NaN, Infinity, -Infinity, '2', null, 2 ** 53]) {
        assert.throws(() => create({ [field]: limit }), /positive safe integer/)
      }
      assert.doesNotThrow(() => create({ [field]: undefined }))
      assert.doesNotThrow(() => create({ [field]: Number.MAX_SAFE_INTEGER }))
    }
    for (const options of [null, false, 1, '1', []]) {
      assert.throws(() => create(options), /must be an object/)
    }
  }
})

for (const [name, create] of factories) {
  test(`${name}: global and peer quotas count waiting work independently`, async () => {
    const scheduler = create({ maxQueued: 3, maxQueuedPerPeer: 1 })
    const blocker = blockPeers(scheduler, name, ['a', 'b', 'c'])
    const controller = new AbortController()
    const reason = new Error('cancel waiting work')
    const schedule = peerId =>
      scheduler.schedule({
        peerId,
        signal: controller.signal,
        run: async () => assert.fail('cancelled work ran')
      })
    const a = schedule('a')
    const aRejected = assert.rejects(a, error => error === reason)
    assert.equal(scheduler.queuedCount, 1)

    await assert.rejects(schedule('a'), error => {
      assert.ok(error instanceof QueueCapacityError)
      assert.equal(error.name, 'QueueCapacityError')
      assert.equal(error.code, 'QUEUE_CAPACITY_EXCEEDED')
      assert.equal(error.scope, 'peer')
      assert.equal(error.peerId, 'a')
      assert.equal(error.limit, 1)
      return true
    })
    assert.equal(scheduler.queuedCount, 1)

    const bRejected = assert.rejects(schedule('b'), error => error === reason)
    const cRejected = assert.rejects(schedule('c'), error => error === reason)
    assert.equal(scheduler.queuedCount, 3)
    await assert.rejects(schedule('b'), error => {
      assert.equal(error.scope, 'global')
      assert.equal(error.limit, 3)
      return true
    })

    controller.abort(reason)
    assert.equal(scheduler.queuedCount, 0)
    await Promise.all([aRejected, bRejected, cRejected])
    await blocker.release()
  })

  test(`${name}: abort removes blocked tasks and releases capacity before the blocker ends`, async () => {
    const scheduler = create({ maxQueued: 32, maxQueuedPerPeer: 32 })
    const blocker = blockPeers(scheduler, name, ['a'])
    const controller = new AbortController()
    const reason = { message: 'the original reason' }
    const cancelled = Array.from({ length: 32 }, () =>
      assert.rejects(
        scheduler.schedule({
          peerId: 'a',
          signal: controller.signal,
          run: async () => assert.fail('aborted task was retained for execution')
        }),
        error => error === reason
      )
    )

    assert.equal(scheduler.queuedCount, 32)
    assert.equal(getEventListeners(controller.signal, 'abort').length, 1)
    controller.abort(reason)
    assert.equal(scheduler.queuedCount, 0)
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)

    // A blocked queue must release captured payloads without waiting for its runner.
    const queue = scheduler._queues ?? scheduler._queue
    assert.equal(Array.isArray(queue) ? queue.length : queue.size, 0)
    await Promise.all(cancelled)

    let admitted = false
    const replacement = scheduler.schedule({
      peerId: 'a',
      run: async () => {
        admitted = true
      }
    })
    assert.equal(scheduler.queuedCount, 1)
    assert.equal(admitted, false)
    await blocker.release()
    await replacement
    assert.equal(admitted, true)
    assert.equal(scheduler.queuedCount, 0)
  })

  test(`${name}: aborting running work does not release its execution slot early`, async () => {
    const scheduler = create({ maxQueued: 1 })
    const gate = deferred()
    const controller = new AbortController()
    let runningSettled = false
    const order = []
    const running = scheduler
      .schedule({
        peerId: 'a',
        signal: controller.signal,
        run: async () => {
          order.push('start')
          await gate.promise
          order.push('finish')
        }
      })
      .finally(() => {
        runningSettled = true
      })
    const waiting = scheduler.schedule({
      peerId: 'a',
      run: async () => {
        order.push('next')
      }
    })

    controller.abort(new Error('running code must cooperate'))
    await Promise.resolve()
    assert.equal(runningSettled, false)
    assert.equal(scheduler.queuedCount, 1)
    assert.deepEqual(order, ['start'])
    gate.resolve()
    await Promise.all([running, waiting])
    assert.deepEqual(order, ['start', 'finish', 'next'])
    assert.equal(scheduler.queuedCount, 0)
  })

  test(`${name}: starting and failing queued work releases its listener and reservation`, async () => {
    const scheduler = create({ maxQueued: 1, maxQueuedPerPeer: 1 })
    const blocker = blockPeers(scheduler, name, ['a'])
    const controller = new AbortController()
    const failure = new Error('task failure')
    const rejected = assert.rejects(
      scheduler.schedule({
        peerId: 'a',
        signal: controller.signal,
        run: () => {
          assert.equal(scheduler.queuedCount, 0)
          assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
          throw failure
        }
      }),
      error => error === failure
    )

    await blocker.release()
    await rejected
    await scheduler.schedule({ peerId: 'a', run: async () => {} })
    assert.equal(scheduler.queuedCount, 0)
  })
}

test('pre-aborted tasks never enter any scheduler or call key selectors', async () => {
  const controller = new AbortController()
  controller.abort(null)
  const schedulers = [
    ...factories.map(([, create]) => create({ maxQueued: 1 })),
    new ImmediateScheduler(),
    new KeyedSerialScheduler(() => assert.fail('key selector must not run')),
    new ComposableScheduler([
      { schedule: async () => assert.fail('composed scheduler must not run') }
    ])
  ]

  for (const scheduler of schedulers) {
    await assert.rejects(
      scheduler.schedule({
        signal: controller.signal,
        run: async () => assert.fail('pre-aborted task ran')
      }),
      reason => reason === null
    )
    if ('queuedCount' in scheduler) assert.equal(scheduler.queuedCount, 0)
  }
})

test('composed schedulers recheck cancellation at each layer', async () => {
  const controller = new AbortController()
  const reason = new Error('cancel between layers')
  const scheduler = new ComposableScheduler([
    {
      schedule: async task => {
        controller.abort(reason)
        await task.run()
      }
    },
    { schedule: async () => assert.fail('inner scheduler ran after abort') }
  ])

  await assert.rejects(
    scheduler.schedule({
      signal: controller.signal,
      run: async () => assert.fail('task ran after abort')
    }),
    error => error === reason
  )
})

test('priority cancellation preserves FIFO ties and removes empty priority buckets', async () => {
  const scheduler = new PriorityScheduler({ maxQueued: 4 })
  const blocker = blockPeers(scheduler, 'priority', ['a'])
  const controller = new AbortController()
  const order = []
  const task = (priority, value, signal) =>
    scheduler.schedule({
      priority,
      signal,
      run: async () => {
        order.push(value)
      }
    })
  const low = task(1, 'low')
  const highFirst = task(5, 'first')
  const removed = assert.rejects(task(10, 'removed', controller.signal), /removed/)
  const highLast = task(5, 'last')
  controller.abort(new Error('removed'))
  await removed
  assert.equal(scheduler.queuedCount, 3)

  await blocker.release()
  await Promise.all([low, highFirst, highLast])
  assert.deepEqual(order, ['first', 'last', 'low'])
})

test('a shared signal uses one listener across scheduler instances and removes it on drain', async () => {
  const schedulers = [new PriorityScheduler(), new ConcurrencyScheduler(1)]
  const blockers = schedulers.map(scheduler => blockPeers(scheduler, '', ['a']))
  const controller = new AbortController()
  const pending = schedulers.map(scheduler =>
    scheduler.schedule({ signal: controller.signal, run: async () => {} })
  )
  assert.equal(getEventListeners(controller.signal, 'abort').length, 1)
  await blockers[0].release()
  await pending[0]
  assert.equal(getEventListeners(controller.signal, 'abort').length, 1)
  await blockers[1].release()
  await pending[1]
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('keyed quotas are based on task peer identity, not the selected serial key', async () => {
  const scheduler = new KeyedSerialScheduler(task => task.room, {
    maxQueuedPerPeer: 1
  })
  const gate = deferred()
  const running = ['room-a', 'room-b'].map(room =>
    scheduler.schedule({ room, peerId: 'owner', run: () => gate.promise })
  )
  const first = scheduler.schedule({
    room: 'room-a',
    peerId: 'owner',
    run: async () => {}
  })
  await assert.rejects(
    scheduler.schedule({ room: 'room-b', peerId: 'owner', run: async () => {} }),
    error => error.scope === 'peer'
  )
  const other = scheduler.schedule({
    room: 'room-a',
    peerId: 'other',
    run: async () => {}
  })
  assert.equal(scheduler.queuedCount, 2)
  gate.resolve()
  await Promise.all([...running, first, other])
})

test('missing peer identities share one peer quota and option mutation cannot change limits', async () => {
  const options = { maxQueuedPerPeer: 1 }
  const scheduler = new ConcurrencyScheduler(1, options)
  options.maxQueuedPerPeer = 10
  const blocker = blockPeers(scheduler, '', ['a'])
  const first = scheduler.schedule({ run: async () => {} })

  await assert.rejects(
    scheduler.schedule({ run: async () => assert.fail('over limit') }),
    error => error.scope === 'peer' && error.peerId === undefined && error.limit === 1
  )
  await blocker.release()
  await first
})
