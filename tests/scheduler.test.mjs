import test from 'node:test'
import assert from 'node:assert/strict'
import {
  ImmediateScheduler,
  SessionScheduler,
  KeyedSerialScheduler,
  PriorityScheduler,
  ConcurrencyScheduler,
  ComposableScheduler,
  SchedulerRuntime
} from '../packages/scheduler/dist/index.js'

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

test('concurrency rejects invalid limits before accepting tasks', () => {
  for (const limit of [0, -1, 0.5, NaN, Infinity, -Infinity, '2', undefined, 2 ** 53]) {
    assert.throws(() => new ConcurrencyScheduler(limit), /positive safe integer/)
  }
})

test('concurrency bounds running work and releases slots after failure', async () => {
  const scheduler = new ConcurrencyScheduler(2),
    gate = deferred()
  let running = 0,
    peak = 0
  const started = []
  const tasks = Array.from({ length: 6 }, (_, index) =>
    scheduler.schedule({
      run: async () => {
        running++
        peak = Math.max(peak, running)
        started.push(index)
        try {
          await gate.promise
          if (index === 0) throw new Error('failed')
        } finally {
          running--
        }
      }
    })
  )
  const results = Promise.allSettled(tasks)
  assert.deepEqual(started, [0, 1])
  gate.resolve()
  assert.equal((await results).filter(result => result.status === 'rejected').length, 1)
  assert.equal(peak, 2)
  assert.equal(running, 0)
  assert.deepEqual(started, [0, 1, 2, 3, 4, 5])
})

test('session tasks preserve key order, allow other keys and recover from failure', async () => {
  const scheduler = new SessionScheduler(),
    gate = deferred(),
    events = []
  const first = scheduler.schedule({
    peerId: 'a',
    run: async () => {
      events.push('a1')
      await gate.promise
      throw new Error('failed')
    }
  })
  const rejected = assert.rejects(first, /failed/)
  const second = scheduler.schedule({
    peerId: 'a',
    run: async () => {
      events.push('a2')
    }
  })
  await scheduler.schedule({
    peerId: 'b',
    run: async () => {
      events.push('b')
    }
  })
  assert.deepEqual(events, ['a1', 'b'])
  gate.resolve()
  await Promise.all([rejected, second])
  assert.deepEqual(events, ['a1', 'b', 'a2'])
  await scheduler.schedule({
    peerId: 'a',
    run: async () => {
      events.push('a3')
    }
  })
  assert.equal(events.at(-1), 'a3')
})

test('keyless work and immediate scheduler execute without a serial wait', async () => {
  for (const scheduler of [
    new ImmediateScheduler(),
    new KeyedSerialScheduler(() => undefined)
  ]) {
    const gate = deferred()
    let later = false
    const first = scheduler.schedule({ run: () => gate.promise })
    await scheduler.schedule({
      run: async () => {
        later = true
      }
    })
    assert.equal(later, true)
    gate.resolve()
    await first
  }
})

test('priority validates input, is nonpreemptive and preserves equal-priority FIFO', async () => {
  const scheduler = new PriorityScheduler(),
    gate = deferred(),
    order = []
  const first = scheduler.schedule({
    priority: -1,
    run: async () => {
      order.push('running')
      await gate.promise
    }
  })
  for (const priority of [NaN, Infinity, -Infinity, '1', null]) {
    await assert.rejects(
      scheduler.schedule({ priority, run: async () => assert.fail('invalid task ran') }),
      /finite number/
    )
  }
  const queued = [0, 5, 5, -2].map((priority, index) =>
    scheduler.schedule({
      priority,
      run: async () => {
        order.push(index)
        if (index === 1) throw new Error('failed')
      }
    })
  )
  const results = Promise.allSettled(queued)
  assert.deepEqual(order, ['running'])
  gate.resolve()
  await first
  assert.equal((await results)[1].status, 'rejected')
  assert.deepEqual(order, ['running', 1, 2, 0, 3])
})

test('composed schedulers nest in declared order and reject repeated task execution', async () => {
  const events = []
  const layer = name => ({
    schedule: async task => {
      events.push(name)
      await task.run()
      events.push(`/${name}`)
    }
  })
  await new ComposableScheduler([layer('a'), layer('b')]).schedule({
    run: async () => {
      events.push('run')
    }
  })
  assert.deepEqual(events, ['a', 'b', 'run', '/b', '/a'])
  let runs = 0
  const duplicate = {
    schedule: async task => {
      await task.run()
      await task.run()
    }
  }
  await assert.rejects(
    new ComposableScheduler([duplicate]).schedule({
      run: async () => {
        runs++
      }
    }),
    /multiple times/
  )
  assert.equal(runs, 1)
})

test('runtime scheduler replacement leaves existing scheduled work with its owner', async () => {
  const old = new SessionScheduler(),
    runtime = new SchedulerRuntime(old),
    gate = deferred()
  const pending = runtime.inScheduler.schedule({ peerId: 'a', run: () => gate.promise })
  const replacement = new ImmediateScheduler()
  runtime.setInScheduler(replacement)
  assert.equal(runtime.inScheduler, replacement)
  assert.notEqual(runtime.outScheduler, replacement)
  runtime.setScheduler(replacement)
  assert.equal(runtime.outScheduler, replacement)
  gate.resolve()
  await pending
})
