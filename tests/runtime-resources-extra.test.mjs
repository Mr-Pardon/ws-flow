import test from 'node:test'
import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import {
  RuntimeCore,
  ReceiveDispatcher,
  ReceiveStream
} from '../packages/runtime-core/dist/index.js'
import { SessionScheduler, QueueCapacityError } from '../packages/scheduler/dist/index.js'

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

const turn = () => new Promise(resolve => setImmediate(resolve))

for (const stage of ['middleware', 'transport']) {
  test(
    `cancelling a running send preserves FIFO while its ${stage} is pending`,
    {
      timeout: 5000
    },
    async t => {
      const scheduler = new SessionScheduler({ maxQueued: 2, maxQueuedPerPeer: 2 })
      const gate = deferred(),
        entered = deferred(),
        calls = []
      const core = new RuntimeCore({
        base: {
          transport: {
            send: async (_, message) => {
              calls.push(`transport:${message.data}`)
              if (stage === 'transport' && message.data === 'first') {
                entered.resolve()
                await gate.promise
              }
            }
          }
        },
        schedulers: { in: new SessionScheduler(), out: scheduler }
      })
      t.after(() => {
        gate.resolve()
        core.dispose()
      })
      core.pipelines.useSend(async ctx => {
        calls.push(`middleware:${ctx.envelope.data}`)
        if (stage === 'middleware' && ctx.envelope.data === 'first') {
          entered.resolve()
          await gate.promise
        }
      })
      const peer = { id: 'same-peer', state: 'open' }
      const controller = new AbortController(),
        reason = new Error('cancel first send')
      const first = assert.rejects(
        core.send(peer, 'first', { signal: controller.signal }),
        error => error === reason
      )
      await entered.promise
      const second = core.send(peer, 'second')
      controller.abort(reason)
      await first
      await turn()
      assert.equal(scheduler.queuedCount, 1)
      assert.deepEqual(
        calls,
        stage === 'middleware'
          ? ['middleware:first']
          : ['middleware:first', 'transport:first']
      )

      gate.resolve()
      await second
      assert.deepEqual(
        calls,
        stage === 'middleware'
          ? ['middleware:first', 'middleware:second', 'transport:second']
          : [
              'middleware:first',
              'transport:first',
              'middleware:second',
              'transport:second'
            ]
      )
      assert.equal(scheduler.queuedCount, 0)
    }
  )
}

for (const direction of ['send', 'receive']) {
  test(
    `closing a blocked ${direction} peer allows another peer to continue immediately`,
    {
      timeout: 5000
    },
    async t => {
      const scheduler = new SessionScheduler({ maxQueued: 2, maxQueuedPerPeer: 1 })
      const gate = deferred(),
        entered = deferred(),
        delivered = []
      const core = new RuntimeCore({
        base: {
          transport: {
            send: (peer, message) => {
              delivered.push([peer.id, message.data])
            }
          }
        },
        schedulers: {
          in: direction === 'receive' ? scheduler : new SessionScheduler(),
          out: direction === 'send' ? scheduler : new SessionScheduler()
        }
      })
      t.after(() => {
        gate.resolve()
        core.dispose()
      })
      const oldPeer = { id: 'old', state: 'open' },
        newPeer = { id: 'new', state: 'open' }
      core.pipelines[direction === 'receive' ? 'useReceive' : 'useSend'](async ctx => {
        if (ctx.peer === oldPeer) {
          entered.resolve()
          await gate.promise
        }
      })
      if (direction === 'receive') {
        core.pipelines.useReceiveEvent({
          matcher: () => true,
          middlewares: ctx => {
            delivered.push([ctx.peer.id, ctx.envelope.data])
          }
        })
      }
      const dispatch = (peer, data) =>
        direction === 'send'
          ? core.send(peer, data)
          : core.dispatchers.receive.dispatch(peer, { event: 'notice', data })
      const reason = new Error('old connection closed')
      const first = assert.rejects(
        dispatch(oldPeer, 'running'),
        error => error === reason
      )
      await entered.promise
      const queued = assert.rejects(
        dispatch(oldPeer, 'waiting'),
        error => error === reason
      )
      core.closePeer(oldPeer, reason)
      await Promise.all([first, queued])
      assert.equal(scheduler.queuedCount, 0)

      await dispatch(newPeer, 'new connection')
      assert.deepEqual(delivered, [['new', 'new connection']])
      gate.resolve()
      await turn()
      assert.deepEqual(delivered, [['new', 'new connection']])
    }
  )
}

test(
  'failing queue observers preserve the original rejection and complete peer cleanup',
  {
    timeout: 5000
  },
  async t => {
    const scheduler = new SessionScheduler({ maxQueued: 1, maxQueuedPerPeer: 1 })
    const gate = deferred(),
      entered = deferred(),
      observed = [],
      logged = [],
      failures = []
    const syncError = new Error('sync observer failure'),
      asyncError = new Error('async observer failure')
    const originalError = console.error.bind(console)
    t.mock.method(console, 'error', (...args) => {
      if (
        args[0] === '[RuntimeCore] queue observer failed' &&
        (args[1] === syncError || args[1] === asyncError)
      ) {
        logged.push(args[1])
        return
      }
      originalError(...args)
    })
    const core = new RuntimeCore({
      base: { transport: { send() {} } },
      schedulers: { in: scheduler, out: new SessionScheduler() }
    })
    t.after(() => {
      gate.resolve()
      core.dispose()
    })
    core.useQueueErrorObserver(() => {
      throw syncError
    })
    core.useQueueErrorObserver(async () => {
      throw asyncError
    })
    core.useQueueErrorObserver(info => {
      observed.push(info)
    })
    core.pipelines.useReceive(async () => {
      entered.resolve()
      await gate.promise
    })
    const peer = { id: 'overflow', state: 'open' }
    const subscription = core.subscribe(() => true, { peer })
    const captureFailure = error => {
      failures.push(error)
      return error instanceof QueueCapacityError
    }
    const first = assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'running' }),
      captureFailure
    )
    await entered.promise
    const queued = assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'waiting' }),
      captureFailure
    )
    await assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'overflow' }),
      captureFailure
    )
    await Promise.all([first, queued])
    await turn()
    assert.equal(observed.length, 1)
    assert.equal(observed[0].fatal, true)
    assert.equal(observed[0].direction, 'receive')
    assert.equal(failures.length, 3)
    for (const error of failures) assert.equal(error, observed[0].error)
    assert.deepEqual(new Set(logged), new Set([syncError, asyncError]))
    assert.equal(logged.length, 2)
    assert.equal(subscription.closed, true)
    assert.equal(scheduler.queuedCount, 0)
    await assert.rejects(core.send(peer, 'after failure'), /closed/)
  }
)

for (const stream of [false, true]) {
  test(
    `close during request acceptance prevents late ${stream ? 'stream creation' : 'notification delivery'}`,
    {
      timeout: 5000
    },
    async t => {
      const gate = deferred(),
        entered = deferred()
      const peer = { id: 'closing', state: 'open' }
      let published = 0,
        routed = 0,
        created = 0
      const dispatcher = new ReceiveDispatcher({
        pipelines: {
          receiveMiddlewares: [],
          receiveEventMiddlewares: [
            {
              matcher: () => true,
              middlewares: [
                () => {
                  routed++
                }
              ]
            }
          ],
          receiveStreamMiddlewares: []
        },
        schedulers: { inScheduler: new SessionScheduler() },
        invalidStreamObservers: [],
        acceptRequest: async () => {
          entered.resolve()
          await gate.promise
          return false
        },
        publish: () => {
          published++
        },
        createReceiveStream: target => {
          created++
          return new ReceiveStream(target, [])
        }
      })
      t.after(() => {
        gate.resolve()
        dispatcher.dispose()
      })
      const reason = new Error('closed during acceptance')
      const pending = assert.rejects(
        dispatcher.dispatch(
          peer,
          stream
            ? { event: 'values', streamId: 'late', streamState: 'start' }
            : { event: 'notification', data: 'late' }
        ),
        error => error === reason
      )
      await entered.promise
      dispatcher.closePeer(peer, reason)
      await pending
      gate.resolve()
      await turn()
      assert.equal(published, 0)
      assert.equal(routed, 0)
      assert.equal(created, 0)
    }
  )
}

test(
  'a subscription predicate closing its peer prevents later route invocation',
  {
    timeout: 5000
  },
  async t => {
    const core = new RuntimeCore({
      base: { transport: { send() {} } },
      schedulers: { in: new SessionScheduler(), out: new SessionScheduler() }
    })
    t.after(() => core.dispose())
    const peer = { id: 'predicate-close', state: 'open' },
      reason = new Error('closed by subscription predicate')
    let predicates = 0,
      routed = 0
    core.subscribe(() => {
      predicates++
      core.closePeer(peer, reason)
      return true
    })
    core.pipelines.useReceiveEvent({
      matcher: () => true,
      middlewares: () => {
        routed++
      }
    })
    await assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'close now' }),
      error => error === reason
    )
    await turn()
    assert.equal(predicates, 1)
    assert.equal(routed, 0)
  }
)

test(
  'shared cancellation signals use one listener and release it after settlement',
  {
    timeout: 5000
  },
  async t => {
    const scheduler = new SessionScheduler({ maxQueued: 32, maxQueuedPerPeer: 32 })
    const gate = deferred(),
      entered = deferred()
    const core = new RuntimeCore({
      base: { transport: { send() {} } },
      schedulers: { in: new SessionScheduler(), out: scheduler }
    })
    t.after(() => {
      gate.resolve()
      core.dispose()
    })
    core.pipelines.useSend(async ctx => {
      if (ctx.envelope.data === 'blocked') {
        entered.resolve()
        await gate.promise
      }
    })
    const peer = { id: 'shared-signal', state: 'open' },
      controller = new AbortController(),
      reason = new Error('cancel group')
    const signal = controller.signal
    const baseline = getEventListeners(signal, 'abort').length
    const pending = [
      assert.rejects(core.send(peer, 'blocked', { signal }), error => error === reason)
    ]
    await entered.promise
    for (let index = 0; index < 20; index++) {
      pending.push(
        assert.rejects(core.send(peer, index, { signal }), error => error === reason)
      )
    }
    assert.equal(getEventListeners(signal, 'abort').length, baseline + 1)
    assert.equal(scheduler.queuedCount, 20)
    controller.abort(reason)
    await Promise.all(pending)
    assert.equal(getEventListeners(signal, 'abort').length, baseline)
    assert.equal(scheduler.queuedCount, 0)

    gate.resolve()
    const completionSignal = new AbortController().signal
    await Promise.all([
      core.send(peer, 'one', { signal: completionSignal }),
      core.send(peer, 'two', { signal: completionSignal })
    ])
    assert.equal(getEventListeners(completionSignal, 'abort').length, 0)
  }
)
