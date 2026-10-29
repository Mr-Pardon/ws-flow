import test from 'node:test'
import assert from 'node:assert/strict'
import { RuntimeCore } from '../packages/runtime-core/dist/index.js'
import { SessionScheduler, QueueCapacityError } from '../packages/scheduler/dist/index.js'

function deferred() {
  let resolve
  const promise = new Promise(done => {
    resolve = done
  })
  return { promise, resolve }
}

function setup(t, direction) {
  const scheduler = new SessionScheduler({ maxQueued: 2, maxQueuedPerPeer: 1 })
  const sent = [],
    issues = [],
    gate = deferred(),
    entered = deferred()
  const core = new RuntimeCore({
    base: {
      transport: {
        send: (peer, message) => {
          sent.push({ peer, message })
        }
      }
    },
    schedulers: {
      in: direction === 'receive' ? scheduler : new SessionScheduler(),
      out: direction === 'send' ? scheduler : new SessionScheduler()
    }
  })
  core.useQueueErrorObserver(info => {
    issues.push(info)
  })
  const peer = { id: 'a', state: 'open' }
  core.pipelines[direction === 'receive' ? 'useReceive' : 'useSend'](async ctx => {
    if (ctx.envelope.data === 'blocked') {
      entered.resolve(ctx.signal)
      await gate.promise
    }
  })
  t.after(() => {
    core.dispose()
    gate.resolve()
  })
  return { core, scheduler, sent, issues, gate, entered, peer }
}

test(
  'ordinary outbound saturation rejects only that send and later sends remain usable',
  { timeout: 5000 },
  async t => {
    const { core, peer, entered, gate, scheduler, sent, issues } = setup(t, 'send')
    const first = core.send(peer, 'blocked')
    await entered.promise
    const queued = core.send(peer, 'queued')
    assert.equal(scheduler.queuedCount, 1)
    await assert.rejects(core.send(peer, 'overflow'), QueueCapacityError)
    assert.equal(issues.length, 1)
    assert.equal(issues[0].fatal, false)
    assert.equal(issues[0].direction, 'send')
    gate.resolve()
    await Promise.all([first, queued])
    await core.send(peer, 'after')
    assert.deepEqual(
      sent.map(item => item.message.data),
      ['blocked', 'queued', 'after']
    )
    assert.equal(scheduler.queuedCount, 0)
  }
)

test(
  'aborting a waiting send releases queue capacity before blocked work finishes',
  { timeout: 5000 },
  async t => {
    const { core, peer, entered, gate, scheduler, sent } = setup(t, 'send')
    const first = core.send(peer, 'blocked')
    await entered.promise
    const controller = new AbortController(),
      reason = new Error('cancel queued')
    const waiting = assert.rejects(
      core.send(peer, 'cancelled', { signal: controller.signal }),
      error => error === reason
    )
    assert.equal(scheduler.queuedCount, 1)
    controller.abort(reason)
    await waiting
    assert.equal(scheduler.queuedCount, 0)
    const replacement = core.send(peer, 'replacement')
    gate.resolve()
    await Promise.all([first, replacement])
    assert.deepEqual(
      sent.map(item => item.message.data),
      ['blocked', 'replacement']
    )
  }
)

for (const direction of ['send', 'receive']) {
  test(
    `${direction} closePeer promptly rejects running waits and removes queued work`,
    { timeout: 5000 },
    async t => {
      const { core, peer, entered, gate, scheduler, sent } = setup(t, direction)
      const reason = new Error('peer shutdown')
      const dispatch = value =>
        direction === 'send'
          ? core.send(peer, value)
          : core.dispatchers.receive.dispatch(peer, { data: value })
      const first = assert.rejects(dispatch('blocked'), error => error === reason)
      const signal = await entered.promise
      const queued = assert.rejects(dispatch('queued'), error => error === reason)
      const subscription = core.subscribe(() => true, { peer })
      core.closePeer(peer, reason)
      await Promise.all([first, queued])
      assert.equal(signal.aborted, true)
      assert.equal(scheduler.queuedCount, 0)
      assert.equal(subscription.closed, true)
      gate.resolve()
      await new Promise(resolve => setImmediate(resolve))
      assert.equal(sent.length, 0)
    }
  )
}

test(
  'receive admission failure settles tasks and other peers continue',
  { timeout: 5000 },
  async t => {
    const { core, peer, entered, gate, scheduler, issues } = setup(t, 'receive')
    const first = assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'blocked' }),
      QueueCapacityError
    )
    await entered.promise
    const queued = assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'queued' }),
      QueueCapacityError
    )
    const request = assert.rejects(
      core.request(peer, 'pending', null),
      QueueCapacityError
    )
    const scoped = core.subscribe(() => true, { peer })
    await assert.rejects(
      core.dispatchers.receive.dispatch(peer, { data: 'overflow' }),
      QueueCapacityError
    )
    await Promise.all([first, queued, request])
    assert.equal(issues.length, 1)
    assert.equal(issues[0].fatal, true)
    assert.equal(issues[0].direction, 'receive')
    assert.equal(scoped.closed, true)
    assert.equal(scheduler.queuedCount, 0)
    const other = { id: 'b', state: 'open' },
      subscription = core.subscribe(() => true, { peer: other })
    await core.dispatchers.receive.dispatch(other, { data: 'unaffected' })
    assert.equal((await subscription.next()).value.data, 'unaffected')
    gate.resolve()
  }
)

test(
  'outbound task/control saturation closes the peer instead of losing control frames',
  { timeout: 5000 },
  async t => {
    const { core, peer, entered, scheduler, issues } = setup(t, 'send')
    const first = assert.rejects(core.send(peer, 'blocked'), QueueCapacityError)
    await entered.promise
    const queued = assert.rejects(core.send(peer, 'queued'), QueueCapacityError)
    await assert.rejects(
      core.dispatchers.send.dispatch(peer, { kind: 'stream_cancel', streamId: 's' }),
      QueueCapacityError
    )
    await Promise.all([first, queued])
    assert.equal(issues.length, 1)
    assert.equal(issues[0].fatal, true)
    assert.equal(scheduler.queuedCount, 0)
    await assert.rejects(core.send(peer, 'later'), /closed/)
  }
)

test(
  'dispose releases pending work on schedulers even after scheduler replacement',
  { timeout: 5000 },
  async t => {
    const { core, peer, entered, scheduler } = setup(t, 'send')
    const first = assert.rejects(core.send(peer, 'blocked'), /closed/)
    await entered.promise
    const queued = assert.rejects(core.send(peer, 'queued'), /closed/)
    core.schedulers.setOutScheduler(new SessionScheduler())
    core.dispose()
    await Promise.all([first, queued])
    assert.equal(scheduler.queuedCount, 0)
  }
)
