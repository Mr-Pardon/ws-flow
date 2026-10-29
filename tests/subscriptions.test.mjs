import test from 'node:test'
import assert from 'node:assert/strict'
import {
  RuntimeCore,
  genericAdapter,
  SubscriptionOverflowError
} from '../packages/runtime-core/dist/index.js'

const peer = { id: 'same-id', state: 'open' }
const make = () =>
  new RuntimeCore({
    base: {
      protocol: genericAdapter(),
      transport: {
        send() {
          throw new Error('Subscriptions must not send')
        }
      }
    }
  })

test('raw subscription filters without event configuration and fans out independently', async () => {
  const core = make()
  const a = core.subscribe(raw => raw.type === 'progress')
  const b = core.subscribe(raw => raw.job === 42)
  await core.dispatchers.receive.dispatch(peer, { type: 'other', job: 0 })
  await core.dispatchers.receive.dispatch(peer, { type: 'progress', job: 42, value: 1 })
  assert.equal((await a.next()).value.raw.value, 1)
  assert.equal((await b.next()).value.raw.value, 1)
  core.clearSubscriptions()
  assert.equal((await a.next()).done, true)
})

test('predicate and consumer payload mutations do not affect other subscribers or input', async () => {
  const core = make()
  const a = core.subscribe(raw => {
    raw.nested.value = 99
    return true
  })
  const b = core.subscribe(() => true)
  const input = { nested: { value: 1 } }
  await core.dispatchers.receive.dispatch(peer, input)
  const first = (await a.next()).value
  first.raw.nested.value = 100
  const second = (await b.next()).value
  assert.equal(second.raw.nested.value, 1)
  assert.equal(input.nested.value, 1)
  assert.equal(second.peer, peer)
  assert.equal(second.raw, second.data)
  core.clearSubscriptions()
})

test('failed predicates and async predicates only fail their own subscription', async () => {
  const core = make()
  const bad = core.subscribe(() => {
    throw new Error('predicate failed')
  })
  const asyncBad = core.subscribe(async () => {
    throw new Error('async predicate')
  })
  const good = core.subscribe(() => true)
  await core.dispatchers.receive.dispatch(peer, 42)
  await assert.rejects(bad.next(), /predicate failed/)
  await assert.rejects(asyncBad.next(), /synchronously/)
  assert.equal((await good.next()).value.raw, 42)
  core.clearSubscriptions()
})

test('slow consumer overflow is bounded and does not stop another consumer', async () => {
  const core = make()
  const slow = core.subscribe(() => true, { capacity: 1 })
  const fast = core.subscribe(() => true, { capacity: 1 })
  await core.dispatchers.receive.dispatch(peer, 1)
  assert.equal((await fast.next()).value.raw, 1)
  await core.dispatchers.receive.dispatch(peer, 2)
  await assert.rejects(slow.next(), SubscriptionOverflowError)
  assert.equal((await fast.next()).value.raw, 2)
  assert.equal(slow.closed, true)
  core.clearSubscriptions()
})

test('abort and iterator return release pending reads and stop future delivery', async () => {
  const core = make()
  const controller = new AbortController()
  let matches = 0
  const subscription = core.subscribe(
    () => {
      matches++
      return true
    },
    { signal: controller.signal }
  )
  const pending = subscription.next()
  controller.abort()
  assert.equal((await pending).done, true)
  await core.dispatchers.receive.dispatch(peer, 1)
  assert.equal(matches, 0)
  const iterator = core.subscribe(() => true)
  await core.dispatchers.receive.dispatch(peer, 2)
  for await (const message of iterator) {
    assert.equal(message.raw, 2)
    break
  }
  assert.equal(iterator.closed, true)
  assert.equal((await iterator.next()).done, true)
  const preAborted = core.subscribe(() => true, { signal: controller.signal })
  assert.equal(preAborted.closed, true)
})

test('peer scope uses connection identity, while global subscriptions survive scoped cleanup', async () => {
  const core = make()
  const other = { ...peer }
  const scoped = core.subscribe(() => true, { peer })
  const global = core.subscribe(() => true)
  await core.dispatchers.receive.dispatch(other, 1)
  await core.dispatchers.receive.dispatch(peer, 2)
  assert.equal((await scoped.next()).value.raw, 2)
  assert.equal((await global.next()).value.raw, 1)
  core.clearSubscriptions(peer)
  assert.equal(scoped.closed, true)
  assert.equal(global.closed, false)
  core.clearSubscriptions()
})

test('receive pipeline filtering and transformed payload precede subscription delivery', async () => {
  const core = make()
  core.pipelines.useReceive(ctx => {
    if (ctx.envelope.data === 0) ctx.stop()
    else ctx.envelope.data *= 2
  })
  const subscription = core.subscribe(() => true)
  await core.dispatchers.receive.dispatch(peer, 0)
  await core.dispatchers.receive.dispatch(peer, 3)
  const { raw, data } = (await subscription.next()).value
  assert.equal(raw, 3)
  assert.equal(data, 6)
  core.clearSubscriptions()
})

test('invalid capacity and non-cloneable payloads fail explicitly', async () => {
  const core = make()
  for (const capacity of [0, -1, 1.5, Infinity]) {
    assert.throws(() => core.subscribe(() => true, { capacity }), /positive integer/)
  }
  const subscription = core.subscribe(() => true)
  await core.dispatchers.receive.dispatch(peer, { nonWireValue() {} })
  await assert.rejects(subscription.next(), error => error.name === 'DataCloneError')
  const healthy = core.subscribe(() => true)
  await core.dispatchers.receive.dispatch(peer, 'still running')
  assert.equal((await healthy.next()).value.raw, 'still running')
  core.clearSubscriptions()
})
