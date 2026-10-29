import test from 'node:test'
import assert from 'node:assert/strict'
import { composeMiddleware, PipelineRegistry } from '../packages/pipeline/dist/index.js'

test('middleware nests in order and waits for asynchronous downstream work', async () => {
  const order = []
  const run = composeMiddleware([
    async (ctx, next) => {
      order.push('before')
      await next()
      order.push(ctx.value)
    },
    async ctx => {
      await Promise.resolve()
      ctx.value = 'after'
    }
  ])
  await run({})
  assert.deepEqual(order, ['before', 'after'])
})

test('middleware short circuit is local to each invocation', async () => {
  const run = composeMiddleware([
    (ctx, next) => {
      if (!ctx.stop) return next()
    },
    ctx => {
      ctx.reached = true
    }
  ])
  const stopped = { stop: true },
    allowed = { stop: false }
  await Promise.all([run(stopped), run(allowed)])
  assert.equal(stopped.reached, undefined)
  assert.equal(allowed.reached, true)
})

test('synchronous and asynchronous failures retain their original reason', async () => {
  const error = new Error('original')
  for (const middleware of [
    () => {
      throw error
    },
    async () => {
      throw error
    }
  ]) {
    await assert.rejects(composeMiddleware([middleware])({}), value => value === error)
  }
})

test('repeated next is rejected and a later invocation remains usable', async () => {
  let calls = 0
  const run = composeMiddleware([
    async (ctx, next) => {
      await next()
      if (ctx.repeat) await next()
    },
    () => {
      calls++
    }
  ])
  await assert.rejects(run({ repeat: true }), /multiple times/)
  await run({ repeat: false })
  assert.equal(calls, 2)
})

test('registry expands plugins into all six pipelines in registration order', () => {
  const registry = new PipelineRegistry(),
    first = () => {},
    second = () => {}
  const a = { matcher: 'notice', middlewares: first }
  const b = { matcher: /notice/, middlewares: second }
  registry.use([
    {
      receive: first,
      send: first,
      receiveStream: first,
      sendStream: first,
      receiveEvent: a,
      sendEvent: a
    },
    {
      receive: [second],
      send: [second],
      receiveStream: [second],
      sendStream: [second],
      receiveEvent: [b],
      sendEvent: [b]
    }
  ])
  for (const name of [
    'receiveMiddlewares',
    'sendMiddlewares',
    'receiveStreamMiddlewares',
    'sendStreamMiddlewares'
  ]) {
    assert.deepEqual(registry[name], [first, second])
  }
  assert.deepEqual(registry.receiveEventMiddlewares, [a, b])
  assert.deepEqual(registry.sendEventMiddlewares, [a, b])
})
