import test from 'node:test'
import assert from 'node:assert/strict'
import { RuntimeCore } from '../packages/runtime-core/dist/index.js'

function setup(t) {
  const sent = [],
    peer = { id: 'original', state: 'open' }
  const core = new RuntimeCore({
    base: {
      transport: {
        send: (target, message) => {
          sent.push({ target, message })
        }
      }
    }
  })
  t.after(() => core.dispose())
  return { core, peer, sent }
}

const changes = {
  streamId: 'redirected',
  streamState: 'end',
  event: 'other',
  kind: 'request_stream',
  requestId: 'injected',
  resultMode: 'stream'
}

for (const direction of ['send', 'receive']) {
  for (const [field, value] of Object.entries(changes)) {
    test(`${direction} middleware cannot change proactive stream ${field}`, async t => {
      const { core, peer, sent } = setup(t)
      core.pipelines[direction === 'send' ? 'useSend' : 'useReceive'](ctx => {
        ctx.envelope[field] = value
      })
      const frame = { event: 'feed', streamId: 'original-stream', streamState: 'start' }
      await assert.rejects(
        core.dispatchers[direction].dispatch(peer, frame),
        /control field/
      )
      assert.equal(sent.filter(item => item.message.streamState).length, 0)
      if (direction === 'receive') {
        assert.deepEqual(
          sent.map(item => item.message.streamId),
          ['original-stream']
        )
        assert.equal(sent[0].message.kind, 'stream_cancel')
      }
    })
  }

  test(`${direction} middleware cannot strip stream identity or change peer`, async t => {
    for (const rewrite of [
      ctx => {
        ctx.envelope = { data: 1 }
      },
      ctx => {
        ctx.peer = { id: 'other', state: 'open' }
      }
    ]) {
      const { core, peer } = setup(t)
      core.pipelines[direction === 'send' ? 'useSend' : 'useReceive'](rewrite)
      await assert.rejects(
        core.dispatchers[direction].dispatch(peer, {
          streamId: 's',
          streamState: 'start'
        }),
        /cannot be changed/
      )
    }
  })

  test(`${direction} middleware cannot promote ordinary data into a stream`, async t => {
    const { core, peer } = setup(t)
    core.pipelines[direction === 'send' ? 'useSend' : 'useReceive'](ctx => {
      ctx.envelope.streamId = 'injected'
      ctx.envelope.streamState = 'start'
    })
    await assert.rejects(
      core.dispatchers[direction].dispatch(peer, { data: 1 }),
      /control field/
    )
  })
}

test('rejected active chunk fails its consumer and cancels the original stream once', async t => {
  const { core, peer, sent } = setup(t)
  let stream
  core.pipelines.useReceiveEvent({
    matcher: 'feed',
    middlewares: ctx => {
      stream = ctx.stream
    }
  })
  core.pipelines.useReceive(ctx => {
    if (ctx.envelope.streamState === 'chunk') ctx.envelope.streamId = 'wrong'
  })
  await core.dispatchers.receive.dispatch(peer, {
    event: 'feed',
    streamId: 's',
    streamState: 'start'
  })
  const waiting = assert.rejects(stream.next(), /control field/)
  await assert.rejects(
    core.dispatchers.receive.dispatch(peer, {
      streamId: 's',
      streamState: 'chunk',
      data: 1
    }),
    /control field/
  )
  await waiting
  assert.equal(stream.isClosed(), true)
  assert.deepEqual(
    sent.map(item => [item.message.kind, item.message.streamId]),
    [['stream_cancel', 's']]
  )
})

test('blocking a proactive frame rejects instead of reporting silent send success', async t => {
  const { core, peer, sent } = setup(t)
  core.pipelines.useSend(ctx => ctx.stop())
  const stream = core.createSendStream(peer, 'feed')
  await assert.rejects(
    stream.runGenerator(
      (async function* () {
        yield 1
      })()
    ),
    /blocked/
  )
  assert.equal(stream.isClosed(), true)
  assert.equal(sent.length, 0)
})

test('stream hooks reject state or peer rewrites but allow payload transformations', async t => {
  for (const direction of ['send', 'receive']) {
    for (const rewrite of [
      ctx => {
        ctx.state = 'end'
      },
      ctx => {
        ctx.peer = { id: 'other', state: 'open' }
      }
    ]) {
      const { core, peer } = setup(t)
      core.pipelines[direction === 'send' ? 'useSendStream' : 'useReceiveStream'](rewrite)
      if (direction === 'send') {
        const stream = core.createSendStream(peer, 'feed')
        await assert.rejects(
          stream.runGenerator(
            (async function* () {
              yield 1
            })()
          ),
          /cannot be changed/
        )
      } else {
        await assert.rejects(
          core.dispatchers.receive.dispatch(peer, {
            streamId: 's',
            streamState: 'start'
          }),
          /cannot be changed/
        )
      }
    }
  }
  const { core, peer } = setup(t)
  let stream
  core.pipelines.useReceive(ctx => {
    if (ctx.envelope.streamState === 'chunk') ctx.envelope.data *= 2
  })
  core.pipelines.useReceiveStream(ctx => {
    if (ctx.state === 'chunk') ctx.data += 1
  })
  core.pipelines.useReceiveEvent({
    matcher: 'feed',
    middlewares: ctx => {
      stream = ctx.stream
    }
  })
  await core.dispatchers.receive.dispatch(peer, {
    event: 'feed',
    streamId: 's',
    streamState: 'start'
  })
  await core.dispatchers.receive.dispatch(peer, {
    streamId: 's',
    streamState: 'chunk',
    data: 3
  })
  assert.equal((await stream.next()).value, 7)
})
