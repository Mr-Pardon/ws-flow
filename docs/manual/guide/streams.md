# 流式任务

一次任务会不断产生结果时，用 `requestStream` 逐个接收。处理方用 `yield` 交付分片，调用方用 `for await` 消费；任务结束，循环也随之结束。

它适合进度更新、逐段生成、分页读取等场景，不需要另外维护“开始、分片、结束”事件之间的关联。

## 请求一次任务，连续读取结果

下面用三条固定进度展示流的写法。真实业务中，在每个阶段完成后 `yield` 当前进度即可。先启动 `server.ts`，再运行 `client.ts`。

::: code-group

```ts [server.ts]
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleStream('progress', async function* () {
  yield 20
  yield 60
  yield 100
})
```

```ts [client.ts]
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })
await client.connect('ws://127.0.0.1:8080')

for await (const progress of client.requestStream<number>('progress', null)) {
  console.log(`进度：${progress}%`)
}
```

:::

`null` 表示这个任务不需要输入；需要参数时，放在同一位置，处理器通过 `ctx.data` 读取。

生成器正常结束会自动结束结果流。例子保持连接，便于继续发起任务；应用退出时调用 `client.dispose()` 和 `server.close()`。

## 不需要后续结果时，直接退出循环

`break` 会结束消费，并尽力通知远端取消任务：

```ts
import { WSClient } from '@ws-flow/client/node'

export async function readFirstProgress(client: WSClient) {
  const task = client.requestStream<number>('progress', null)

  for await (const progress of task) {
    console.log('首次进度：', progress)
    break
  }
}
```

这里传入已连接的 Client。`requestStream` 直接返回 `StreamTask`，创建时无需 `await`。除了迭代，它还提供 `state`、`closed` 和 `cancel(reason?)`，便于按钮或页面生命周期控制任务。

显式调用 `task.cancel()` 后，后续读取会拒绝；`break` 则正常退出当前循环。已经创建、尚未开始迭代的任务，也应在不再使用时取消。

处理器可以把 `ctx.signal` 传给支持取消的业务操作，让远端工作响应取消信号。完整的进度与取消交互见[可取消任务案例](/examples/cancellable-task)。

## 无需先请求，也可以主动推送流

采样批次等数据可能由发送方主动产生。这时用 `stream` 发送，用 `onStream` 接收。两者和前面的请求流使用相同的生成器、异步迭代写法：

::: code-group

```ts [server.ts]
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.onStream('samples', async ctx => {
  if (!ctx.stream) return

  for await (const sample of ctx.stream) {
    console.log('收到采样：', sample)
  }
})
```

```ts [client.ts]
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })
await client.connect('ws://127.0.0.1:8080')

await client.stream('samples', async function* () {
  yield { value: 10 }
  yield { value: 12 }
  yield { value: 11 }
})
```

:::

同样先运行接收端，再运行发送端。`stream(...)` 的 Promise 表示本地流发送完成，不表示对方的业务已经处理完毕；需要业务结果时使用请求模式。

反向推送时，由 Server 调用 `session.stream(...)`，Client 用 `onStream(...)` 接收。

## 两种流如何选择

| 业务过程 | 发起入口 | 对端入口 |
| --- | --- | --- |
| 请求一个任务，连续读取它的结果 | `requestStream` | `handleStream` |
| 直接把一批数据连续发给对方 | `stream` | `onStream` |

两种方式都在断连时清理对应任务。生成器抛错也会使流失败，接收方可以在 `for await` 外用 `try/catch` 处理。

::: details 期限、缓冲与取消的细节

**请求总期限。** `requestStream` 的 `timeoutMs` 默认 30 秒，覆盖整个请求，不会随分片到达延长。长任务可以设置适合业务的总期限。

**流空闲期限。** `idleTimeoutMs` 限制流开始后连续没有有效进度的时间；`idleTimeoutMs: null` 关闭当前本地流的空闲期限，不会改变对端配置。两种期限的区别见[资源配置](/reference/resources)。

**接收容量。** 接收流默认最多缓存 128 个未消费分片；请求流可以用 `capacity` 调整。超限会失败并清理任务，不会静默丢弃分片。这个本地缓冲限制不会自动暂停远端生产，也不是端到端背压协议。

**业务取消。** 请求流的处理器使用 `ctx.signal`。主动发送流则接受 `signal` 选项；生成器工厂没有隐式注入的上下文，可以通过闭包引用同一个信号并传给业务操作。取消会尽力通知远端，不会回滚已完成的业务。

**多目标发送。** 向多个 Session 推送时，需要传入生成器工厂，为每个目标分别创建生成器；一个生成器实例不能供多个目标重复读取。

**输入校验。** 类型参数只描述开发时的约定。真实接口仍应校验网络输入，见[请求与响应](/guide/requests)。

:::
