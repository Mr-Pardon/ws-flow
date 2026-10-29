# 事件与订阅

用 `emit` 告诉对方“发生了什么”，用 `on` 监听自己关心的事件。需要更灵活的分流时，再用 `subscribe` 按条件筛选消息。

发送方不需要登记回调或等待结果，接收方也不需要维护一个集中的消息分发函数。

## 发送通知，按名称接收

下面把一条 `notice` 通知从 Client 发送到 Server。先启动 `server.ts`，再运行 `client.ts`。

::: code-group

```ts [server.ts]
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

const stop = server.on('notice', data => {
  console.log('收到通知：', data)
})
```

```ts [client.ts]
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })
await client.connect('ws://127.0.0.1:8080')

await client.emit('notice', { text: '你好，ws-flow' })
```

:::

`on` 返回取消订阅的函数，这里保存为 `stop`。不再关心该事件时调用 `stop()`，其他订阅会继续工作。

这两个示例保持连接，供应用继续收发消息。应用退出时调用 `client.dispose()` 和 `server.close()`。`emit` 完成表示本地发送完成，不代表对方已经处理；需要确认结果时，使用[请求与响应](/guide/requests)。

## 从完整消息流中筛出一组事件

假设设备服务用 `device:online`、`device:offline` 等事件报告状态。一个订阅就能把这组事件汇集到同一个处理循环中：

```ts
import { WSClient } from '@ws-flow/client/node'

export async function watchDevices(client: WSClient, signal: AbortSignal) {
  const updates = client.subscribe(
    (_, message) => message.event?.startsWith('device:') === true,
    { signal }
  )

  for await (const message of updates) {
    console.log(message.event, message.data)
  }
}
```

调用时传入已连接的 Client，以及一个 `AbortController` 的 `signal`。循环会持续等待匹配消息；页面卸载或停止观察时，调用该 controller 的 `abort()` 即可结束订阅。

筛选函数的两个参数分别是解码后的原始消息 `raw` 和统一的消息视图 `message`。适配器负责解释 `message.event`、`message.data`，`message.peer` 表示来源连接。因此，除了事件名，你也可以按原始消息内容或来源连接筛选。

筛选函数必须**同步返回布尔值**。异步业务放在 `for await` 循环或 `on` 回调中处理。

## 每个订阅都有自己的消费过程

同一消息可以同时交给多个匹配订阅。例如，一个订阅更新界面，另一个记录日志：它们各自拥有队列和载荷快照，不会相互取走消息。`peer` 仍指向实际连接对象。

同一个订阅对象则对应一个队列。多个循环共享它时会分摊消息；希望各自收到完整副本，就分别调用 `subscribe`。

`on` 也基于订阅工作。同一回调的多次执行会依次完成；不同订阅可以独立消费。

## 停止观察与重连

- 调用 `subscription.cancel()` 或退出 `for await` 循环，会结束该订阅。
- 取消传入的 `signal` 也会结束订阅。这是本地停止观察，不会自动给对端发送取消命令。
- Client 断连后，旧订阅结束。重连时应在新的 `open` 状态重新注册，见[连接与清理](/guide/lifecycle)。
- Server 全局订阅可继续观察其他 Session；限定到某个 Session 的订阅随该连接清理。

::: details 慢消费与异常

默认每个订阅最多缓存 128 条未消费消息，可用 `capacity` 调整。超出容量时，这个订阅会失败，后续迭代抛出 `SubscriptionOverflowError`；库不会静默丢弃旧数据。

`on` 回调抛错也会结束该订阅，并进入异常观察入口。对可以恢复的业务错误，可以在回调内部捕获；完整分类见[错误处理](/reference/errors)。

:::

## 选择合适的消息入口

| 你想做什么 | 使用 |
| --- | --- |
| 发送一条带事件名的通知 | `emit(event, data)` |
| 发送协议中的普通数据 | `send(data)` |
| 观察某类通知 | `on` / `subscribe` |
| 等待某次操作的返回值 | `request` |
| 连续读取某次任务的结果 | `requestStream` |

订阅只观察普通通知，不重复接收请求响应、取消帧或流控制帧。

ws-flow 模式可以直接使用 `emit`。独立 Client 的 generic 模式需要先提供 `writeEvent` 映射，见[按需扩展](/guide/extensions)。
