# 按需扩展

ws-flow 把常用通信能力和可替换的处理环节分开：业务使用通知、请求和流，公共处理交给插件，外部消息格式交给 codec 与 protocol，执行顺序交给 scheduler。

你可以先使用默认配置，再只扩展需要变化的那一层。

## 用插件统一处理消息

例如，为服务端收到的消息统一记录日志，只需注册一次 `use()`，各个业务处理器就不必重复编写日志代码：

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.use({
  receive: async (ctx, next) => {
    console.log('收到消息：', ctx.session.id, ctx.envelope.event)
    await next()
  }
})

server.handleRequest('health', () => ({ ready: true }))
```

`receive` 在入站管线上执行，`await next()` 将处理交给后续中间件。插件就是这样的配置对象，不需要继承基类；同一个配置也可以抽到独立模块，供多个服务注册使用。

Client 同样提供 `use()`。两端的上下文都能访问消息，Client 通过 `peer` 访问连接，Server 还提供 `session`。服务退出时统一关闭 Server，连接的释放见[连接与生命周期](/guide/lifecycle)。

::: details 插件可以挂在哪里？

六类入口是 `receive`、`receiveEvent`、`receiveStream`、`send`、`sendEvent` 和 `sendStream`，分别面向整体消息、匹配事件与流阶段。流管线区分 start、chunk、end 和 error 阶段。

异步中间件应 `await next()` 或 `return next()`。不调用 `next` 只停止当前中间件链，不等于取消消息或任务；需要显式使用上下文的控制方法。

管线适合日志、校验和字段变换等短处理。等待反向请求、未来流分片等长业务应放在请求处理器或消费者中，以免阻塞后续帧。请求标识、流标识与控制类型等任务身份字段不能改写，内核会校验它们。

插件按注册顺序追加，不执行依赖排序或热替换。插件额外创建的计时器、监听器等资源由应用释放。

:::

::: details 如何停止消息处理或结束一条流？

普通 `receive` / `send` 中间件通过 `ctx.stop()` 阻止本次通知继续分发或发送。拦截请求、响应或流控制帧会使相应任务失败，避免调用方无限等待。接收事件管线在通知交给订阅后运行，不能撤回已经分发的通知；需要在分发前过滤时，使用 `receive`。

流中间件有独立的控制方法：

| 方法 | 接收流 | 发送流 |
| --- | --- | --- |
| `stop()` | 忽略当前 start/chunk，不向消费者交付；end/error 仍结束流 | 停止生产并正常结束本地流 |
| `end()` | 正常结束消费 | 发送结束帧并结束生产 |
| `fail(reason)` | 以错误结束消费 | 发送错误帧并以错误结束生产 |
| `reject(reason)` | 与接收侧 `stop()` 相同 | 以错误结束本地生产 |

同一次流阶段处理以最先调用的控制方法为准。已开始的流在本地提前结束时，宿主会尽力通知对端；业务资源仍需响应相应的 `signal` 并完成清理。

:::

## 用 protocol 适配已有消息格式

Client 可以独立接入已有 WebSocket 服务。例如，对方发送 `{ type: 'price', payload: ... }`，你只需要告诉 Client：`type` 是事件名，以及如何写出同样格式的事件。

```ts
import { WSClient, genericAdapter } from '@ws-flow/client/browser'

const client = new WSClient({
  protocol: genericAdapter({
    event(message) {
      if (typeof message !== 'object' || message === null) return
      if (!('type' in message) || typeof message.type !== 'string') return

      return message.type
    },
    writeEvent: (event, data) => ({ type: event, payload: data })
  })
})

client.on('price', message => console.log('行情消息：', message))

await client.connect('ws://localhost:8080')
await client.emit('subscribe', { topic: 'prices' })
```

这里假设已有服务识别 `subscribe` 命令，并按上述格式推送 `price`。发出的内容是 `{ type: 'subscribe', payload: { topic: 'prices' } }`；收到的 `price` 消息被分流到相应回调。

generic 模式保留外部数据结构，所以回调收到的是**完整解码对象**，不会自动变成其中的 `payload`。连接继续用于接收行情，不再使用时调用 `client.dispose()`。

::: details 什么时候需要完整的 ProtocolAdapter？

上面的映射只支持普通事件。只有对方协议确实提供请求标识、响应和终态时，才能进一步映射请求或流任务。`request.cancel`、`request.stream`、`stream.cancel` 等能力需要分别声明，不能仅因支持请求就假定支持取消。

Client 支持注入协议适配器。Server 入口使用标准 ws-flow 协议；修改 codec 或 Envelope builder 不等于支持任意协议。更底层的职责划分见[架构设计](/design/)。

:::

## 用 codec 接收纯文本或二进制

默认 codec 处理 JSON。若远端是纯文本 echo 服务，只需改掉编解码规则；不需要为没有事件名的消息编造协议。

```ts
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient({
  codec: {
    encode(message) {
      if (typeof message !== 'string') throw new TypeError('只能发送文本')
      return message
    },
    decode: data => data
  }
})

const messages = client.subscribe(raw => typeof raw === 'string')

await client.connect('ws://localhost:8080')
await client.send('你好，WebSocket')

for await (const message of messages) {
  console.log('收到文本：', message.raw)
}
```

将地址换成实际的纯文本服务。循环持续接收文本；调用 `messages.cancel()` 可结束观察，应用退出时再销毁 Client。

处理二进制时，让 codec 解码具体的载荷格式即可。Client 的 `decode` 支持异步处理，并保持同一连接的解码顺序。Server 的 codec 位于 `serverOptions.message`，见[Server API](/reference/server)。

## 调度通常保持默认即可

默认调度保持同连接帧的处理顺序，不要求使用者选择策略。需要自定义排队或并发策略时，Server 可以注入调度器，例如：

```ts
import { WSServer } from '@ws-flow/server'
import { SessionScheduler } from '@ws-flow/scheduler'

const server = WSServer.create(
  { port: 8080 },
  {
    schedulers: {
      in: new SessionScheduler({ maxQueuedPerPeer: 128 }),
      out: new SessionScheduler({ maxQueuedPerPeer: 128 })
    }
  }
)
```

这个配置仍保持每个连接内串行，不同连接之间可并行。如果只是限制等待数量，直接配置宿主 `queueLimits` 更简洁；同方向不能同时设置自定义 scheduler 和宿主 `queueLimits`。

::: details 调度与业务任务有什么区别？

调度器控制帧处理的排队、顺序和并发，不负责强行终止任意业务 Promise，也不提供端到端确认。长请求业务脱离帧调度运行，因此帧并发数不是业务执行并发上限。

重排同连接的控制帧可能破坏语义。没有明确需求时保留默认顺序即可，容量与期限选项按需查阅[资源配置](/reference/resources)。

:::

## 扩展点速查

| 想要改变什么 | 使用哪个扩展点 |
| --- | --- |
| 字符串、二进制与业务对象之间的转换 | `codec` |
| 外部消息中的事件名、请求关联和流语义 | `protocol` |
| 公共日志、数据校验、字段变换 | `plugin / middleware` |
| 帧处理的排队、顺序和并发策略 | `scheduler` |

入站消息依次经过解码、协议解释、管线与任务分发；出站方向经过管线、协议映射、编码和 socket 发送。扩展某一层时，其他层继续承担各自的职责。
