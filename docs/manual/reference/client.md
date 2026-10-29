# Client

`WSClient` 为一条 WebSocket 连接提供订阅、请求与流式任务 API。浏览器和 Node.js 使用同样的调用方式，只需选择对应入口；接入已有服务时，也可以沿用服务自己的消息格式。

本页用于查选项与行为。第一次接入，先从[快速开始](/guide/quick-start)的完整通信示例开始。

## 入口与环境

| 导入路径 | 底层连接 | 用途 |
| --- | --- | --- |
| `@ws-flow/client/browser` | 原生 `WebSocket` | 浏览器应用 |
| `@ws-flow/client/node` | 动态加载 `ws` | Node.js；应用需安装 `ws` |
| `@ws-flow/client` | 浏览器条件选择 browser，其余默认 Node | 支持条件导出的构建工具 |

Node.js 支持范围为 22 及以上。Node 入口始终使用 `ws`，不会因为存在全局 `WebSocket` 而改用另一个实现。`/browser` 不引入 `ws`，运行环境需要提供标准 WebSocket API。

```ts
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient({ protocol: 'ws-flow' })

try {
  await client.connect('ws://localhost:8080')
  const result = await client.request('status', null)
  console.log(result) // { ready: true }
} finally {
  client.dispose()
}
```

这个脚本请求 [Server 参考页](/reference/server#创建与接管)中的状态处理器，完成后关闭连接。页面应用通常复用同一个 Client，在页面卸载或应用退出时调用 `dispose()`；不必为每次请求创建连接。

## 构造配置

```text
new WSClient<Events>(options?: ClientOptions)
```

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `protocol` | `'generic'` | `'generic'`、`'ws-flow'` 或自定义 `ProtocolAdapter` |
| `codec` | JSON 编解码 | `encode(value)` 同步编码；`decode(raw)` 可同步或异步解码 |
| `socketFactory` | 当前入口提供的工厂 | `(url, protocols?) => ClientConnection \| Promise<ClientConnection>` |
| `connectTimeoutMs` | `10000` | 一次连接的期限，包含工厂等待与握手 |
| `reconnect` | 关闭 | `false` 或 `{ delayMs?: number, maxAttempts?: number }` |
| `queueLimits` | 不限制等待数 | `{ receive?, send? }`，两侧独立 |
| `transportLimits` | 不限制字节、不启用水位 | `{ receive?, send? }` |
| `taskLimits` | 每连接每方向请求各 `1024`；其他维度不限 | 活动请求与主动流预算 |
| `taskTimeouts` | 均关闭 | 入站请求执行期限、流空闲期限 |

`reconnect: {}` 使用固定 `1000ms` 间隔，最多连续尝试 `5` 次；连接成功后重置尝试次数。`maxAttempts: 0` 不重试。`connectTimeoutMs` 必须为正有限数且不大于 `2147483647`；非法配置在构造时抛错。其他资源字段详见[资源与期限](/reference/resources)。

### generic 与 ws-flow

按对端服务选择一次协议即可：

| 连接什么服务 | 配置 | 可直接使用的能力 |
| --- | --- | --- |
| 已有 WebSocket 服务 | 无需配置，默认 `generic` | `send` 发送业务数据，`subscribe` 筛选消息 |
| 使用 `topic` 等字段区分事件的服务 | `genericAdapter({ event, writeEvent })` | 增加按事件名调用的 `on` 与 `emit` |
| ws-flow Server | `protocol: 'ws-flow'` | 通知、双向请求、请求流与主动流 |

generic 保留解码后的业务对象，不会把其中的 `kind`、`requestId` 自动当作任务字段。`event` 负责读出事件名，`writeEvent` 负责写出对端要求的格式；它们不会替第三方服务增加请求或流协议。

例如，服务使用 `{ topic: 'status', payload: ... }` 传递状态，下方示例完成映射与持续订阅。只需要原始消息筛选时，直接看[独立 Client 案例](/examples/generic-client)。

::: details 按 topic 订阅状态：连接、事件映射与清理

```ts
import { WSClient, genericAdapter } from '@ws-flow/client/browser'

export async function connectStatus(
  url: string,
  onStatus: (message: unknown) => void
) {
  const client = new WSClient({
    protocol: genericAdapter({
      event: message => {
        if (typeof message !== 'object' || message === null) return
        if ('topic' in message && typeof message.topic === 'string') {
          return message.topic
        }
      },
      writeEvent: (topic, payload) => ({ topic, payload })
    })
  })

  const unsubscribe = client.on('status', onStatus)
  const stop = () => {
    unsubscribe()
    client.dispose()
  }

  try {
    await client.connect(url)
    await client.emit('subscribe', { channel: 'status' })
    return stop
  } catch (error) {
    stop()
    throw error
  }
}
```

`connectStatus(url, onStatus)` 建立持续订阅，返回清理函数；应用在页面卸载或不再需要状态消息时调用它。

这里的对象检查用于确认外部消息确实含有字符串 `topic`；只需在适配处写一次，业务订阅仍按事件名使用。

:::

此例的 `message` 是完整解码对象；generic 的 `event` 提取不改变 `data`。默认 codec 发送时 `JSON.stringify`，接收字符串时 `JSON.parse`，接收二进制保留原始值。纯文本服务或直接发送二进制时，请提供相应 codec。codec 处理传输表示，adapter 解释消息语义，见[插件与扩展](/guide/extensions)。

## 连接与状态

| API | 返回值 | 行为 |
| --- | --- | --- |
| `connect(url, protocols?)` | `Promise<void>` | 等待连接打开；已连接、正在连接或已销毁时拒绝 |
| `close(code?, reason?)` | `void` | 清理本次连接的任务，关闭 socket，停止自动重连；可再次连接 |
| `detach()` | `void` | 移除本库监听并清理任务，保留打开的外部 socket；连接过程中不可调用 |
| `dispose()` | `void` | 关闭连接、清理注册与观察器；对象不可复用 |
| `onState(handler)` | 取消函数 | 异步通知状态变化，不立即重放当前状态 |
| `onIssue(handler)` | 取消函数 | 观察宿主处理中的错误；没有观察器时输出到 console |
| `getWsInstance()` | `ClientSocket \| null` | 取得当前底层 socket |
| `state` | `ClientState` | `idle / connecting / open / reconnecting / closed / disposed` |
| `peer` | `ClientPeer \| undefined` | 当前连接身份，含 `id`、`state`、`socket` |

观察器返回的 Promise 会被观察，异常不会破坏连接状态。每次新连接创建新 Peer。断连后旧请求与流结束，`on` / `subscribe` 结束；重连不重发任务、不恢复订阅。`handleRequest`、`handleStream`、`onStream` 注册保留，直至移除或 `dispose()`。

### 接管已打开连接

```text
WSClient.takeover(connection: ClientConnection, options?: ClientOptions): WSClient
client.attach(connection: ClientConnection): this

ClientConnection = {
  socket: ClientSocket
  send(data): void | Promise<void>
}
```

socket 必须处于打开状态，具备 `addEventListener` / `removeEventListener`、`send`、`close`、`readyState`、可写的 `binaryType`。启用水位还要求可用的 `bufferedAmount`。Client 会设置 `binaryType = 'arraybuffer'`。接管连接不自动重连；`detach()` 保留 socket，`close()` / `dispose()` 仍会关闭它。

Node 自定义连接应把 `ws.send` 回调包装为 Promise，使异步发送失败可被任务观察。内置 Node 工厂已提供此行为。

## 通知与订阅

```text
send(data: unknown, options?: SendOptions): Promise<void>
emit(event: string, data: unknown, options?: SendOptions): Promise<void>
subscribe(filter, options?): MessageSubscription
on(event, handler, options?): () => void
```

`SendOptions` 支持 `signal`、`priority`、`meta`、`sendMiddlewares`。`priority` 是否影响顺序取决于调度器；默认 Client 的会话调度不按优先级重排。`meta` 能否出现在对端数据中取决于协议。

`filter(raw, message)` 必须同步返回 boolean。`message` 含 `raw`、`data`、`event?`、`peer`；`raw` 为原始解码值，`data` / `event` 来自协议和接收管线处理后的消息。各订阅得到独立的数据快照，Peer 身份保持原引用；不可被 `structuredClone` 复制的负载会使相应订阅失败。

订阅选项为 `{ capacity?: number, signal?: AbortSignal }`，默认积压容量 `128`。`MessageSubscription` 可 `for await` 迭代，提供 `closed`、`cancel()`、`return()`；提前 `break` 等同于本地结束订阅，不向对端取消业务任务。每个订阅是一条消费队列，多路观察应创建独立订阅。

`on` 按订阅顺序逐条等待 handler；返回取消函数。handler 抛错或订阅溢出后，会通过 `onIssue` 报告并结束该订阅。

## 请求与请求流

```text
request<Result>(event, input, options?): Promise<Result>
requestStream<Chunk>(event, input, options?): StreamTask<Chunk>
handleRequest<Input, Result>(event, handler): () => void
handleStream<Input, Chunk>(event, handler): () => void

handler context = { peer, event, requestId, signal, data: Input }
```

`request` 选项在 `SendOptions` 上增加 `timeoutMs`：默认 `30000`，`null` 关闭；总期限包含发送排队和等待结果。`requestStream` 再增加 `capacity`（默认 `128`）与 `idleTimeoutMs`（继承宿主流期限，`null` 在本地关闭）。

单结果 handler 返回结果或 Promise；流 handler 返回 `AsyncGenerator<Chunk>` 或其 Promise。两种 handler 共享事件注册表，同事件不能同时注册两种结果模式。注册返回移除函数。

`StreamTask` 是异步迭代器，状态为 `waiting / open / ended / failed / cancelled`，提供 `closed` 和 `cancel(reason?)`。正常结束后继续读完已缓冲数据；失败或取消丢弃缓冲并让读取拒绝。提前 `break` 会结束本地任务，并在协议支持时尽力发送请求取消。

`requestStream` 直接返回任务，不需要先 `await`；错误通常在迭代时出现。但 Client 未连接、容量或空闲期限参数非法等情况可能同步抛出，应把任务创建与迭代一起放进 `try`。`request` 通过 rejected Promise 报错。

## 主动流与插件

```text
stream(event, source: AsyncGenerator | (() => AsyncGenerator), options?): Promise<void>
onStream(event, handler): () => void

use(plugin | plugins): this
useReceive(middleware | middlewares): this
useSend(middleware | middlewares): this
useReceiveStream(middleware | middlewares): this
useSendStream(middleware | middlewares): this
```

主动流发送选项在 `SendOptions` 上增加 `idleTimeoutMs` 和 `streamMiddlewares`。接收 handler 从 `ctx.stream` 迭代分片，主动流未注册接收 handler 时在 Client 上失败。`ctx` 同时含 `peer`、`envelope`、`raw` 和可选的取消 `signal`。

通知订阅不能消费流分片。流迭代与 `ctx.stream.onData` 回调消费也不能混用；推荐异步迭代以表达串行处理。插件各阶段的顺序和 `stop/end/fail` 语义见[插件与扩展](/guide/extensions)。
