# Server

`WSServer` 管理连接与公共处理器，每个 `WSSession` 代表一条客户端连接。公共业务注册在 Server 上，需要单独向某个客户端发送通知或发起请求时，使用它的 Session。

Server 在 Node.js 上运行，使用标准 ws-flow 消息协议。完整使用过程见[服务端与会话](/guide/server)，本页用于查找 API 和配置。

## 创建与接管

```text
WSServer.create<Events, Meta>(wsOptions, serverOptions?): WSServer
WSServer.takeover<Events, Meta>(wss: WebSocketServer, serverOptions?): WSServer
new WSServer<Events, Meta>(serverOptions?)
```

- `create` 的首个参数传给 `ws.WebSocketServer`，例如 `{ port: 8080 }`。本库拥有此服务器，`close()` 时一并关闭。
- `takeover` 监听外部服务器的新连接。既有连接需要显式 `attach`；本库关闭时只移除自己的监听，保留外部服务器及应用注册的监听。
- `new WSServer()` 创建未绑定监听端口的宿主，适合随后手动 `attach(ws)`。

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleRequest('status', () => ({ ready: true }))
```

这样就注册了一个状态查询，客户端使用 `request('status', null)` 取得返回对象。连接保持期间还可以继续发送其他请求。

Server 使用 Node.js 22 及以上环境。配套 Client 需设置 `protocol: 'ws-flow'`；应用退出时调用 `server.close()` 释放连接与处理器。

## ServerOptions

| 字段 | 默认行为 | 用途 |
| --- | --- | --- |
| `message.encode` | `JSON.stringify` | 同步把 envelope 编码为 `string / Buffer / ArrayBuffer / ArrayBufferView` |
| `message.decode` | 按 UTF-8 解码后 `JSON.parse` | 同步解释 `ws.RawData` |
| `message.builder` | 默认 envelope builder | 自定义消息构造规则，保留任务控制语义 |
| `schedulers.in / out` | 两个独立的 `SessionScheduler` | 替换入站或出站调度 |
| `queueLimits.receive / send` | 未指定维度不限 | 配置默认调度器的等待数 |
| `transportLimits.receive / send` | 未指定维度不限 | 原始或编码字节预算、发送水位 |
| `taskLimits` | 每连接入站、出站请求各 `1024` | 活动任务预算 |
| `taskTimeouts` | 关闭 | 入站请求执行期限与流空闲期限 |

同一方向的自定义 scheduler 与 `queueLimits` 不能一起配置，例如 `schedulers.in` 和 `queueLimits.receive` 会在构造时抛错；自定义 scheduler 应自行提供队列策略。

Server 不提供 Client 那样的通用 `protocol` 选项。自定义 codec 和 builder 也不意味着可以任意改写 `requestId`、`kind`、`streamId` 等任务身份；通用协议适配由独立 Client 或 RuntimeCore 承担。扩展方法见[插件与扩展](/guide/extensions)。

## Session 与连接生命周期

| API | 返回值或字段 | 行为 |
| --- | --- | --- |
| `server.attach(ws)` | `WSSession` | 仅纳管 `readyState === WebSocket.OPEN` 的 socket，否则抛错；同一已纳管且仍打开的 socket 返回原 Session |
| `server.detach(session)` | `void` | 移除监听、清理任务和房间，底层 socket 保持打开 |
| `server.close()` | `void` | 关闭所有纳管连接并销毁内核；幂等，不可重新使用 |
| `server.onConnect(fn)` | `void` | 新 Session 纳管后回调 |
| `server.onDisconnect(fn)` | `void` | Session 移除后回调，包括显式 detach |
| `server.sessions` | `ReadonlySet<WSSession>` | 当前纳管连接 |
| `server.rooms` | `ReadonlyMap<string, Set<WSSession>>` | 房间成员视图 |
| `session.id` | `string` | 当前连接的进程内标识，不是持久用户 ID |
| `session.state` | `'open' \| 'closing' \| 'closed'` | 连接状态 |
| `session.ws` | `WebSocket` | 底层 `ws` socket |
| `session.meta` | `Partial<Meta>` | 应用维护的连接元数据 |
| `session.close()` | `void` | 请求关闭 socket；关闭事件触发后由宿主清理 |

`server.close()` 清理连接、任务与处理器，并清空通过 `onConnect`、`onDisconnect`、`onIssue` 注册的观察器。它发起关闭，但不返回“所有网络连接已经完成关闭”的 Promise。外部服务器的监听端口和应用中的数据库、定时任务仍由应用管理。

手动接入时，请在 socket 已打开后调用 `attach`；连接中、关闭中和已关闭的 socket 均不接受。避免在处理器中直接改变内部 Session 状态。

## 请求注册与通知处理

```text
server.handleRequest<Input, Result>(event, handler): () => void
server.handleStream<Input, Chunk>(event, handler): () => void
session.handleRequest<Input, Result>(event, handler): () => void
session.handleStream<Input, Chunk>(event, handler): () => void

handler context = { session, peer, event, requestId, signal, data: Input }
```

`peer` 和 `session` 指向同一个连接。Session 局部处理器优先于 Server 全局处理器；全局和局部可注册同名事件，但每个作用域内同名事件只能注册一次。`handleRequest` 和 `handleStream` 共享名称空间，结果模式不匹配会返回 `MODE_MISMATCH`。

所有请求注册返回移除函数。Session 局部注册随连接关闭清理；全局注册保留到显式移除或 Server 关闭。流处理器必须返回 `AsyncGenerator` 或其 Promise，普通 handler 返回业务结果即可。

| 通知 / 主动流 API | 范围 | 结果 |
| --- | --- | --- |
| `server.subscribe(filter, options?)` | 默认全部 Session，可用 `options.peer` 限定 | `MessageSubscription` |
| `session.subscribe(filter, options?)` | 该 Session | `MessageSubscription` |
| `server.on(event, handler)` | 全部 Session 的普通通知 | 取消函数 |
| `session.on(event, handler)` | 该 Session 的普通通知 | 取消函数 |
| `server.onStream(event, handler)` | 按事件接收主动流 | 移除函数 |

`subscribe` 的筛选函数、快照与容量语义同 [Client](/reference/client#通知与订阅)。`server.on`、`session.on` 不接收容量选项，需要定制缓冲时使用 `subscribe`。Server 全局订阅可跨 Session 存在；绑定 Session 的订阅随连接关闭。

主动流 handler 从 `ctx.stream` 消费分片。没有专用 `onStream` 时，Server 仍允许已有接收事件管线处理主动流；应用应确保某个处理器确实消费或结束它。

### 接收事件管线

```text
server.route(matcher, handler): void
server.handle(matcher, handler, autoSend = 'smart'): void
```

`route` 与 `handle` 将处理器注册到接收事件管线。`matcher` 支持字符串、正则或匹配函数；handler 收到 `WSReceiveContext`，含 `session`、`envelope`、`raw`、可选 `stream`。

`route` 不自动发送返回值。`handle` 可根据返回值自动发送：

| autoSend | 行为 |
| --- | --- |
| `false` | 不自动发送 |
| `'data'` | 按普通数据发送 |
| `'envelope'` | 按完整 envelope 发送 |
| `'stream'` | 将生成器或生成器工厂作为主动流发送 |
| `'smart'` | 先识别生成器，再识别带 envelope 字段的对象，否则按普通数据发送 |

这些返回消息不自动关联某个 `requestId`。`request` / `requestStream` 已由内核分发给专用 handler，不能用 `route` / `handle` 代替 `handleRequest` / `handleStream`。需要将结果关联到发起方的调用时，使用请求 API。

## 从 Server 发出消息

Session 提供 `send`、`emit`、`request`、`requestStream`、`stream`，与 [Client](/reference/client) 对应能力一致，并增加 `sendEnvelope(envelope, options?)`。

```text
session.request<Result>(event, input, options?): Promise<Result>
session.requestStream<Chunk>(event, input, options?): StreamTask<Chunk>
session.stream(event, generatorOrFactory, options?): Promise<void>
session.stream(generatorOrFactory, options?): Promise<void>
```

请求选项支持 `timeoutMs`（默认 `30000`，`null` 关闭）、`signal`、`priority`、`meta`、`sendMiddlewares`；请求流还支持 `capacity`、`idleTimeoutMs`。主动流支持 `signal`、`idleTimeoutMs`、`streamMiddlewares` 等发送选项。请求处理器不会把入站请求的发送选项自动复制给业务发起的其他请求。

### 房间与目标选择

```text
session.join(room): void
session.leave(room): void
session.leaveAll(): void
session.rooms: ReadonlySet<string>

server.to(sessionId | predicate): Sender
server.all(): Sender
sender.to(sessionId | predicate): this
sender.except(sessionId | predicate): this
sender.all(): this
sender.use(sendMiddleware | sendMiddlewares): this
```

`to('...')` 匹配 **Session ID**。按房间发送应使用 `server.to(session => session.rooms.has('room-name'))`。多次 `to` 条件取交集，多次 `except` 条件取并集。`all()` 清除包含条件，保留已设置的排除条件。Sender 为可变选择器，建议每次操作从 `server.to(...)` 或 `server.all()` 重新取得。

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.onConnect(session => session.join('workers'))

// 在管理页面点击“刷新状态”等业务入口调用。
export async function inspectWorkers() {
  const results = await server
    .to(session => session.rooms.has('workers'))
    .requestAll('worker.status', null, { timeoutMs: 3000 })

  for (const result of results) {
    if (result.status === 'fulfilled') {
      console.log(result.session.id, result.value)
    } else {
      console.error(result.session.id, result.reason)
    }
  }
}
```

客户端注册 `handleRequest('worker.status', () => ({ busy: false }))` 后，应用调用 `inspectWorkers()` 即可查询当前所有 worker。没有已连接的 worker 时，结果为空数组。

`Sender` 支持 `send`、`emit`、`stream` 和 `requestAll`。`requestAll` 在调用时快照目标，返回与目标顺序一致的结果数组，每项为 `{ session, status: 'fulfilled', value }` 或 `{ session, status: 'rejected', reason }`；无目标时返回 `[]`。一个目标失败不会抛掉其他目标的结果。

批量 `send/emit/stream` 使用并行发送，任一目标失败会使返回 Promise 拒绝，其他已启动的发送不会自动撤销。向多个目标发送流时必须传**生成器工厂**，让每个目标持有独立生成器。多目标请求流的聚合由应用组织。

## 插件与错误观察

Server 提供 `use`、`useReceive`、`useReceiveEvent`、`useReceiveStream`、`useSend`、`useSendEvent`、`useSendStream`，均返回自身以便链式调用。上下文增加 `session` 别名。

`setScheduler` 同时切换两个方向；`setInScheduler` / `setOutScheduler` 分别切换。修改调度器不等于迁移旧调度器内已经排队的工作，应优先在构造时配置。

`onIssue(handler)` 观察所有问题，`onIssue(type, handler)` 只观察一种问题，二者返回 Server。此注册没有对应移除函数，由 `server.close()` 统一清理。底层监听或服务异常通过 `server_error` 报告，携带 `error: unknown`，不关联单个 Session。问题类型及异常处理见[错误处理](/reference/errors)。
