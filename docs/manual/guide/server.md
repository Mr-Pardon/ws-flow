# 服务端使用

服务端的日常工作可以从两个对象理解：**Session 代表一个客户端，Sender 代表选中的一组客户端。** 单个连接可以发送、请求和接收流；多个连接可以广播通知，也可以分别查询并收集结果。

## 给一组客户端发送通知

例如，一个看板服务把接入的客户端加入 `dashboard` 房间。业务更新后，调用 `announce()`，就能向这些看板发送通知：

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.onConnect(session => {
  session.join('dashboard')
})

export function announce(text: string) {
  return server
    .to(session => session.rooms.has('dashboard'))
    .emit('notice', { text })
}
```

客户端用 `client.on('notice', ...)` 接收通知。等客户端接入后，业务代码调用 `await announce('报表已更新')` 即可发送；服务退出时调用 `server.close()`。

只发给一个连接时使用 `session.emit(...)`；按 ID 查找时用 `server.to(sessionId)`；发给全部当前连接时用 `server.all().emit(...)`。

房间是当前进程内的连接分组。`join`、`leave`、`leaveAll` 改变成员关系，断连时自动移除成员；房间不负责跨进程同步。

## 向客户端查询结果

WebSocket 是双向的，服务端也能主动询问客户端。以查询工作节点状态为例，客户端先提供 `status` 能力：

```ts
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })

client.handleRequest('status', () => ({ ready: true, jobs: 2 }))

await client.connect('ws://localhost:8080')
```

客户端继续保持连接。服务端可以在管理操作触发时，查询所有已连接节点：

```ts
import type { WSServer } from '@ws-flow/server'

type WorkerStatus = { ready: boolean; jobs: number }

export async function inspectWorkers(server: WSServer) {
  const results = await server.all().requestAll<WorkerStatus>('status', null, {
    timeoutMs: 3_000
  })

  for (const result of results) {
    if (result.status === 'fulfilled') {
      console.log(result.session.id, '节点状态：', result.value)
    } else {
      console.error(result.session.id, '查询失败：', result.reason)
    }
  }
}
```

把当前的 Server 传给 `inspectWorkers(server)` 即可。每个客户端对应一项结果；某个节点超时，不会丢失其他节点的成功结果。只查询一个连接时，直接使用 `await session.request('status', null)`。

::: details 多目标操作的具体规则

| 调用 | 选择规则 |
| --- | --- |
| `server.all()` | 全部当前 Session |
| `server.to(id)` | 指定 Session ID；字符串不是房间名 |
| `.to(predicate)` | 添加筛选条件，多次调用取交集 |
| `.except(idOrPredicate)` | 排除匹配的连接 |
| `.all()` | 清除包含条件，保留已有排除条件 |

链式调用返回 Sender，可继续调用 `send`、`emit`、`stream` 或 `requestAll`。

`requestAll` 在调用时固定目标集合和顺序，后来接入的连接不加入这次任务；返回数组保持目标顺序，没有目标时返回 `[]`。它聚合单结果请求，不合并多个目标的结果流。

多目标 `send` / `emit` 的 Promise 不包含逐目标结果；某个发送失败可能使整个 Promise 拒绝，已经发送的消息不会回滚。需要每个目标的业务返回值时，使用 `requestAll`。

:::

## 统一处理与连接专属处理

常用能力直接注册在 Server 上，例如 `server.handleRequest('health', handler)`，所有接入连接都能调用。

某个连接确实需要不同的处理逻辑时，再使用 `session.handleRequest(...)` 或 `session.handleStream(...)`。它们只对当前连接生效，并优先于 Server 上的同名处理器。

普通通知使用 `server.on` 或 `session.on`；主动流使用 `server.onStream`。Session 的 `meta` 可保存应用已经验证的身份、角色等本地状态，不会自动同步到对端。

::: details 注册与会话清理

同一作用域内，单结果和流结果处理器共享事件名，不应重复注册。注册函数返回注销函数；Session 断开时，其局部处理器也会清理。

`onConnect` 和 `onDisconnect` 用于观察连接变化；它们与 `onIssue` 注册的观察器都会在 `server.close()` 时清理。Session 还提供 `id`、`state`、原生 `ws` 与只读的 `rooms` 视图。

客户端发送的身份声明需要经过应用验证，再写入 `meta` 供后续业务使用。

:::

## 接入已有 WebSocket 服务

已有 HTTP、HTTPS 或 `ws` 服务时，不需要再监听一个端口。把已有的 `WebSocketServer` 交给 `takeover()`，即可增加 ws-flow 的消息处理能力：

```ts
import type { WebSocketServer } from 'ws'
import { WSServer } from '@ws-flow/server'

export function attachMessaging(socketServer: WebSocketServer) {
  const server = WSServer.takeover(socketServer)

  server.onIssue('server_error', issue => {
    console.error('WebSocket 服务异常', issue.error)
  })

  server.handleRequest('health', () => ({ ready: true }))

  return server
}
```

在现有服务初始化时调用 `attachMessaging(socketServer)`。`server_error` 用于观察底层服务器的异常，不关联单个客户端。`server.close()` 释放 ws-flow 管理的会话、处理器和观察器，只移除自己在外部服务器上注册的监听；外部 `WebSocketServer` 仍由原应用负责关闭。

上例直接引用了 `ws` 的类型，消费项目应自行声明依赖：

```sh
npm install ws
npm install --save-dev @types/ws
```

::: details 创建、接管与配置归属

- `WSServer.create(wsOptions, serverOptions)` 创建并拥有底层监听服务器，`server.close()` 会一起关闭它。
- `WSServer.takeover(socketServer)` 使用外部监听服务器，不接管它的关闭责任。
- `attach(socket)` / `detach(session)` 适合单连接接入与解绑；`attach` 只接受已经打开的 socket，`detach` 不关闭原始 socket。

`create` 的第一个参数属于 `ws`，例如端口和 `maxPayload`；第二个参数属于 ws-flow，例如 codec、调度及资源限制。

:::

默认配置适合先完成通信。更多配置按需查阅[资源配置](/reference/resources)与[Server API](/reference/server)。
