# @ws-flow/server

Node.js WebSocket 服务端，提供连接管理、房间筛选、双向请求和流式任务。Session 代表单个客户端连接，Sender 组合一组目标，RuntimeCore 统一处理消息与任务生命周期。

## 安装

```sh
npm install @ws-flow/server@^1
```

要求 Node.js 22 或更新版本，使用 ESM。配套客户端使用 `@ws-flow/client@^2`，并设置 `protocol: 'ws-flow'`。

## 注册业务能力

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleRequest<number, number>('double', ctx => ctx.data * 2)
server.handleStream('numbers', async function* () {
  yield 1
  yield 2
})

server.on('notice', (data, message) => {
  console.log(message.peer.id, data)
})

server.onConnect(session => {
  session.join('workers')
})

export function inspectWorkers() {
  return server
    .to(session => session.rooms.has('workers'))
    .requestAll('status', null)
}
```

客户端接入后即可发送通知、发起请求或读取结果流。调用 `inspectWorkers()` 向 workers 房间内的客户端查询状态，每个目标返回独立的成功或失败结果。服务停止时调用 `server.close()`。

## 模型与消息路径

入站：`ws → codec.decode → RuntimeCore → 请求 / 流 / 订阅 → 业务处理器`。

出站：`Session / Sender → RuntimeCore → codec.encode → ws.send`。

WSServer 持有连接集合、RoomManager 和 RuntimeCore；WSSession 对应一个 Peer；Sender 负责筛选和多目标操作。默认采用标准 ws-flow 协议与 JSON codec，入站和出站分别使用 SessionScheduler。

| 入口 | 用途 |
| --- | --- |
| `session.send / emit` | 单连接普通消息 / 命名通知 |
| `session.request / requestStream` | 向客户端请求单个 / 连续结果 |
| `server/session.handleRequest / handleStream` | 公共 / 会话局部处理器，局部优先 |
| `server/session.subscribe / on` | 普通通知筛选与订阅 |
| `session.stream / server.onStream` | 主动发送 / 消费流 |
| `server.all / to → Sender.except` | 组合目标筛选与排除 |
| `Sender.requestAll` | 按目标收集独立结果 |
| `use` 与各阶段中间件入口 | 组合模块化消息处理 |
| `onIssue / onConnect / onDisconnect` | 观察异常与连接变化 |

链式 `to` 条件取交集，`except` 取排除并集；`all` 清除包含条件并保留排除条件。`requestAll` 固定调用时的目标与顺序，不聚合多个目标的结果流。

订阅只观察通知，响应和控制帧交给任务系统。`on` 回调、请求处理器与流消费者脱离帧调度，可以等待后续结果；收发中间件应保持短处理，避免等待同连接未来帧。`route` / `handle` 是帧内路由，`handle` 返回值的发送不具有请求关联语义。

## 连接所有权

| 入口 | 底层资源归属 |
| --- | --- |
| `WSServer.create(wsOptions, serverOptions)` | 创建并拥有底层服务器，`close()` 一并关闭 |
| `WSServer.takeover(wss, serverOptions)` | 接管外部服务器的新连接，外部服务器由调用方关闭 |
| `server.attach(socket)` | 将处于 OPEN 状态的 socket 纳入会话管理 |
| `server.detach(session)` | 解除绑定并清理本库任务，底层 socket 保持打开 |

断连移除会话与房间成员，并结束其关联请求、流和局部订阅；全局订阅继续接收其他连接消息。取消遵循 `AbortSignal` 协作规则，业务等待应继续传递 `ctx.signal`。

`server.close()` 释放连接、处理器、连接观察器与 issue 注册。接管外部服务器时，仅移除 ws-flow 自己绑定的监听，保留调用方的其他监听。

底层监听失败等宿主异常通过 `onIssue('server_error', issue => ...)` 观察，例如端口占用。该事件包含 `error: unknown`，不关联 Session；按会话处理的消息错误与资源错误保留各自的事件类别。

## 资源保护

资源配置位于 `create` 的第二个参数；第一个参数交给 `ws`，例如端口与解析器的 `maxPayload`。

| 配置 | 计量范围 |
| --- | --- |
| `queueLimits.receive / send` | 所有会话 / 每会话的等待任务数，不包含已开始工作 |
| `transportLimits.receive / send` | 单消息、总积压与每会话积压的原始 / 编码载荷字节 |
| `transportLimits.send.highWaterMark` | 原生发送缓冲与当前待发送帧的容量 |
| `taskLimits` | 请求、主动流的共享及分类登记数 |
| `taskTimeouts` | 入站请求执行期限、流空闲期限 |

默认每个 Peer 的入站、出站请求各有 1024 个额度，其他可选限额和期限未配置时关闭。请求流在每端各计为一个请求，结果流不重复占主动流额度；终态、失败、取消、超时和断连释放登记。出站请求的 `timeoutMs` 默认 30 秒，与执行及空闲期限独立。

同方向自定义 scheduler 与 `queueLimits` 不能同时指定。SessionScheduler 按连接串行安排帧，不同连接可并行；帧调度并发不等于业务函数并发。发送水位准入至 `ws.send` callback 完成按 Peer 串行，确保并发调度器不会重复放行同一段容量。

普通通知出站队列或传输超限仅拒绝本次调用；入站或任务帧的队列、传输超限关闭受影响连接，其他连接独立运行。任务容量或期限只结算对应任务；反馈控制帧再触发队列、字节或水位限制时，执行该层关闭策略。

`onIssue('resource_error')` 提供错误、方向与 Session；任务错误另含 `taskKind`。应用应根据错误类别处理，不能假定每个资源错误都会关闭连接。详细默认值与错误字段见[资源与期限](../../docs/manual/reference/resources.md)和[错误参考](../../docs/manual/reference/errors.md)。

这些额度管理逻辑登记与特定传输阶段，不控制所有业务对象、全部堆内存或外部 socket 写入。取消无法强行终止用户代码，实际业务并发需在业务入口管理；本地发送水位不代表对端消费确认。

## 源码与维护

- `src/core/wss.ts`：宿主装配、codec、连接绑定与公共 API。
- `src/core/session.ts`：单连接接口；`sender.ts`：目标与聚合；`roomManager.ts`：房间索引。
- `src/core/runtimeAdapters.ts`：将共享 Peer 上下文映射为 Server Session 上下文。
- `src/core/issues.ts` 与 `src/types/`：异常分发和公开声明。

任务语义归属 runtime-core。监听绑定与解绑成对维护，接管外部资源保持所有权边界。执行 `pnpm run check-types`、`pnpm test` 和 `pnpm run verify-packages` 验证源码、真实 socket 行为与安装包类型。

完整使用方式见[服务端指南](../../docs/manual/guide/server.md)，签名与选项见 [Server API](../../docs/manual/reference/server.md)，验证流程见[贡献指南](../../CONTRIBUTING.md)。
