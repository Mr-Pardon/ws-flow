# Client 2.x：迁移与使用

更新：2026-09-28。本文描述当前实现。旧 Client 位于 `legacy/client-v1`，不参与构建；完整原始快照见实施路线。

## 1. 环境与协议

```ts
import { WSClient } from '@ws-flow/client/browser'
// Node 使用 @ws-flow/client/node，并安装 ws
const generic = new WSClient() // 任意服务的普通 JSON 消息
const standard = new WSClient({ protocol: 'ws-flow' }) // 配套 Server
```

根入口按 browser 条件选择，Node 默认使用 Node 入口，不依赖是否存在全局 WebSocket。公开基础 socket 类型不依赖 @types/ws。浏览器 Worker 可使用 /browser，但未单独做 Worker 运行验证。

generic 不把业务对象中的 event/kind/requestId 等字段自动解释为任务。send 保留数据结构，subscribe 使用原始解码消息筛选。默认 generic 不支持 request 或 stream，调用报告能力缺失；标准协议支持完整任务能力。

## 2. codec 与 adapter

codec 负责 socket 数据与 JS 值的转换；adapter 负责 JS 值的语义。默认 codec 接收字符串时 JSON.parse，发送时 JSON.stringify，接收二进制保留原数据。发送原始二进制或使用纯文本服务需要提供 codec。

```ts
import { WSClient, genericAdapter } from '@ws-flow/client/browser'
const client = new WSClient({
  codec: { encode: value => value, decode: value => value },
  protocol: genericAdapter({
    event: raw => typeof raw === 'string' ? raw.split(':')[0] : undefined,
    writeEvent: (event, data) => `${event}:${data}`,
  }),
})
client.on('progress', data => console.log(data))
await client.connect('wss://your-service.example/socket')
await client.emit('resume', 42) // 发送 resume:42
```

复杂协议实现 ProtocolAdapter：read 返回 RuntimeMessage，write 返回对端接受的消息。只有存在真实关联规则时才实现 request.write；请求取消、请求流、主动流、主动流取消分别启用，不能用本地猜测替代对端协议。

## 3. 业务 API

| 场景 | API | 结果 |
| --- | --- | --- |
| 普通数据/通知 | send / emit | 本地发送完成 Promise |
| 筛选通知 | subscribe | AsyncIterable，元素含 raw/data/event/peer |
| 命名通知 | on | 取消函数；回调逐条执行 |
| 单结果请求 | request / handleRequest | Promise / 处理器注册 |
| 流式请求 | requestStream / handleStream | StreamTask / 生成器处理器 |
| 主动流 | stream / onStream | 发送 Promise / 消费 ctx.stream |

on/subscribe 只分发普通通知；请求响应和控制帧由内核处理。onStream 接收主动流，handleStream 回应流式请求。注册处理器返回移除函数，重复主动流处理器会报错。stream 的 options.signal 可取消，接收侧提前 break 请求远端取消。

发送成功不代表对端业务成功，确认使用 request。浏览器 send 完成只代表提交到原生缓冲。未连接时 requestStream 同步抛错，request 返回 rejected Promise；请连接成功后再发起任务。

## 4. 连接生命周期

```ts
const client = new WSClient({
  protocol: 'ws-flow', connectTimeoutMs: 10_000,
  reconnect: { delayMs: 1000, maxAttempts: 5 },
})
client.onIssue(error => console.error(error))
client.onState(state => {
  if (state === 'open') client.on('notice', data => console.log(data))
})
await client.connect('ws://localhost:8080')
```

自动重连默认关闭；开启后以固定间隔最多尝试 maxAttempts 次，成功后重置计数。显式 close/dispose 停止重连。连接期限涵盖 socket factory 和握手，超时后迟到的 socket 会被关闭；无法强制取消 factory 自身的 Promise。

每次连接创建新 Peer，旧任务结算、旧订阅结束、迟到解码结果丢弃。不自动重发请求或恢复订阅。handleRequest/handleStream/onStream 注册保留到移除或 dispose；on/subscribe 属于连接周期，需要在新的 open 状态重新建立。

状态和错误观察器异步执行并隔离异常；默认错误输出到 console。

| 操作 | socket | 内核任务 | 后续使用 |
| --- | --- | --- | --- |
| close | 请求关闭 | 清理 | 可重新连接 |
| detach | 保持打开，移除本库监听 | 清理 | 可重新连接/接管 |
| dispose | 请求关闭 | 清理并销毁注册 | 不可复用 |

接管要求已打开的连接：

```ts
const client = WSClient.takeover({
  socket: openedSocket,
  send: data => openedSocket.send(data),
}, { protocol: 'ws-flow' })
```

Node 接管时可以把 ws.send callback 包为 Promise，以保留异步发送失败；内置 Node factory 已这样处理。detach 不关闭外部 socket，dispose/close 会关闭。

收发资源可另配 queueLimits（等待条数）和 transportLimits（单消息/积压字节、发送水位）。资源限制独立于协议，通用文本/二进制客户端同样适用；未填写的维度不限制。Client 的接收额度在 decode 前预占，发送额度在 encode 后预占，等待水位时取消不会把旧消息提交到新连接。完整参数和计量边界见[字节限额与发送水位](字节限额与发送水位.md)。

具备请求/流协议能力时，可配置 taskLimits 限制登记中的活动任务，taskTimeouts.incomingRequestMs / streamIdleMs 启用独立执行/空闲期限；单次 stream/requestStream 可用 idleTimeoutMs 覆盖空闲期限。超限与超时优先结算该任务，连接继续可用。默认请求总期限与入站/流期限分开，详见[活动任务限额与期限](活动任务限额与期限.md)。

## 5. 旧版迁移

已移除独立 ws-event-proxy 实现，共用 RuntimeCore。旧 core/types 内部路径、代理接口和 watch 用法不提供兼容层：通知迁移到 on/subscribe，任务迁移到 request/requestStream，状态观察迁移到 onState，接管迁移到 ClientConnection。

原先配套 Envelope 的代码必须设置 protocol:'ws-flow'，旧重连/订阅恢复假设也需要调整。业务应捕获迭代异常，对生产者内部耗时操作传递 AbortSignal；取消无法强制中断用户自己的无限等待。这是从 1.x 到 2.x 的破坏性升级，不保证新旧任务协议互通。
