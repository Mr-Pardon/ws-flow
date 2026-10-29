# @ws-flow/client

可独立接入第三方服务的 WebSocket 客户端，提供事件订阅、双向请求和流式任务。连接、编解码和重连由 Client 管理，任务关联与清理由共享 RuntimeCore 管理。

## 安装与环境入口

```sh
# 浏览器
npm install @ws-flow/client@^2

# Node.js
npm install @ws-flow/client@^2 ws@8
```

| 入口 | 传输 |
| --- | --- |
| `@ws-flow/client/browser` | 浏览器原生 WebSocket |
| `@ws-flow/client/node` | Node.js `ws` |
| `@ws-flow/client` | browser 条件选择浏览器入口，其余使用 Node 入口 |

Node.js 要求 22 或更新版本。浏览器入口不引入 Node `ws`，公开 socket 类型不要求 `@types/ws`。Worker 可以选择 browser 入口；独立 Worker 运行验证不在验证矩阵内。

## 连接已有服务

默认 generic 模式保留对方的消息格式。下面假设服务接受 `watch` 命令并持续返回 JSON 消息：

```ts
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient()
const messages = client.subscribe(() => true)

await client.connect('wss://your-service.example/socket')
await client.send({ action: 'watch', topic: 'prices' })

for await (const message of messages) {
  console.log(message.raw)
}
```

替换实际地址与命令即可接入。修改筛选函数可分流关心的数据；`break` 或 `messages.cancel()` 结束本地订阅，应用卸载时调用 `client.dispose()`。

默认 codec 发送 JSON、解析收到的 JSON 字符串，收到二进制时保留原始载荷。纯文本或二进制发送需提供相应 codec。命名事件通过 `genericAdapter({ event, writeEvent })` 映射；generic 不把业务 `requestId`、`kind` 自动识别为任务字段。

## 使用请求与流

连接 ws-flow Server 时，显式启用配套协议：

```ts
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })
client.handleRequest('status', () => ({ ready: true }))

await client.connect('ws://localhost:8080')

const result = await client.request<number>('double', 21)
console.log(result) // 42

for await (const chunk of client.requestStream<number>('numbers', null)) {
  console.log(chunk)
}
```

Server 需注册对应的 `double` 和 `numbers` 处理器；Client 的 `status` 处理器可被 Server 反向调用。连接可持续使用，应用退出时调用 `client.dispose()`。

| API | 意图 |
| --- | --- |
| `send` / `emit` | 普通数据 / 命名通知 |
| `subscribe` / `on` | 原始筛选 / 命名订阅，独立消费 |
| `request` / `handleRequest` | 发起 / 处理单结果请求 |
| `requestStream` / `handleStream` | 发起 / 处理流式请求 |
| `stream` / `onStream` | 主动发送 / 消费流 |
| `use` 与各阶段中间件入口 | 组合公共处理规则 |
| `onState` / `onIssue` | 观察连接状态 / 异常 |

第三方协议的请求与流能力通过 `ProtocolAdapter` 声明，必须得到对端协议支持。普通订阅不接收请求控制帧；`send` 成功表示本地发送完成，业务确认使用请求。

## 连接与资源所有权

连接状态为 `idle / connecting / open / reconnecting / closed / disposed`。`connectTimeoutMs` 默认 10 秒，包含工厂等待与握手。自动重连默认关闭；配置 `reconnect: { delayMs: 1000, maxAttempts: 5 }` 后使用固定间隔，连接成功时重置尝试次数。

| 操作 | 行为 |
| --- | --- |
| `close()` | 关闭 socket，停止重连；Client 可再次连接 |
| `detach()` | 清理本库绑定与任务，保留外部 socket |
| `dispose()` | 关闭连接、销毁内核与注册；Client 不可复用 |
| `takeover(connection)` | 接管已打开的 `{ socket, send }` |

每次连接创建独立 Peer。断连结束该连接的请求、流和普通订阅；请求处理器和 `onStream` 注册跨连接保留。重连不重放业务命令，普通订阅需重新创建。异步 decode 保持连接内顺序，断连后的迟到结果不会进入另一连接。

自定义 Node 传输应将 `ws.send` callback 包装为 Promise，准确反馈发送完成与失败。浏览器原生 `send` 完成表示提交至缓冲区，两者均不表示对端业务处理完成。

## 资源保护

| 配置 | 管理的资源 | 默认行为 |
| --- | --- | --- |
| `queueLimits.receive / send` | 等待调度的任务数量 | 不限；不统计运行中任务 |
| `transportLimits.receive / send` | 单消息与积压传输字节 | 不限；只计量相应传输阶段 |
| `transportLimits.send.highWaterMark` | 原生发送缓冲与当前待发送帧 | 未配置时关闭 |
| `taskLimits` | 逻辑请求和主动流登记数 | 每 Peer 每方向请求各 1024，其余不限 |
| `taskTimeouts` | 入站请求执行期限、流空闲期限 | 关闭 |

请求流在每一端各计为一个请求，结果流不重复占主动流额度。`request` / `requestStream` 的 `timeoutMs` 默认 30 秒，覆盖请求登记到结果终结，与流空闲期限独立。

普通通知的出站队列或传输超限仅拒绝本次调用；入站或带任务身份的传输超限关闭连接，队列使用关闭码 4008，字节与水位使用 4009。活动任务超限和任务期限只结算相关任务；反馈控制帧若又触发传输超限，则执行该层关闭规则。原始错误通过 `onIssue` 观察。

`signal` 支持排队取消和业务协作退出。逻辑任务结束会回收库内登记，但不会强制停止忽略取消的用户 Promise。这些限额不等于 CPU、全部堆内存、真实业务并发或端到端交付保证，也不拦截外部直接调用 socket 的写入。

配置默认值、错误类型与计量范围见[资源与期限](../../docs/manual/reference/resources.md)。完整 API 见 [Client 参考](../../docs/manual/reference/client.md)，升级见[迁移说明](../../docs/manual/reference/migration.md)。

## 源码与维护

- `src/client.ts`：环境无关宿主、连接状态、Peer 隔离与公共 API。
- `src/browser.ts`：原生 socket 工厂；`src/node.ts`：`ws` 工厂与 callback 错误传播。
- `src/index.ts` 与 package.json exports：环境条件导出和声明入口。

Node 导入限定在 Node 入口。连接回调、异步 codec、连接期限与重连计时器必须检查归属并可清理。任务语义在 runtime-core 修改，同时验证两个方向；事件泛型不替代运行时数据校验。

在仓库根执行 `pnpm run check-types`、`pnpm test`、`pnpm run verify-packages`。环境与连接行为由 client/transport 测试覆盖，安装验证检查包入口与示例类型；浏览器验证方式见[贡献指南](../../CONTRIBUTING.md)。
