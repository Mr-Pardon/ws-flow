# 从 Client 1.x 升级

本页面向使用 Client 1.x 的项目。Client 2.x 的 API 与 1.x 不兼容，升级时需要调整导入路径、协议配置和任务调用。首次使用 ws-flow 请直接阅读[快速开始](/guide/quick-start)。

## API 对照

| Client 1.x 用法 | Client 2.x 用法 |
| --- | --- |
| 默认按配套服务端消息格式工作 | 连接 ws-flow Server 时设置 `protocol: 'ws-flow'` |
| 通过 `watch` 等方式捕获任务结果 | 通知使用 `on/subscribe`，关联结果使用 `request/requestStream` |
| 从 `core/types` 等内部路径导入 | 使用包根、`/browser` 或 `/node` 的公开入口 |
| 根据全局 WebSocket 是否存在选择环境 | 浏览器使用 `/browser`，Node 使用 `/node` |
| 接管裸 socket | 提供含 `socket` 与 `send` 的 `ClientConnection` |

升级两端协议时，应同步部署相容的服务。保留第三方或自定义服务协议时，使用 generic 收发普通消息，或实现适配器映射已有请求、响应和流控制约定。ws-flow 不自动转换 Client 1.x 的任务协议。

## 升级步骤

1. **选择环境入口。** 浏览器使用原生 WebSocket；Node 安装 `ws` 并使用 Node 入口。
2. **配置消息协议。** 配套 Server 使用 `protocol: 'ws-flow'`；已有服务按需配置 codec 与协议适配器。
3. **选择通信 API。** 事件通知使用 `on/subscribe`，单结果调用使用 `request`，连续结果使用 `requestStream`。
4. **注册处理器。** `handleRequest` 返回单个结果，`handleStream` 返回结果流，`onStream` 接收主动流。
5. **绑定清理时机。** 在页面卸载或应用退出时关闭 Client，将任务的 `signal` 传给耗时业务。
6. **恢复业务订阅。** 每次重连重新建立普通订阅，持久请求处理器只注册一次。

## 重连后的订阅

```ts
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient({
  protocol: 'ws-flow',
  reconnect: { delayMs: 1000, maxAttempts: 5 }
})

// 请求处理器属于 Client 实例，重连后继续生效。
client.handleRequest('client.status', () => ({ ready: true }))

client.onState(state => {
  if (state === 'open' && client.state === 'open') {
    client.on('notice', data => console.log(data))
  }
})

client.onIssue(error => console.error(error))
await client.connect('ws://localhost:8080')

// 页面卸载或应用退出时调用。
export function shutdown() {
  client.dispose()
}
```

`onState` 异步通知状态变化。需要接住首次连接后的第一条消息时，应在 `connect()` 前订阅；重连后通过业务握手，让对端在订阅恢复后开始推送。

重连恢复连接，未完成请求和流不会自动重发。是否重试业务操作、如何避免重复执行，由应用决定。完整行为见[连接与生命周期](/guide/lifecycle)。

## 返回值与类型

`request` 返回 Promise；`requestStream` 直接返回可迭代的 `StreamTask`。使用请求流时，将任务创建和迭代一起放在 `try` 中处理错误，因为未连接等创建错误可以同步抛出。

`Events` 泛型约束事件名，部分事件方法的负载类型为 `Events[keyof Events]` 联合。请求使用 `request<Result>` 和 `handleRequest<Input, Result>` 分别描述结果与输入。类型声明不承担运行时数据校验，接收端应校验来自网络的输入。

协议选项和完整签名见 [Client API](/reference/client)，错误处理见[错误与排查](/reference/errors)，容量和期限见[资源与期限](/reference/resources)。
