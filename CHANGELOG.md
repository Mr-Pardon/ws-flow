# Changelog

## 1.0.0 — 2026-10-29

首个完整架构版本：Client 2.0.0；Server、RuntimeCore、Envelope、Pipeline、Scheduler、Consumer 1.0.0。

### 通信模型

- 提供通知订阅、双向单结果请求、请求结果流与主动流，统一请求关联、取消、错误传播和断连清理。
- Client 可独立连接第三方服务，默认采用 generic 模式；浏览器使用原生 WebSocket，Node.js 使用 `ws`。
- 协议适配、插件管线与调度策略可按需组合；Server 提供 Session、房间筛选与多目标结果收集。
- 支持可选的队列、传输字节、活动任务和期限限制，以及发送缓冲高低水位。
- 提供用户手册、API 参考、完整案例和模块维护说明。

### 连接与异常处理

- Server 通过 `server_error` issue 暴露底层监听失败等宿主异常。
- `attach` 只接纳已打开的 socket；关闭 Server 时释放连接、观察器与 issue 注册，接管外部服务器时保留调用方监听。

### Client 2.x 兼容信息

- 使用 `subscribe` / `on` 订阅通知，使用 `request` / `requestStream` 接收任务结果；不提供 Client 1.x 的代理和 `watch` API。
- 连接配套 Server 时设置 `protocol: 'ws-flow'`；默认 generic 模式保留外部消息结构。
- 连接重建后重新注册普通订阅；请求与流不会自动重放。

升级步骤见[Client 迁移说明](docs/manual/reference/migration.md)。
