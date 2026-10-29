# API 参考

查一个方法怎么调用、一个选项默认多少，从这里开始。日常业务只需使用 `@ws-flow/client` 和 `@ws-flow/server` 的公开 API；协议、插件与调度按需扩展。

两端共用任务与流内核，也各自管理连接。Client 可以单独接入已有 WebSocket 服务。

还没有运行第一个例子？先看[快速开始](/guide/quick-start)。需要理解模块分工时，再看[架构设计](/design/)。

## 查找入口

| 参考 | 你会找到什么 |
| --- | --- |
| [Client](/reference/client) | 浏览器与 Node 入口、连接、订阅、请求、流和协议配置 |
| [Server](/reference/server) | 创建服务、Session、处理器、房间、目标选择与聚合请求 |
| [错误处理](/reference/errors) | 请求错误码、观察器、异常传播与定位顺序 |
| [资源与期限](/reference/resources) | 缓冲、队列、字节、活动任务与超时的默认值和计量边界 |
| [迁移说明](/reference/migration) | Client 1.x 到 2.x 的替换方式与兼容边界 |

## 四种通信方式

| 意图 | 发起方 | 接收方 | 发起方得到什么 |
| --- | --- | --- | --- |
| 发送普通通知 | `send` / `emit` | `subscribe` / `on` | 本地发送完成的 `Promise<void>` |
| 请求一个结果 | `request` | `handleRequest` | `Promise<Result>` |
| 请求一段结果流 | `requestStream` | `handleStream` | 可异步迭代的 `StreamTask<Chunk>` |
| 主动推送一段流 | `stream` | `onStream` | 整段发送过程的 `Promise<void>` |

Client 和 Server Session 都可以发起请求和回应请求。`onStream` 处理对端主动发送的流，`handleStream` 回应由 `requestStream` 发起的任务。

## 共同约定

- `event` 是应用定义的字符串。请求事件必须非空；处理器对同一作用域、同一事件只允许注册一次。
- 请求与流控制帧由内核匹配，不进入普通通知订阅。通知订阅只观察通过接收管线的普通消息。
- `send` / `emit` 成功表示本地传输步骤完成。需要确认对端业务结果时使用 `request`。
- `signal` 用于协作取消；断连、超时与显式取消都会结束相关本地任务。已发生的远端副作用不会自动回滚。
- `request<Result>`、`requestStream<Chunk>` 的类型参数描述预期结果，不执行运行时数据校验。外部输入应由业务处理器或插件校验。
- 订阅和流迭代都要消费或显式结束。不要创建后一直闲置；缓冲耗尽会让相应订阅或流失败。

完整的清理与重连行为见[连接生命周期](/guide/lifecycle)，模块化接入见[插件与扩展](/guide/extensions)。

## 基础包与公开入口

`runtime-core`、`scheduler`、`consumer`、`pipeline`、`envelope` 组成模块化通信内核。安装 Client 或 Server 时，由包管理器解析它们的依赖；普通用户无需逐个手动安装。

需要编写自定义协议、调度器或复用内核时，可以显式依赖对应基础包并使用其公开导出。请不要依赖 `dist` 下的内部文件路径。基础模块的职责与组合关系见[架构设计](/design/)。
