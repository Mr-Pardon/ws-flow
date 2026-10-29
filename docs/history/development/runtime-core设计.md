# @ws-flow runtime-core 设计

> 历史设计草案。当前内核说明见[模块 README](../../../packages/runtime-core/README.md)和[架构模型](../../manual/design/index.md)。

## 一、设计前提

`server` 和 `client` 后续虽然会保留各自的宿主 API：

- `server`
  - `WSServer`
  - `WSSession`
  - `route / handle`
- `client`
  - `WSClient`
  - `on / subscribe / watch / request`

但它们内部的运行时数据处理主链，应尽量统一为同一条执行路径，并且统一流经 `runtime-core`。

也就是说，未来真正应该共享的不是“表层 API”，而是：

```txt
source
-> protocol decode
-> receive dispatcher
-> scheduler
-> pipeline
-> route / stream sink

send api / command
-> send dispatcher
-> scheduler
-> pipeline
-> protocol encode
-> transport
```

这条链路一旦统一，后续：

- `plugin pipeline`
- `scheduler`
- `stream`
- `dispatch`
- `protocol adapter`

都可以以相同的运行时模型服务双端。

---

## 二、为什么需要 runtime-core

当前已经拆出的 `@ws-flow/pipeline` 和 `@ws-flow/scheduler` 都比较像“基础设施包”。

但继续往下拆时会遇到一个问题：

- `dispatcher`
- `stream`
- `deps contracts`
- `runtime assembly`

这几部分已经不再是松散平级模块，而是强协作的数据流执行链。

如果继续强行全部拆成细粒度独立包，会出现这些问题：

1. 语义边界变得奇怪  
   某些包脱离上下游后不再是自然完整的概念。

2. 组装成本升高  
   `server/client` 都要重新拼很多 deps。

3. 宿主层过重  
   `server/client` 会承担过多运行时装配职责。

因此更自然的结构是：

- `pipeline` 独立
- `scheduler` 独立
- `dispatcher + stream + deps + assembly` 收束为 `runtime-core`

---

## 三、runtime-core 的定位

`runtime-core` 不是宿主层，也不是单一基础设施。

它的角色是：

> WebSocket 应用层消息运行时执行内核

它负责：

- 组织消息进入系统后的执行顺序
- 组织流的生产与消费
- 调用 scheduler 和 pipeline
- 维护运行时需要的局部 store / deps / factories

它不负责：

- 提供面向用户的宿主 API
- 管理具体 websocket server/client 生命周期
- 暴露房间、重连、订阅体验等宿主能力

---

## 四、runtime-core 与其他包的关系

建议未来包层级如下：

```txt
@ws-flow/pipeline
@ws-flow/scheduler
@ws-flow/runtime-core
@ws-flow/protocol
@ws-flow/server
@ws-flow/client
```

依赖方向：

```txt
server/client
  -> runtime-core
  -> protocol
  -> pipeline
  -> scheduler
```

其中：

- `pipeline` 提供 middleware registry 与 compose 能力
- `scheduler` 提供调度器协议与默认实现
- `protocol` 提供 strict/generic 适配协议
- `runtime-core` 负责编排执行链
- `server/client` 只做宿主适配与高级 API 暴露

---

## 五、统一后的双端链路

## 1. Server

### 接收

```txt
ws transport
-> protocol decode
-> runtime-core.receiveDispatcher
-> scheduler
-> receive pipeline
-> route middleware
-> receive stream / handler
```

### 发送

```txt
session / sender
-> envelope build
-> runtime-core.sendDispatcher
-> scheduler
-> send pipeline
-> protocol encode
-> ws transport
```

### 主动流

```txt
generator
-> runtime-core.sendStream
-> start/chunk/end/error
-> sendDispatcher
```

## 2. Client

### 接收

```txt
ws / proxy source
-> protocol decode
-> runtime-core.receiveDispatcher
-> scheduler
-> receive pipeline
-> route / subscribe sink
-> receive stream / consumer
```

### 发送

```txt
send / request / watch command
-> runtime-core.sendDispatcher
-> scheduler
-> send pipeline
-> protocol encode
-> ws / proxy transport
```

### watch

```txt
watch host api
-> subscribe resolver
-> receive stream core
-> stream pipeline
-> user consumer
```

## 3. 核心约束

这里最重要的约束是：

> `server/client` 的运行时主链必须流经 `runtime-core`，而不是各自维护一套完全不同的执行模型。

允许不同的是：

- 宿主 API 形式
- transport 接入方式
- route/subscribe 的上层体验

不应不同的是：

- dispatcher 驱动模型
- pipeline 执行位置
- scheduler 参与时机
- stream lifecycle 语义

---

## 六、runtime-core 的职责边界

建议 `runtime-core` 包含以下能力。

### 1. Dispatch

- `ReceiveDispatcher`
- `SendDispatcher`
- dispatcher deps contracts
- route middleware merge
- invalid stream reject flow

这部分属于执行链核心编排，不再适合作为完全孤立的基础包。

### 2. Stream

- `SendStreamCore`
- `ReceiveStreamCore`
- stream lifecycle types
- stream consumer/iterator buffer（如果后续收进来）

这里强调：

- `stream` 内核只负责流的生产与消费
- `subscribe/watch` 语义属于宿主层，不放进 core

### 3. Runtime deps / assembly helpers

- `createSendDispatcherDeps`
- `createReceiveDispatcherDeps`
- `createSessionDeps`
- `createSendStreamDeps`

这部分显然是 runtime 组装层能力，最适合留在 core 内。

### 4. Runtime stores（可选逐步收束）

如果后续需要，也可以收进：

- receive stream records
- session records 的抽象 store

但第一阶段不必强行全部纳入。

---

## 七、不属于 runtime-core 的能力

### 1. Pipeline

已独立为 `@ws-flow/pipeline`。

原因：

- 它本身是通用 registry / compose 基础设施
- 不依赖具体运行时执行链

### 2. Scheduler

已独立为 `@ws-flow/scheduler`。

原因：

- 它是通用调度基础设施
- 双端复用场景清晰

### 3. Session

`session` 暂时仍属于 `server` 宿主层。

原因：

- `session` 是 server 宿主语义的一部分
- client 没有完全对等的 session 模型

未来如需统一，可再演进出更通用的 `SessionLike / EndpointLike`。

### 4. Rooms / reconnect / subscribe APIs

这些都属于宿主层能力：

- `server` 的 rooms
- `client` 的 reconnect
- `client` 的 on/subscribe/watch

不应进入 `runtime-core`。

---

## 八、关于 Events 和 SessionMeta 的关系

当前阶段暂不强行对 `Events` 和 `SessionMeta` 做宿主层瘦身，但要明确它们在 `runtime-core` 里的地位：

### 1. Events

`Events` 未来更适合服务于：

- envelope 的 `event -> data` 类型映射
- 宿主 API 的类型体验

而不是服务于 runtime 内核本身。

### 2. SessionMeta

`SessionMeta` 未来更适合逐步收敛到 `session` 宿主语义中，而不是作为 runtime-core 的核心泛型。

### 3. 当前策略

当前拆离内核时，不急着删除宿主层的泛型，而是优先做到：

- 新拆出的基础包尽量不直接感知 `Events / SessionMeta`
- `runtime-core` 尽量通过 `Ctx` / `SessionLike` / `EnvelopeLike` 间接感知

这样后续宿主层自然瘦身会更顺。

---

## 九、建议的文件归属

### 第一阶段保留在 `server`

- `core/wss.ts`
- `core/session.ts`
- `core/sender.ts`
- rooms 相关逻辑
- server host runtime assembly

### 第一阶段迁入 `runtime-core`

- `core/receiveDispatcher.ts`
- `core/sendDispatcher.ts`
- `core/receiveStream.ts`
- `core/sendStream.ts`
- `runtime/deps.ts`
- `runtime/dispatchers.ts`

### 已独立

- `pipeline`
- `scheduler`

---

## 十、建议的演进顺序

### 第一步

完成：

- `@ws-flow/pipeline`
- `@ws-flow/scheduler`

并接回 `server`。

### 第二步

设计 `runtime-core` 结构，但先不急着挪所有文件。

目标是先明确：

- core 的职责边界
- server/client 都会流经它
- 它依赖 `pipeline/scheduler/protocol`

### 第三步

先从 `stream + dispatcher deps` 开始迁入 `runtime-core`。

原因：

- 这两块已经高度协作
- 与宿主层边界较清晰

### 第四步

在 `client` 重构时，以 `runtime-core` 为中心重建接收/发送链路。

目标：

- `client` 外部 API 保持易用
- `client` 内部执行模型与 `server` 对齐

---

## 十一、结论

当前最合理的整体收束方向是：

1. `pipeline` 和 `scheduler` 继续保持独立基础设施包。
2. `dispatcher + stream + deps + runtime assembly` 收束为 `runtime-core`。
3. `server/client` 后续必须以 `runtime-core` 作为内部运行时数据处理主链。
4. `server/client` 的差异保留在宿主 API 和 transport/route 适配层，而不是保留在执行模型本身。

最终目标不是让 `server/client` 看起来一样，而是让它们：

> 在不同宿主语义下，共享同一套实时通信运行时执行内核。
