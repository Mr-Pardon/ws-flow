# 双向通信架构与 API 基线

> 历史设计草案。当前 API 与完成状态见[架构编排与设计说明](架构编排与设计说明.md)，下文未落地的提案不作为当前能力承诺。

日期：2026-09-20。状态：设计基线草案，尚非已实现 API。

本文承接项目作者对通用双向通信模型的确认。历史架构文档保留作为演进记录；涉及下一阶段设计的冲突，以本文和《任务与流生命周期》为讨论基线。下面明确区分已确认原则与建议方案，建议方案不代表作者已逐项确认。

## 1. 已确认的目标与边界

- 从完整消息流中，通过事件匹配和订阅模型分出业务关心的消息。
- 发起请求后，自动关联和收集本次请求的结果，不要求业务再自行捕获结果事件。
- 同时支持单个结果、流式结果和主动流推送。
- Client 与 Server Session 都具备发送、订阅、请求、响应和流式通信能力。
- 插件管线负责模块化消息预处理；调度层负责可替换的执行策略。
- 通信能力对称，宿主职责不同；单连接任务由内核管理，多连接任务由组合层协调。
- 先完成 Server 和共享内核，再重构 Client；这属于实施顺序，不缩减双向能力的设计范围。
- Client 可以独立接入任意 WebSocket 服务，不强制服务端使用 ws-flow Envelope。配套协议通过预置适配器提供。
- Client 的传输按环境选择：浏览器使用原生 WebSocket，Node 使用 ws；传输选择与消息协议选择是两条独立的配置边界。

## 2. 用户心智模型

| 概念 | 含义 | 结束条件 |
| --- | --- | --- |
| 消息 / 事件 | 一次单向通知，可有事件名 | 交付或处理结束 |
| 订阅 | 对后续一组消息表达兴趣 | 取消、所属连接销毁或明确的订阅策略 |
| 请求 | 发起一次操作，取得属于本次调用的结果 | 成功、失败、取消、超时、断连 |
| 流 | 一次有身份、按顺序传输的数据序列 | end、error、取消或断连 |

订阅本身不发送业务命令。请求自带结果关联，不依赖事件名唯一。请求可以返回流，但主动推送流不要求存在请求。

## 3. 模块职责

```text
WSClient ──────────┐
                  ├─ RuntimeCore
WSSession ────────┘    ├─ Dispatcher：编排收发步骤
                       ├─ SubscriptionRegistry：消息匹配、扇出、取消订阅
                       ├─ HandlerRegistry：请求处理器选择
                       ├─ RequestManager：待完成请求、结果关联、超时和取消
                       ├─ Stream：生产、消费、状态和缓冲
                       ├─ Pipeline：转换、校验、拦截
                       ├─ Scheduler：队列、并发与顺序策略
                       └─ Observer：旁路观察

Server 宿主：连接接入、Session、房间、目标选择、公共处理器注册
Client 宿主：连接、重连、连接代际与可选订阅恢复
传输适配：编码 / 解码、transport send、连接关闭通知
```

Registry 和 Manager 是建议的内部职责名称，不要求新增独立 npm 包，也不要求创建公共 Endpoint 基类。

标准 Envelope 定义 ws-flow 配套协议。Codec 只负责编码与解码；Adapter 提取可选的事件、关联和流信息，并保留原始业务消息。通用 Client 的基础发送和订阅不要求外部消息具备标准 Envelope。不能仅凭一条任意外部消息就推断出请求关联或流生命周期。

当前 runtime-core 已通过 ProtocolAdapter 映射外部消息与内部 RuntimeMessage；通用格式收发、接收流隔离与清理、发送流本地取消、单结果及流式请求关联、请求远端取消已实现。Client 尚未迁入，主动流远端取消、多目标组合与完整背压仍未实现。已落地 API 见《协议适配实现》《单结果请求实现》《流式请求实现》，本文其余部分仍是目标设计。

### 传输环境与协议能力

| 维度 | 选择 | 对另一维度的影响 |
| --- | --- | --- |
| 浏览器传输 | 原生 WebSocket | 不要求标准协议，也不应引入 Node ws 实现 |
| Node 传输 | ws | 同样允许任意外部服务协议 |
| 接管既有连接 | takeover 或未来显式工厂 | 复用调用方传输，不重新创建连接 |
| 通用协议 | Codec + 可选 Adapter 能力 | 发送 / 订阅先可用，请求 / 流按协议能力开放 |
| 配套协议 | 预置 wsFlowAdapter（草案） | 提供完整事件、关联、流生命周期与取消语义 |

不以“环境存在全局 WebSocket”直接认定为浏览器；按本项目约定，Node 默认仍使用 ws。Worker、SSR 与自定义运行时的检测 / 工厂覆盖在 Client 重构时单独验证，不能只用 window 判断宣称覆盖全部环境。

ws 是 Client 的可选 peer dependency：浏览器用户无需安装，Node 默认传输需要安装。共享内核不直接依赖 ws。浏览器打包时不引入 Node 传输，需要单独的产物测试验证。

通用协议能力按条件开放：无匹配信息时仍可订阅原始消息；具备事件提取时开放事件名快捷 API；具备关联或明确 matcher 时开放请求；具备归属和终止条件时开放流任务；远端支持取消命令时才能传播取消。缺少必要能力应明确报错，不悄悄包装成标准协议发给远端。

基于 matcher 的请求若没有唯一关联信息，需限制歧义并发或声明结果分发策略；不承诺适配器可以消除协议本身的信息缺失。轻量化通过按需状态和构建体积验证，不仅依赖拆包命名。

业务 Events、请求输入和结果类型由宿主 API 提供类型体验；内核通过 Peer 和规范消息工作。runtime-core 不依赖 Server、Client 或 Node 专有 transport。

## 4. 双端能力矩阵

下面的名称与签名是建议草案。Client 与 Session 的语义保持相同，Server 提供面向全部连接的注册入口。

| 能力 | Client / Session 建议 API | Server / Sender 角色 |
| --- | --- | --- |
| 无事件名消息 | send(data, options) | Sender 多目标发送 |
| 有事件名通知 | emit(event, data, options) | Sender 多目标发送 |
| 简单事件监听 | on(event, handler) | Server 注册公共监听 |
| 规则订阅 | subscribe(filter, options) | Server 可按连接与消息过滤 |
| 单次请求 | request(event, input, options) | 通过指定 Session 发起 |
| 请求处理 | handle(event, handler) | Server 注册公共处理器 |
| 流式请求 | requestStream(event, input, options) | 通过指定 Session 发起 |
| 流式请求处理 | handleStream(event, handler) | Server 注册公共处理器 |
| 主动发送流 | sendStream(event, source, options) | Sender 通过 factory 为每个目标创建 source |
| 接收主动流 | onStream(event, handler) | Server 注册公共流接收器 |
| 任务取消 | options.signal；流任务 cancel() | 转发到对应连接任务 |
| 原始规范消息 | sendEnvelope(envelope, options) | 高级接口，仍经过校验和发送管线 |

### 建议返回值

- send / emit：Promise<void>，代表本地传输发送完成，不代表远端业务已处理。
- on / handle / handleStream / onStream：返回幂等的注销函数。
- request：Promise<Result>，使用 AbortSignal 取消；正常返回 undefined 也必须产生成功响应。
- requestStream：立即返回 StreamTask<Chunk>，可异步迭代、取消并查询状态；发送失败通过任务失败呈现。
- subscribe：返回 Subscription，支持异步迭代和取消。简单回调形式由 on 提供。
- sendStream：Promise<void>，在所有帧本地发送完成后完成；不承诺远端完成消费。

异步迭代是 Subscription / StreamTask 的建议基础消费方式。是否额外暴露 onData，以及回调与迭代能否并存，先保持为待收敛项；不能隐式维护无人读取的第二份无限缓冲。

### 使用示意（目标 API，当前不可直接运行）

```ts
const off = client.on('device:changed', (data, ctx) => {
  console.log(data)
})

server.handle('user:get', ctx => findUser(ctx.data.id))
const user = await client.request('user:get', { id: 1 }, {
  timeoutMs: 10_000,
  signal: controller.signal,
})

client.handle('device:status', ctx => readDeviceStatus(ctx.signal))
const status = await session.request('device:status', {})

server.handleStream('report:export', ctx => exportChunks(ctx.data, ctx.signal))
const task = client.requestStream('report:export', { id: 1 }, {
  signal: controller.signal,
})
for await (const chunk of task) {
  await saveChunk(chunk)
}
```

示例中的业务函数与 controller 由应用提供。handler 使用统一 context 形态：data、peer / session、signal、只读请求元信息；具体类型定义在协议契约收敛后实现。

## 5. 订阅、处理器与管线不能混用

### 订阅扇出

一条事件可以匹配多个订阅，各订阅独立收到消息。不要求订阅者调用 next()。一个订阅者失败或取消，不影响其他订阅者。

普通订阅默认接收通知消息；请求和响应不作为普通通知再次分发。全链路观察使用 Observer，避免事件 handler 意外响应 response 或重复处理 request。

分发时固定匹配者快照。共享消息视为只读，分支变换不能回写其他订阅。具体隔离方式需在实现时验证，不能仅靠浅拷贝宣称深层数据隔离。

### 请求处理器

建议按精确事件名选择唯一处理器。单结果与流结果使用同一个注册命名空间，重复注册应报错；通配处理器以后单独设计优先级。

Session 本地处理器优先于 Server 公共处理器；未命中时才回退。Server 公共注册对现有和未来 Session 生效，不复制多份注册记录。

未找到处理器时返回明确的 METHOD_NOT_FOUND。handler 返回值是业务结果，不通过 data、meta 等字段猜测是否为 envelope。

### 中间件

管线是有序转换 / 拦截链，next() 控制同一阶段内部后续中间件。跨阶段是否采用完整洋葱模型尚未确定；不得用 next() 是否调用来决定多个订阅是否收到消息。

通知被过滤可以正常结束；请求被拒绝必须返回关联错误；响应被拒绝必须让本地关联任务失败。不能静默丢弃后留下永久等待。

控制字段在任务登记后不可任意修改。业务 data / meta 可按阶段变换，发送前重新验证规范消息。

观察回调不改变消息或任务状态；同步抛错与异步拒绝均被隔离。

## 6. Server 与 Client 保留的差异

- Server 管理 Session、房间和公共注册；Client 管理连接建立与重连。
- Sender 选择目标，复用每个 Session 的能力，不成为另一套请求内核。
- Client 的 watch 如保留，是订阅、命令发送、恢复策略的高层组合，不进入 stream core。
- 新连接属于新代际，旧请求不自动重发。重新发送可能重复执行非幂等业务，必须由业务或明确策略负责。

## 7. 多目标请求：设计范围内的组合能力

建议单独提供 requestAll / collect 类入口，而不是让 request 同时返回单值和数组。

- 调用时固定目标 Session 快照；后续新连接不加入这次调用。
- 每个目标有独立关联任务、超时和取消。
- 返回结果包含目标身份和成功 / 失败状态，部分失败不丢弃其他结果。
- 默认等待全部目标完成各自结算；空目标返回空结果。
- 首个成功、法定数量、结果按到达顺序输出等策略作为后续组合策略，不改变单连接协议。
- 调用取消时取消所有尚未结束的子任务。

以上是建议默认行为。多目标流还需要明确聚合输出、背压与单目标失败策略，不能直接把单流 API 广播后宣称完成。

## 8. 与当前实现的迁移关系

| 当前实现 | 处理方向 |
| --- | --- |
| runtime-core / pipeline / scheduler 分层 | 保留，补齐共同通信职责 |
| route 与 handle 作为 receive middleware | 分开事件扇出与请求处理，不再用一个链表承担所有语义 |
| handle 的 autoSend smart | 迁移为确定的请求结果返回，评估弃用策略 |
| session.stream | 收敛为明确的主动发送流 API，是否保留别名后续决定 |
| client.request 依赖 ws-event-proxy | Client 重构时接入统一 RequestManager |
| client.watch | 拆开底层订阅 / 请求与高层恢复策略 |
| envelope 只有可选字段 | 增加明确消息类别及合法组合校验 |

API 草案涉及行为变化，不能未经迁移说明就作为现有 client 1.x 的兼容升级发布。

## 9. 后续需要作者参与的集中评审点

不需要逐项审批内部实现。优先在编写公共 API 类型前集中评审：

1. request / requestStream 与 handle / handleStream 是否采用显式分组。
2. subscribe 是否以可迭代 Subscription 为核心，on 保持回调快捷入口。
3. Session 处理器覆盖 Server 公共处理器的优先级是否符合预期。
4. 多目标请求是否采用上述“固定快照 + 每目标结算”的默认模型。

实现者可按本文建议先补内部契约与测试；公共 API 名称和协议字段在评审前均标记为草案。
