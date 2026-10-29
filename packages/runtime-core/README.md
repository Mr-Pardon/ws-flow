# @ws-flow/runtime-core

与 socket 环境无关的通信任务内核。Server 和 Client 共用它的通知订阅、请求关联、流推进与连接清理。自定义连接宿主可通过传输接口、协议适配器和调度器组合这些能力。

## 安装

```sh
npm install @ws-flow/runtime-core@^1
```

使用 Client 或 Server 时，内核作为依赖自动安装。直接接入内核适合实现自定义传输宿主，需要由调用方管理连接与编解码。

## 模型与依赖

RuntimeCore 组合 envelope 构建器、pipeline 注册表、scheduler 和 consumer；不导入 ws，不创建连接、不解析 JSON、不决定重连。

输入是宿主解码后的值，ProtocolAdapter 将其映射成 RuntimeMessage。Peer 表示一个连接周期，其对象身份决定任务归属；peer.id 同时用作 SessionScheduler 的调度键，宿主应分配唯一 ID，不能在重连时复用旧 Peer。

## 最小接入

```ts
import { RuntimeCore, genericAdapter } from '@ws-flow/runtime-core'
import { SessionScheduler } from '@ws-flow/scheduler'

const core = new RuntimeCore({
  base: {
    protocol: genericAdapter(),
    transport: {
      send: (peer, message) => console.log('交给宿主发送', peer.id, message)
    }
  },
  schedulers: {
    in: new SessionScheduler(),
    out: new SessionScheduler()
  }
})

const peer = { id: 'connection-1', state: 'open' as const }
const messages = core.subscribe(() => true, { peer, capacity: 16 })

await core.dispatchers.receive.dispatch(peer, { type: 'notice', value: 1 })
console.log((await messages.next()).value?.raw)
await core.send(peer, { action: 'ack' })

core.closePeer(peer)
core.dispose()
```

未传 protocol 时默认 wsFlowAdapter；未传调度器时 SchedulerRuntime 默认 ImmediateScheduler。Server/Client 宿主明确注入 SessionScheduler，不能把宿主默认值误认为裸内核默认值。

## 执行链

接收：adapter.read → 校验 → 入站调度 → receive 中间件 → 任务身份校验 → 请求表/主动流/通知分发。

发送：内部消息 → 出站调度 → send 中间件 → 任务身份校验 → adapter 编码 → 宿主 transport.prepare（可选）→ transport 发送。

ProtocolAdapter 可分别提供普通编码、请求编码、请求取消、请求流、主动流和主动流取消。ID、任务表、超时归运行时所有；第三方协议不能仅通过字段猜测启用 RPC。

## 核心不变量

- 入站与出站请求按 Peer 隔离，两个方向相同 ID 不冲突；结果/超时/失败/取消只结算一次。
- 请求先注册再发送，结算后清除记录、计时器和监听；默认总期限 30 秒，null 禁用。
- 每 Peer 入站/出站请求默认各最多 1024 个，可通过 taskLimits 调整；不提供已完成 ID 的去重缓存。
- 业务请求处理器和流消费者不占据帧调度的整个生命周期；中间件不可等待同连接未来帧。
- StreamTask 为 waiting/open/ended/failed/cancelled；响应流有 requestId + streamId，主动流只有流身份。
- 订阅按原始消息筛选，各有快照和队列；默认 128 条，不能克隆的值或溢出只使对应订阅失败。
- 接收流默认缓冲 128 条，错误可从迭代抛出。break/cancel/断连负责回收，远端取消尽力发送。
- closePeer 后不能重新激活同一个 Peer；dispose 后不可继续使用内核。

请求、控制帧和无 kind 主动流均保护任务身份，禁止中间件把普通消息提升为流或把流降为普通消息。流钩子可改数据，但不能直接修改 peer/state；提前结束/失败使用 ctx.end()/ctx.fail()。接收流帧在消息中间件被破坏时，原消费者失败并尝试取消原流。所有有界条数队列都不等于端到端背压。

## 活动任务预算与期限

构造参数顶层 taskLimits 接受 maxActive/maxActivePerPeer，还可对 incomingRequests、outgoingRequests、incomingStreams、outgoingStreams 各设置同名字段。所有数量须为正安全整数；总预算和分类预算同时生效。默认仅 incomingRequests/outgoingRequests 各有每 Peer 1024 的上限，其余维度不限制。内部 ActiveTaskBudget 负责原子申请、按 Peer 对象隔离与幂等释放。

requestStream 在发起端计入 outgoingRequests，处理端计入 incomingRequests，结果流不重复占主动流额度。出站请求先登记后排队；入站请求通过消息管线后、调用 handler 前登记。主动接收流在有效新 start 上申请；主动发送流从 createSendStream 分配起申请，即使没有发送也计数。终态、失败、超时、取消、closePeer/dispose 回收额度，迟到释放不能扣减后续任务。

taskTimeouts.incomingRequestMs 限制请求被接纳后的 handler、流式结果和响应发送总时间；此前的入站排队与消息中间件不在该期限内。taskTimeouts.streamIdleMs 控制流进度：消费者从接纳 start、生产者从发送流分配开始，只有通过流钩子的有效 start/chunk 或成功发送的 start/chunk 续期。重复、过滤或其他任务的帧不续期。两者默认禁用，undefined/null 都禁用；启用值须是正整数且不超过 2147483647 毫秒。

requestStream 与 createSendStream 的 options.idleTimeoutMs 覆盖本端全局流期限；null 显式关闭，不修改远端配置。出站请求 timeoutMs 默认为 30 秒，包含本地排队、发送及等待结果，独立于空闲期限。入站执行超时返回 EXECUTION_TIMEOUT：结果流开始前使用 request_error，开始后使用 request_stream 错误终态；已提交终态不重复发送。流空闲超时为 STREAM_IDLE_TIMEOUT，请求结果消费者会尽力取消对应请求。

TaskCapacityError 使用 CAPACITY_EXCEEDED，并提供 kind、scope、limit、actual、budget；budget 为 all 表示顶层总预算，否则表示分类预算。TaskTimeoutError 提供 EXECUTION_TIMEOUT/STREAM_IDLE_TIMEOUT 和 timeoutMs。任务资源失败通过 useTaskErrorObserver(handler) 观察，handler 接收 {peer,kind,error}；它只影响相应任务，不关闭 Peer。新的入站请求收到错误响应，新的主动流尽力取消，本地超限任务不会发送；若反馈发送再触发队列/传输限制，则仍遵循对应层的 fatal 策略。

getActiveTaskCount(peer?,kind?) 返回当前逻辑登记数量，既可查询总数，也可按 Peer/类别查看。该数值不代表仍在执行的用户代码数量：期限到达能结算任务并抑制迟到结果，但忽略 signal 的 Promise 或生成器可能继续持有内存。流中间件的 ctx.signal 支持协作取消；无法强制停止任意用户异步代码。handler 脱离收帧调度运行，WSScheduler 并发限制不能约束其整个执行期；需要真实执行上限时，应在业务函数外使用持有到真实完成的执行池。完整契约见[资源与期限](../../docs/manual/reference/resources.md)。

## 字节预算与传输准入

本包提供浏览器安全的传输资源原语，Server/Client 在 codec 边界组合使用；裸 RuntimeCore 不解析或计量原始 socket 数据，也不会自动给自定义宿主配置上限。

| 导出 | 职责 |
| --- | --- |
| ByteLimits / SendTransportLimits | 单消息、全局及每 Peer 字节上限；出站另含高低水位和等待参数 |
| ByteBudget | 原子预占和幂等释放，统计等待与执行中的 reservation |
| wireByteLength | UTF-8 文本、ArrayBuffer、视图、Blob 及视图数组的精确载荷字节数 |
| validateSendLimits | 校验并复制配置，补齐启用高水位后的低水位和计时默认值 |
| waitForWritable | 检查 bufferedAmount，按低水位恢复，处理期限、取消和断连 |
| TransportSendQueue | 按 Peer 串行化水位准入至实际发送完成，隔离自定义并发调度器 |
| TransportLimitError | code、limit、actual、scope 等结构化诊断字段 |

ByteBudget 只对显式配置的维度限额，enabled 表示是否启用。reserve 返回释放函数；closePeer/clear 回收旧预占，迟到 release 不能扣减新周期的额度。宿主必须在 decode 前预占入站原始字节、encode 后预占出站编码结果，并负责成功、失败、取消和关闭时释放。它不统计对象堆内存，不能代替条数限制。

可选 transport.prepare(peer, data, signal) 返回 PreparedTransportSend：send() 提交捕获连接的消息，release() 幂等释放预占且不得抛错。Dispatcher 会先等待 prepare，重新检查 Peer 和 signal，随后才标记任务进入真实传输；准备期间取消不产生错误的远端取消帧。send 结算后在 finally 中释放。未实现 prepare 的宿主沿用 transport.send，需自行承担编码、缓冲和准入边界。

启用 highWaterMark 的宿主用 TransportSendQueue 包住 prepare，让同一 Peer 的下一次容量检查等待上一 send 完成。直接并发调用 waitForWritable 不会预占原生缓冲，不具备这个保证。水位默认 low=floor(high/2)、waitTimeoutMs=10000、pollIntervalMs=10；该期限从当前帧开始检查原生水位时计时，不覆盖前面的调度和准入排队。尚未开始的等待可由 signal 取消，已提交的任意用户发送回调仍需协作结束。

useTransportErrorObserver 提供 {peer,error,direction,fatal}；普通出站通知触发 TransportLimitError 时只拒绝该次发送，任何带任务身份的出站错误均关闭 Peer。入站字节检查由宿主执行并关闭连接。fatal 时自定义宿主还必须关闭真实 socket，避免对端继续等待。具体配置与计量范围见[资源与期限](../../docs/manual/reference/resources.md)。

## 源码导航

| 文件 | 职责 |
| --- | --- |
| `src/protocol.ts` | 内部消息、Adapter、能力检查及校验 |
| `src/core/runtime.ts` | 依赖装配、公共 API、资源回收 |
| `requests.ts` | 请求关联、处理器、期限和取消 |
| `sendDispatcher.ts / receiveDispatcher.ts` | 消息执行链 |
| `sendStream.ts / receiveStream.ts / streamTask.ts` | 发送流、接收流、请求流消费 |
| `subscriptions.ts` | 筛选、快照、独立队列 |
| `src/transportLimits.ts` | 字节计量、预算、水位等待、错误与参数校验 |
| `src/transportSendQueue.ts` | 每 Peer 发送准入与完成顺序 |
| `src/taskResources.ts` | 活动任务分类预算、计数、期限与结构化错误 |
| `src/types/` | 宿主边界与中间件公开声明 |

表中省略目录的文件均位于 src/core。扩展任务协议时同时更新 Adapter 校验、收发分支、清理路径和双向真实 socket 测试；不能只增加发送字段。

## 验证与限制

根目录运行 `pnpm run check-types`、`pnpm test`、`pnpm run verify-packages`。tests/protocol、subscriptions、requests、request-stream 和两组 lifecycle 测试是核心契约。

注入的内置排队调度器可配置全局/每 Peer 等待限额；运行时为调度任务传入 signal，closePeer/dispose 会取消仍在原调度器中的排队项。收发中间件可用 ctx.signal 协作退出，已开始的用户 Promise 不保证立即结束。

useQueueErrorObserver 提供 {peer,error,direction,fatal}。普通出站通知超限只拒绝该次发送；入站或带任务身份的出站超限关闭 Peer，自定义宿主必须在 fatal 通知后关闭真实 socket，不能只留在本地状态。观察器失败不影响原始错误和清理。

内核管理连接周期内的任务，不提供持久化恢复。逻辑任务额度、字节预算与本地发送水位都不构成端到端背压，也不限制全部解码对象、业务持有内存、原生解析器和外部 socket 写入。有限等待队列不能保证所有正在执行的用户代码及时结束。

模块协作见[系统架构](../../docs/manual/design/index.md)，验证流程见[贡献指南](../../CONTRIBUTING.md)。
