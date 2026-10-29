# 资源与期限

先使用默认配置完成业务通信，再根据真实负载添加必要的限制。这些选项分别控制等待、缓冲、活动任务和持续时间，没有一个“总容量”参数能替代它们。

## 默认行为

| 资源 | 默认值 | 计量内容 |
| --- | --- | --- |
| 普通通知订阅 | 每订阅 `128` 条 | 尚未被读取的通知 |
| 请求结果流 / 主动接收流 | 每流 `128` 个分片 | 尚未被消费的 `chunk` |
| 收发调度队列 | 不限 | 等待运行的调度任务数，不含已经运行的任务 |
| 收发字节 | 不限 | 相应处理阶段中已计量的传输字节 |
| 活动入站请求 | 每连接 `1024` 个 | 已接受但尚未终结的请求，含请求流 |
| 活动出站请求 | 每连接 `1024` 个 | 已登记但尚未终结的请求，含请求流 |
| 活动主动流与任务总量 | 不限 | 主动流以及各类任务合计 |
| 请求总期限 | `30000ms` | 从本地请求登记到结果终结 |
| 入站请求执行期限 | 关闭 | 已接受入站请求的执行与响应生命周期 |
| 流空闲期限 | 关闭 | 有效流进展之间的间隔 |
| socket 发送水位 | 关闭 | 底层 `bufferedAmount` 与待发送数据的字节数 |

普通订阅容量可通过 `subscribe(..., { capacity })` 配置，Client 的 `on` 也接收此选项；请求结果流使用 `requestStream(..., { capacity })`。`onStream` API 不提供逐主动流容量选项，其接收缓冲采用默认值。

容量溢出会明确失败相应订阅或流，不静默丢弃旧数据，也不自动让对端按消费速度降速。端到端流量控制需要双方应用协商。

## 配置位置

只配置当前需要约束的资源。例如，每个连接最多同时登记 32 个未结束的入站请求：

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create(
  { port: 8080 },
  { taskLimits: { incomingRequests: { maxActivePerPeer: 32 } } }
)
```

Client 构造选项与 Server 的 `serverOptions` 均接受这些配置。Server 的第一个参数是底层 `ws` 配置，资源选项放在第二个参数。队列、字节和期限都可以独立添加，无需一起启用。

::: details 同时配置队列、消息大小、任务数与期限

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create(
  { port: 8080 },
  {
    queueLimits: {
      receive: { maxQueuedPerPeer: 128 },
      send: { maxQueuedPerPeer: 128 }
    },
    transportLimits: {
      receive: { maxMessageBytes: 256 * 1024 },
      send: { maxMessageBytes: 256 * 1024, highWaterMark: 1024 * 1024 }
    },
    taskLimits: {
      incomingRequests: { maxActivePerPeer: 32 }
    },
    taskTimeouts: {
      incomingRequestMs: 30_000,
      streamIdleMs: 15_000
    }
  }
)

server.onIssue('resource_error', issue => console.error(issue.error))
```

示例数值仅用于说明位置，并非所有业务的推荐阈值。长时间无数据也正常的业务流不应随意设置较短空闲期限。

:::

## 等待队列

`queueLimits.receive` 与 `queueLimits.send` 各自支持：

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `maxQueued` | 正安全整数 | 该方向所有连接合计等待任务数 |
| `maxQueuedPerPeer` | 正安全整数 | 该方向单连接等待任务数 |

未填写的维度不限制。任务进入运行阶段即离开等待预算，运行中的业务耗时不计入 `maxQueued`。取消会移除尚未执行的队列条目并释放其额度。

Client 默认按连接串行解码和接收、按连接串行发送；Server 默认入站和出站各使用独立的 `SessionScheduler`。请求 handler 与主动流消费的整个生命周期不会占住帧调度器，否则同连接的后续流分片或反向请求无法被处理。

因此，调度并发数不等于业务 handler 的实际并发数。如需限制不可取消的数据库查询或 CPU 工作，应在业务执行层管理并发。

## 字节预算与发送水位

`transportLimits.receive` 和 `transportLimits.send` 共同支持：

| 参数 | 单位 / 默认值 | 含义 |
| --- | --- | --- |
| `maxMessageBytes` | 字节；不限 | 一条原始或编码消息的最大长度 |
| `maxPendingBytes` | 字节；不限 | 该方向所有连接当前计量中的总字节数 |
| `maxPendingBytesPerPeer` | 字节；不限 | 该方向单连接当前计量中的总字节数 |

所有显式字节上限必须为正安全整数。计量支持字符串 UTF-8 长度、ArrayBuffer、Buffer/TypedArray 等视图、Blob，以及视图数组。

接收预算从原始帧进入宿主、解码之前开始，覆盖到对应派发完成；不会持续覆盖由该帧启动的完整业务任务。Client 的异步解码等待也在接收预算内。

发送预算从调度与中间件完成、codec 编码之后开始，覆盖等待水位和实际发送步骤；尚未编码的排队业务对象不在字节预算内。Node 的发送完成由 `ws.send` 回调确定；浏览器原生 `send()` 返回只代表提交到浏览器缓冲，不是对端确认。

`transportLimits.send` 还支持：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `highWaterMark` | 关闭 | 发送前要求缓冲量加本次字节不超过此值 |
| `lowWaterMark` | `floor(highWaterMark / 2)` | 进入等待后还需缓冲降至此值才继续 |
| `waitTimeoutMs` | `10000` | 单次等待发送缓冲下降的期限 |
| `pollIntervalMs` | `10` | 检查底层缓冲的间隔 |

后三项只能与 `highWaterMark` 一起设置。高水位为正安全整数；低水位为小于高水位的非负安全整数；时间为 `1..2147483647` 的整数。单条消息大于高水位时直接失败，不会无限等待。

同连接的水位检查与发送提交按顺序执行，避免多个发送同时判断“仍有空间”后共同冲破准入条件。取消、断连或到期会停止等待。

这些限制不构成 JavaScript 堆内存、底层网络缓冲或应用全局资源的硬上限：解码对象、业务缓存、未编码队列和不响应取消的 Promise 都可能另外持有内存。

## 活动任务

```text
taskLimits: {
  maxActive?: number
  maxActivePerPeer?: number
  incomingRequests?: { maxActive?: number, maxActivePerPeer?: number }
  outgoingRequests?: { maxActive?: number, maxActivePerPeer?: number }
  incomingStreams?: { maxActive?: number, maxActivePerPeer?: number }
  outgoingStreams?: { maxActive?: number, maxActivePerPeer?: number }
}
```

顶层限制总量，分类限制相应类别；一次登记必须同时满足四个适用维度：总量、每连接总量、类别总量和每连接类别总量。容量为正安全整数。

- `incomingRequests`：对端发起，本地执行。单结果请求与请求流都计入此类。
- `outgoingRequests`：本地发起，等待对端返回。`requestStream` 的结果流不会再占一次主动流额度。
- `incomingStreams`：对端主动推送的流，从有效 start 被接受到终止。
- `outgoingStreams`：本地主动推送的流，从生产任务创建到终止；即使还没发送首帧也占用额度。

请求分类的 `maxActivePerPeer` 默认各 `1024`，可用显式正整数替换；仅设置该类别的 `maxActive` 不会取消默认每连接限制。其他未填写的维度不设限。

容量耗尽时，本地发起任务在发送前失败；入站请求尝试回应 `CAPACITY_EXCEEDED`；入站主动流尝试发送取消。限额针对逻辑任务登记，取消、超时、断连和正常终结都会释放额度。

如果用户 handler 忽略 `AbortSignal`，逻辑任务释放后其底层工作仍可能继续运行。不能用活动任务数推断进程中所有实际异步工作的数量。

## 三类任务期限

| 参数 | 作用方 | 开始与结束 | 关闭方式 |
| --- | --- | --- | --- |
| `request(..., { timeoutMs })` | 发起方 | 从本地登记到单结果或请求流终结，包含排队 | `null`；不填为 `30000` |
| `taskTimeouts.incomingRequestMs` | 响应方 | 接收管线通过、请求被接受后开始；覆盖 handler、结果流及响应发送 | `null` 或不填 |
| `taskTimeouts.streamIdleMs` | 流生产方 / 消费方 | 生产方从任务创建开始；消费方从首个有效 start 开始；有效进展续期 | `null` 或不填 |

入站执行期限与流空闲期限必须为 `1..2147483647` 的整数。请求总期限接受正有限数、不大于 `2147483647`。它们是独立策略，不会因为打开其中一个就自动关闭另一个。

`stream(..., { idleTimeoutMs })` 可覆盖本次生产者的空闲期限；`requestStream(..., { idleTimeoutMs })` 可覆盖本次结果消费者的空闲期限。`undefined` 继承宿主，`null` 仅关闭本地此流的空闲期限，不改变对端设置，也不关闭请求总期限。

有效且成功处理的 start/chunk 会更新空闲期限；无关消息、被过滤的分片和非法帧不会续期。接收 start 的异步中间件也受首个空闲期限约束。发送与接收在关键阶段复查截止时间，避免仅依赖计时器回调而放行迟到数据。

期限触发会先结算本地任务、发出取消信号，再尽力通知远端。相互竞争的执行期限和流空闲期限只允许一个成为该任务的终止原因。JavaScript 同步长任务会阻塞事件循环，期限机制无法抢占这段代码。

## 清理责任

| 操作 | 释放什么 | 远端行为 |
| --- | --- | --- |
| 普通订阅 `cancel()` / 提前 `break` | 本订阅队列与监听 | 不取消任何远端任务 |
| 请求 `controller.abort()` / 请求流 `cancel()` | 本地请求登记、定时器与缓冲 | 已发送且协议支持时尽力发送 request_cancel |
| 主动流 `controller.abort()` / 接收侧提前结束 | 本地流登记与缓冲 | 协议支持时尽力通知对端结束相关流 |
| 连接关闭 / detach | 本连接任务、队列等待、计量预算 | 已断开的连接无法保证送达取消消息 |
| 宿主销毁 | 全部上述资源及持久注册 | 不自动关闭用户自己创建的其他资源 |

表中的 `controller` 是创建并持有传入 `signal` 的 `AbortController`；调用它的 `abort()` 发出取消信号。

业务处理器应向数据库、HTTP 调用或内部等待继续传递 `signal`，并用 `finally` 清理自己持有的资源。更多生命周期示例见[连接生命周期](/guide/lifecycle)，错误类型见[错误处理](/reference/errors)。
