# 错误处理

日常请求使用普通的 `try/catch` 即可。需要区分超时、业务拒绝等原因时，再判断 `RequestError.code`；连接与插件的后台问题由 `onIssue` 记录。

```ts
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient({ protocol: 'ws-flow' })

try {
  await client.connect('ws://localhost:8080')
  const value = await client.request('double', 21, { timeoutMs: 5000 })
  console.log(value)
} catch (error) {
  console.error('请求未完成', error)
} finally {
  client.dispose()
}
```

`try/catch` 处理这次调用的失败，`onIssue` 观察连接、管线与后台执行问题；二者不能互相替代。上面的独立脚本在结束时关闭连接，页面应用可以把 `try/catch` 放进按钮等业务入口，并在卸载时统一清理 Client。

::: details 需要按请求错误码处理时

```ts
import { RequestError } from '@ws-flow/client/browser'

export function reportRequestError(error: unknown) {
  if (error instanceof RequestError && error.code === 'TIMEOUT') {
    console.error('请求超时，请稍后重试')
    return
  }

  console.error('请求失败', error)
}
```

在业务的 `catch` 中调用此函数即可。先判断类型，才能安全读取 `code`；并非所有连接或取消错误都是 `RequestError`。

:::

## 错误从哪里返回

| 操作 | 失败入口 |
| --- | --- |
| `connect / send / emit / request` | 返回 Promise 拒绝 |
| `stream` | 发送期间通过 Promise 拒绝；`Session.stream` 在创建阶段也可能同步抛错 |
| `requestStream` | 通常由迭代器读取拒绝；创建阶段也可能同步抛错 |
| `subscribe` | 参数错误同步抛出；筛选异常或缓冲溢出由读取拒绝 |
| `on` | handler 或订阅失败交给宿主 issue 观察器，随后结束订阅 |
| `handleRequest / handleStream` | 业务异常编码为远端请求错误；本地反馈发送失败进入 issue |
| `onStream` | handler 异常终止本地流，并进入 issue |
| `requestAll` | 每个目标的异常存入该项 `reason`，不采用遇错即整体失败策略 |

本地 `AbortSignal` 可以携带任意 `reason`；取消不保证一定抛出 `RequestError` 或 DOM `AbortError`。断连也可能以普通 `Error` 结算任务。请对 `unknown` 做类型判断，不要把所有失败都当作带 `code` 的对象。

## 请求错误码

`RequestError` 从 Client、Server 和 RuntimeCore 根入口导出，公开字段为 `code: string`、`message: string`。对端返回的请求错误会转换成此类型。

| code | 含义 | 排查方向 |
| --- | --- | --- |
| `TIMEOUT` | 本地主动请求的总期限到期 | 检查发送排队、处理器耗时；按业务需求调整 `timeoutMs` |
| `METHOD_NOT_FOUND` | 没有对应请求 handler | 核对事件名、注册时机与 Session 作用域 |
| `MODE_MISMATCH` | 单结果与流结果模式不匹配 | 对齐 `request/handleRequest` 与 `requestStream/handleStream` |
| `INVALID_STREAM` | 流处理器没有返回合法异步生成器 | 返回 `AsyncGenerator`，而不是普通数组或一般 Promise 结果 |
| `HANDLER_ERROR` | 业务或接收处理异常没有显式请求错误码 | 查看本地业务错误与返回的 message |
| `CAPACITY_EXCEEDED` | 对端活动任务额度不足 | 降低并发、等待任务释放，或调整容量 |
| `EXECUTION_TIMEOUT` | 对端入站请求执行期限到期 | 检查处理器及结果发送耗时 |
| `STREAM_IDLE_TIMEOUT` | 对端请求结果流空闲期限到期 | 检查生成速度与发送链路 |
| `CANCELLED` | 本地响应处理器或主动流生产者收到远端取消 | 检查 `signal.reason`；停止后续业务工作 |

`CANCELLED` 主要用于接收远端取消后的本地任务原因，不表示每次用户主动取消都会收到同名的远端响应。请求总期限、本地流空闲期限和远端执行期限彼此独立。

业务 handler 可抛出 `new RequestError('YOUR_CODE', '说明')`，保留业务错误码。普通 `Error` 会转换为 `HANDLER_ERROR`。线上的请求错误只包含 `code` 和 `message`，不自动传输堆栈或自定义属性；不要把密钥等敏感数据写进错误 message。

## 本地资源与能力错误

| 类型 / code | 发生场景 |
| --- | --- |
| `ProtocolCapabilityError`，没有 `code` 字段 | 当前 adapter 未实现事件发送、请求或流所需能力 |
| `SubscriptionOverflowError`，没有 `code` 字段 | 普通通知订阅的积压超过 `capacity` |
| `QueueCapacityError / QUEUE_CAPACITY_EXCEEDED` | 调度器等待队列超限 |
| `TaskCapacityError / CAPACITY_EXCEEDED` | 本地活动任务登记超限 |
| `TaskTimeoutError / EXECUTION_TIMEOUT` | 本地入站请求执行期限到期 |
| `TaskTimeoutError / STREAM_IDLE_TIMEOUT` | 本地流生产或消费空闲期限到期 |
| `TransportLimitError / MESSAGE_TOO_LARGE` | 单消息超过字节限额或发送高水位 |
| `TransportLimitError / PENDING_BYTES_EXCEEDED` | 全局或每连接的已计量字节超限 |
| `TransportLimitError / SEND_BUFFER_TIMEOUT` | 等待底层发送缓冲下降超时 |
| `TransportLimitError / INVALID_BUFFERED_AMOUNT` | 自定义 socket 未提供有效的 `bufferedAmount` |
| `TransportLimitError / UNSUPPORTED_WIRE_DATA` | 启用字节检查后，codec 产物不是支持计量的传输类型 |

`ProtocolCapabilityError`、`SubscriptionOverflowError`、`TaskCapacityError`、`TaskTimeoutError`、`TransportLimitError` 从 `@ws-flow/runtime-core` 导出；`QueueCapacityError` 从 `@ws-flow/scheduler` 导出。不要假定 Client 和 Server 会重新导出这些类。

流消费缓冲溢出使用普通 `Error`，没有独立错误码。通知订阅溢出和流缓冲溢出是不同错误，不能仅按一个“容量错误”类型处理所有情形。

同一个远端资源故障到达请求发起方时通常表现为 `RequestError`，并非远端那个本地错误类的实例。按 `code` 理解业务结果，按错误类区分本地组件来源。

## Server issue

```ts
import { WSServer } from '@ws-flow/server'

const server = new WSServer()

server.onIssue('server_error', issue => {
  console.error('WebSocket 服务异常', issue.error)
})

server.onIssue('resource_error', issue => {
  console.error({
    sessionId: issue.session.id,
    direction: issue.direction,
    taskKind: issue.taskKind,
    error: issue.error
  })
})
```

所有 issue 含 `type`、`message`、`timestamp`；Session 与附加数据随类型变化。

| type | 附加信息与来源 |
| --- | --- |
| `server_error` | `error: unknown`；底层 WebSocketServer 的监听或服务异常，不关联单个 Session |
| `decode_error` | `raw`、`error`；原始帧无法解码 |
| `process_error` | `session`、`error`、可选 `envelope`；接收、业务处理或连接回调失败 |
| `transport_error` | `session`、`envelope`、`error`；编码或实际发送失败 |
| `invalid_stream` | `session`、`envelope`、`reason`；主动流顺序无效 |
| `subscription_error` | `error`；Server 全局通知订阅的 handler 或消费失败 |
| `resource_error` | `session`、`direction`、`error`、可选 `taskKind`；队列、字节、活动任务或任务期限问题 |

`create` 和 `takeover` 都会观察底层服务器的错误。建议在创建或接管后立即注册 `server_error` 处理器，用于记录端口占用等服务问题；它不表示某个客户端请求失败。

`invalid_stream.reason` 可为 `missing_state / invalid_state / missing_start / duplicate_start`；部分结构不合法的帧会更早被协议校验拒绝，进入 `process_error`。`resource_error.taskKind` 表示任务归属，例如 `outgoingRequests`；此时 direction 按任务入站或出站归属标记，不代表最后失败帧在网络上的实际方向。

Client 的 `onIssue` 接收原始 `unknown` 错误，没有 Server 的分类包装。资源故障会尽量去重报告；不要依靠日志条数作为业务任务结算次数。

## 超限后的连接行为

普通出站通知超限只拒绝本次发送。入站队列、入站字节或任务帧发送资源失败可能破坏任务连续性，因此宿主关闭相关连接。Client 队列失败使用关闭码 `4008`，字节或水位失败使用 `4009`；Server 的关闭码不与这组 Client 自定义关闭码对应。

活动任务超限与执行/空闲超时优先结算相应任务，保持连接可用。发送错误反馈本身也可能遇到队列或传输失败，因此“尝试通知远端”不等于远端一定收到。更具体的参数和边界见[资源与期限](/reference/resources)。
