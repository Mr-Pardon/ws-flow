---
description: 不使用 ws-flow Server，用通用客户端从第三方 JSON 消息流中筛选进度事件。
---

# 独立 Client 与事件筛选

已有服务持续发送心跳、日志和任务进度。你只希望把某个任务的进度交给一个独立消费者，不想在每次收到消息时手动分支。

本例的服务端直接使用 `ws`，发送自己的 JSON 对象。Client 保持默认 `generic` 模式，无需 ws-flow Envelope，也不需要 ws-flow Server 参与消息处理。

## 先运行

在仓库根目录执行：

```sh
pnpm run example:generic
```

预期输出：

```text
Report progress: 25%
Report progress: 100%
Only the matching events were consumed; subscription released.
```

## 关键用法：一份订阅，一条消费路径

以下 JavaScript 摘录自本页的 `examples/generic-client.mjs`，使用其中已创建的 `client`。先定义关心哪些消息，连接成功后发送命令，再读取筛选后的结果。

```js
const progress = client.subscribe(
  raw =>
    typeof raw === 'object' &&
    raw !== null &&
    raw.type === 'progress' &&
    raw.job === 'report-1',
  { signal: AbortSignal.timeout(5000) }
)
```

筛选规则写在订阅入口；`AbortSignal.timeout(5000)` 将本次订阅限制为 5 秒。建立连接后：

```js
await client.send({ action: 'watch', job: 'report-1' })

const values = []
for await (const message of progress) {
  values.push(message.raw.percent)
  console.log(`Report progress: ${message.raw.percent}%`)
  if (message.raw.percent === 100) break
}
```

消费处只处理报告进度。`values` 收集每次进度，页面也可以在这里直接更新进度条。`break` 自动结束这份订阅，不需要另外移除底层 WebSocket 监听。

### 哪些消息会进入这份订阅

| 服务发来的消息 | 本订阅是否接收 |
| --- | --- |
| `{ type: 'heartbeat', online: true }` | 否 |
| `{ type: 'progress', job: 'report-1', percent: 25 }` | 是 |
| `{ type: 'log', text: 'Still working' }` | 否 |
| `{ type: 'progress', job: 'report-1', percent: 100 }` | 是 |

`subscribe` 接受同步筛选函数。通过筛选的消息进入该订阅自己的队列，`for await` 依次取出消息；完成后 `break` 会关闭这份订阅。其他订阅可以独立匹配相同的消息，不会互相抢走结果。

默认 JSON codec 处理序列化与解析，`message.raw` 保留解码后的原始结构。Client 不会因为业务对象碰巧存在 `requestId` 或 `kind` 字段，就将其识别为 ws-flow 的任务控制消息。

## 在本地运行的完整实现

此文件可直接执行。仓库内相对导入指向构建后的公开入口；在自己的项目中改为 `@ws-flow/client/node`。浏览器项目使用 `@ws-flow/client/browser`，并由你的应用管理远端服务地址。

::: details 展开 examples/generic-client.mjs（含两端连接与清理）

<<< ../../../examples/generic-client.mjs

:::

## 可以如何扩展

- 同时关心日志：另建一个 `type === 'log'` 的订阅。
- 希望按名称使用 `on`：通过 `genericAdapter` 映射业务事件字段，见[协议与插件](/guide/extensions)。
- 服务使用纯文本或二进制：提供相应的 codec，避免沿用默认 JSON 编码规则。

本例的 `send` 仅负责发送 `watch` 命令。等待业务结果的可靠请求模式，需要双方协议支持请求身份和响应关联；默认 generic 模式不会替第三方服务发明这套约定。继续阅读[事件与订阅](/guide/events)或[请求与结果](/guide/requests)。
