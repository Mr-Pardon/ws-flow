---
description: 发起一次流式请求，接收进度，提前退出并让远端工作协作停止。
---

# 可取消的进度任务

生成报告、批量导出或分析任务通常持续一段时间。Client 需要看到逐步进度，用户离开页面时还应停止不再需要的远端工作。

本例用 `requestStream` 发起报告任务，Server 的 `handleStream` 逐步产生进度。Client 收到 40% 时提前退出，远端通过 `ctx.signal` 停止等待，随后连接仍可继续处理其他请求。

## 先运行

```sh
pnpm run example:cancel
```

预期输出：

```text
Report progress: 20%
Report progress: 40%
Server work stopped; cancelled: true
The stream was cancelled; the connection still answers requests.
```

## 关键用法：像读数据一样接收进度

以下 JavaScript 摘录自本页的 `examples/cancellable-task.mjs`，使用其中已连接的 `client`：

```js
const values = []
for await (const progress of client.requestStream('report', { job: 'report-1' })) {
  values.push(progress.percent)
  console.log(`Report progress: ${progress.percent}%`)
  if (progress.percent >= 40) break
}
```

`requestStream` 发起任务，`for await` 逐次收到进度，`break` 表达“不再需要后续结果”。报告任务的关联、流的结束和取消通知由 ws-flow 处理。`values` 收集已收到的进度，结果为 `[20, 40]`。

Server 在 `handleStream('report', ...)` 中生成进度，把当前请求的 `ctx.signal` 传给支持取消的工作：

```js
await delay(100, undefined, { signal: ctx.signal })
yield { job: ctx.data.job, percent }
```

这两行位于完整源码的生成器循环内，`delay` 来自 `node:timers/promises`。实际业务可以在同一位置调用支持 signal 的 HTTP 或数据处理接口。

### 一次提前退出发生了什么

| 步骤 | Client | Server |
| --- | --- | --- |
| 发起 | `requestStream('report', ...)` | 接纳请求，调用生成器 |
| 推进 | 用 `for await` 接收进度 | 等待工作完成，再 `yield` |
| 提前退出 | 在 40% 时 `break`，结束本地消费并尝试发送取消 | 对应请求的 `ctx.signal` 被取消 |
| 清理 | 本地流任务结束 | 支持 signal 的等待被打断，执行 `finally` |
| 继续使用 | 再发送 `ping` 请求 | 正常回复 `pong` |

本地退出与远端实际清理存在时间差。界面可以在退出循环后结束加载状态；远端在收到取消并停止业务操作后释放资源。

## 在本地运行的完整实现

`node:timers/promises` 的定时等待支持 `AbortSignal`，用它模拟可取消的工作。实际业务可以把同一个 signal 传给 `fetch` 或其他支持取消的接口，并在批量处理循环中主动检查。

::: details 展开 examples/cancellable-task.mjs（含任务处理与资源清理）

<<< ../../../examples/cancellable-task.mjs

:::

## 应用中的处理方式

用户主动取消可通过退出迭代或传入的 `AbortController` 表达。处理器应把资源释放写在 `finally`，并把真实的业务异常继续抛出，让发起方得到失败结果。

::: warning 取消是双方协作
取消信号可以结束本库管理的任务等待，无法强制停止任意 Promise、同步 CPU 工作或不支持取消的第三方调用。连接中断时，远端也可能无法收到取消帧。请在业务中响应 signal，并根据实际资源决定清理方式。
:::

请求总期限默认为 30 秒，取消会在它之前结束任务。期限、断连和重连的规则见[连接与生命周期](/guide/lifecycle)，请求流与主动流的选择见[流式通信](/guide/streams)。
