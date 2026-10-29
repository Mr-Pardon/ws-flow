---
description: 在一条 WebSocket 连接上双向发起请求，并收集多个 Client 的状态结果。
---

# 双向请求与结果收集

WebSocket 两端都可以主动发送，业务请求也可以双向发起。Client 查询 Server 的计算结果，Server 则向已连接的 Client 查询状态；双方共用请求关联、超时和取消语义。

本例先完成一次单结果请求和一次请求流，再由 Server 使用 `requestAll` 查询 Client。两端通过同一套请求 API 完成任务关联和结果回收。

## 先运行

```sh
pnpm run example:duplex
```

也可以先构建，再执行 `node examples/duplex.mjs`。预期结果：

```text
Client requests Server: 42
Streaming response: [ 0, 1, 2 ]
Server requests Clients: [
  { session: '<本次连接 ID>', status: 'fulfilled', value: { ready: true } }
]
```

Session ID 每次连接都可能不同。

## 关键用法：两端都能提问，也都能回答

以下 JavaScript 摘录自本页的 `examples/duplex.mjs`。`server` 与 `client` 的创建、连接和清理放在完整源码中。

### Client 向 Server 要一个结果

Server 注册处理器，Client 用 `await` 取得返回值：

```js
server.handleRequest('double', ctx => ctx.data * 2)

console.log('Client requests Server:', await client.request('double', 21))
```

输出 `42`。不需要再注册一个“计算完成”事件，也不需要手动管理请求 ID。

### Server 向 Client 查询状态

换一个方向，仍是“注册处理器 → 发起请求”。这里向当前所有 Client 查询：

```js
client.handleRequest('status', () => ({ ready: true }))

const results = await server.all().requestAll('status', null)
```

`results` 保存每个连接的成功或失败结果。如果只查询某一个 Client，使用对应的 `session.request('status', null)`。

### 结果分多次返回时

完整案例还注册了 `handleStream('numbers', ...)`，依次产生 `0`、`1`、`2`。Client 只需改用迭代读取：

```js
const values = []
for await (const value of client.requestStream('numbers', 3)) {
  values.push(value)
}
console.log('Streaming response:', values)
```

### 三次通信的对应关系

| 发起方 | 调用 | 接收处理 | 结果形式 |
| --- | --- | --- | --- |
| Client | `request('double', 21)` | Server 的 `handleRequest` | 一个值 `42` |
| Client | `requestStream('numbers', 3)` | Server 的 `handleStream` | 顺序产生 `0`、`1`、`2` |
| Server | `all().requestAll('status', null)` | Client 的 `handleRequest` | 按 Session 收集结果 |

## 在本地运行的完整实现

::: details 展开 examples/duplex.mjs（含本地连接、完整处理器与清理）

<<< ../../../examples/duplex.mjs

:::

## 多个 Client 时如何选择目标

每个 Session 对应一条连接。只请求一个 Client 时调用 `session.request`；请求多个 Client 时先构造目标集合，再调用 `requestAll`。

`requestAll` 固定调用当时的目标及其顺序，每个目标独立成功或失败。新连接不加入已发出的任务，单个目标失败也不会让其他成功结果消失，空集合直接返回 `[]`。

成功项包含 `session`、`status: 'fulfilled'` 和 `value`；失败项包含 `session`、`status: 'rejected'` 和 `reason`。汇总时应按 `status` 区分，避免把连接异常误当成业务值。

房间和筛选条件可以缩小目标范围，见[服务端与会话](/guide/server)。多目标请求流的结果合并与排序需由业务自行安排，`requestAll` 收集每个目标的单个结果。

## 连接所有权

示例先创建原生 `WebSocketServer`，然后交给 `WSServer.takeover`。`server.close()` 清理 ws-flow 的绑定和会话，外部服务器仍由创建它的代码关闭。因此 `finally` 同时关闭 Client、ws-flow Server 和底层服务器，使示例可以正常退出。
