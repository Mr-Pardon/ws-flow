# 快速开始

向服务端发出请求，像调用异步函数一样取得结果。ws-flow 负责请求标识和响应匹配，两端只需约定事件名、输入和返回值。

## 安装

在 Node.js 22 或更高版本的 ESM 项目中安装 Client 与 Server：

```sh
npm install @ws-flow/client@2 @ws-flow/server@1 ws@8
```

浏览器项目只需安装 `@ws-flow/client@2`，并使用 `/browser` 入口；浏览器原生 WebSocket 无需额外依赖。

## 第一次请求

两端约定一个 `double` 操作：传入数字，返回它的两倍。

**服务端：注册处理函数，直接返回结果。**

```ts [server.ts]
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleRequest<number, number>('double', ctx => ctx.data * 2)
```

**客户端：连接，然后 `await` 结果。**

```ts [client.ts]
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })
await client.connect('ws://localhost:8080')

const result = await client.request<number>('double', 21)
console.log(result) // 42
```

`handleRequest` 接收输入并返回结果，`request` 将对应结果交回调用处。处理函数也可以是 `async`，例如查询数据库后返回数据。类型参数描述双方约定，网络输入的校验见[请求与响应](/guide/requests)。

### 运行这两个文件

可以在已有的 TypeScript 项目中运行，也可以使用 `tsx`：

```sh
npm install --save-dev typescript tsx
npm pkg set type=module
```

在第一个终端启动服务端：

```sh
npx tsx server.ts
```

在另一个终端运行客户端，就会看到 `42`：

```sh
npx tsx client.ts
```

连接保持打开，可继续处理请求。应用退出或页面卸载时调用 `client.dispose()`，服务端停止时调用 `server.close()`。更多清理方式见[连接与生命周期](/guide/lifecycle)。

## 已有服务，只使用 Client

Client 也可以独立连接已有 WebSocket 服务，沿用对方的消息格式。假设服务接受行情订阅命令并持续返回 JSON 消息，将 URL 替换为实际地址：

```ts [browser-client.ts]
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient()
const messages = client.subscribe(() => true)

await client.connect('wss://your-service.example/socket')
await client.send({ action: 'subscribe', topic: 'prices' })

for await (const message of messages) {
  console.log(message.raw)
}
```

`subscribe(() => true)` 接收所有普通消息；替换筛选函数，就能把关心的事件单独分流。先注册订阅再发送命令，可以接住立即返回的消息。`message.raw` 是解码后的完整数据，默认 JSON 编解码，无需 Envelope。

此例持续接收，连接关闭或 `messages.cancel()` 后结束；不再使用客户端时调用 `client.dispose()`。只需消费一段消息时，也可以在循环中 `break`，它会结束本地订阅并保留连接。完整用法见[事件与订阅](/guide/events)。

默认模式提供普通收发与订阅。如果对方具备请求或流的协议约定，可以通过[协议适配](/guide/extensions)衔接相应能力。

## 环境入口

| 运行环境 | 导入路径 | 底层连接 |
| --- | --- | --- |
| 浏览器 | `@ws-flow/client/browser` | 浏览器原生 WebSocket |
| Node.js 客户端 | `@ws-flow/client/node` | `ws` |
| Node.js 服务端 | `@ws-flow/server` | `ws` |

浏览器入口不引入 Node 的 `ws`。支持条件导出的构建工具也可使用 `@ws-flow/client` 根入口。Client 可以独立安装，配套通信时再安装 Server；基础模块由包管理器自动解析。

## 运行完整案例

项目仓库提供可直接运行的双向请求、事件筛选与进度任务案例。使用 Node.js 22 或更新版本及 pnpm 10.11.0，在仓库根目录执行：

```sh
pnpm install --frozen-lockfile
pnpm run example:duplex
```

案例使用本机空闲端口创建两端，输出请求结果 `42`、连续结果 `[0, 1, 2]` 和客户端状态，然后关闭连接。代码与说明见[双向调用案例](/examples/duplex)。

## 接下来想做什么

| 需求 | 下一步 |
| --- | --- |
| 发通知，只处理关心的消息 | [事件与订阅](/guide/events) |
| 为请求增加超时、取消或业务错误 | [请求与响应](/guide/requests) |
| 持续读取任务进度、逐段结果 | [流式任务](/guide/streams) |
| 从 Server 主动调用客户端 | [Server 与连接组织](/guide/server) |
| 添加公共处理或适配已有消息格式 | [按需扩展](/guide/extensions) |
