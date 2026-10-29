# @ws-flow/client

[![npm](https://img.shields.io/npm/v/@ws-flow/client.svg)](https://www.npmjs.com/package/@ws-flow/client) [![npm version](https://img.shields.io/npm/v/ws-event-proxy.svg)](https://www.npmjs.com/package/ws-event-proxy) [![license](https://img.shields.io/npm/l/@ws-flow/client.svg)](https://www.npmjs.com/package/@ws-flow/client)

- [English](#english) | [简体中文](#简体中文)

A lightweight, event-driven WebSocket client framework.

一个轻量级、事件驱动的 WebSocket 客户端框架。

---

## English

A lightweight, event-driven WebSocket client framework.

Building on `ws-event-proxy` for event proxying and RPC, WSClient provides:

- WebSocket lifecycle management (auto-reconnect)
- Type-safe event system
- Multi-layer middleware model
- Streaming task abstraction (StreamTask)

It upgrades WebSocket from a low-level transport tool to a composable data-stream system.

---

### ✨ Features

- 🔌 WebSocket connection management (auto-reconnect)
- 🧩 Protocol-agnostic (based on `ws-event-proxy`)
- 🎯 Event-driven API (`on / once`)
- 🧠 Rule-based subscriptions (`subscribe`)
- 🧵 Streaming task system (`watch → StreamTask`)
- ⚙️ Complete middleware stack: 
  - global event middleware
  - route middleware (by event)
  - send middleware
  - stream middleware
- 🔄 Auto-recovery (restore watch tasks after reconnect)
- 🧼 Decoupled protocol layer and runtime
- 🧾 End-to-end type inference

---

### 🧠 Core Capabilities

| Capability | API |
|------|-----|
| Connection management | `connect / close` |
| Message sending | `send / request` |
| Event subscriptions | `on / once` |
| Rule subscriptions | `subscribe` |
| Streaming tasks | `watch` |
| Middleware stack | `use / useEvent / useSend / useStream` |

---

## 🏗️ Architecture Overview

```
                ┌────────────────────────────┐
                │        User Code           │
                │                            │
                │  on / send / watch / use   │
                └────────────┬───────────────┘
                             │
                             ▼
                ┌────────────────────────────┐
                │         WSClient           │
                │                            │
                │  - connection lifecycle    │
                │  - reconnect logic         │
                │  - task management         │
                └────────────┬───────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼

┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Event Pipeline  │   │ Send Pipeline   │   │ Stream Pipeline │
│                 │   │                 │   │                 │
│ use()           │   │ useSend()       │   │ useStream()     │
│ useEvent()      │   │                 │   │                 │
│ local middleware│   │ local middleware│   │ local middleware│
└───────┬─────────┘   └───────┬─────────┘   └───────┬─────────┘
        │                     │                     │
        ▼                     ▼                     ▼

                ┌────────────────────────────┐
                │       WSProxy Layer        │
                │      (ws-event-proxy)      │
                │                            │
                │  - RouteRule matching      │
                │  - RPC (request/response)  │
                └────────────┬───────────────┘
                             │
                             ▼
                ┌────────────────────────────┐
                │        WebSocket           │
                │   (Browser / Node ws)      │
                └────────────────────────────┘
```

---

### 🚀 Quick Start

```bash
npm i @ws-flow/client
```
If it is a `node` environment, you need to install `ws` package.

```bash
npm i ws
```

Instantiate WSClient

```ts
import { WSClient } from '@ws-flow/client'

type Events = {
  message: { type: 'message'; text: string }
  status: { type: 'status'; status: 'online' | 'offline' }
}

const client = new WSClient<Events>()
await client.connect('ws://localhost:3000')
```

#### 1. Send messages / requests

```ts
client.send({ type: 'ping' })

const res = await client.request({
  type: 'getStatus'
}) // { type: 'status', status: 'online' }
```

#### 2. Subscribe to events

```ts
client.on('status', (e) => {
  console.log(e.status)
})

client.subscribe({
  type: 'status',
  status: (value) => value === 'online'
}, (e) => {})
```

#### 3. Streaming tasks

```ts
const command = { type: 'start' } // task start command
const eventType = 'message' // event name or rule-based routing
const options = { signal: { type: 'end' } } // termination event

const task = client.watch(command, eventType, options)

for await (const e of task) {
  console.log(e.text)
}
```

---

### 🧩 Middleware System

Middleware is the core extension mechanism in WSClient.

#### 🧱 Four Layers

| Type | API | Purpose |
|------|-----|------|
| Event middleware | `use` | All inbound messages |
| Route middleware | `useEvent` | Match by event name |
| Send middleware | `useSend` | send / request |
| Stream middleware | `useStream` | watch |

#### Execution Order

- Event: global → route → local
- Send: global → local
- Stream: global → local

---

### 🔹 Middleware Conventions (Important)

#### Recommended (async)

```ts
const middleware = async (ctx, next) => {
  // pre-processing
  await next()
  // post-processing (similar to koa)
}
```

#### Recommended (sync / passthrough)

```ts
const middleware = (ctx, next) => {
  return next()
}
```

#### ❗ Avoid this (breaks the chain)

```ts
const middleware = async (ctx, next) => {
  next() // ❌ missing await / return
}
```

---

### 🔹 Event Middleware

```ts
// Global event middleware
const Interceptor = async (ctx, next) => {
  if (!ctx.data.author) ctx.stop()
  await next()
}
client.use([ Interceptor ])
```

---

### 🔹 Send Middleware

```ts
// Global send middleware
const Interceptor = async (ctx, next) => {
  if (!ctx.data.token) ctx.stop()
  await next()
}
client.useSend([ Interceptor ])
```

---

### 🔹 Route Middleware

```ts
// Route middleware
const Interceptor = async (ctx, next) => {
  await next()
}
// Supports String | RegExp | Predicate
client.useEvent('status', [ Interceptor ])
```

#### ⚠️ Important Limitation

`useEvent` depends on the **event name**

- `on()` ✅ auto-binds event
- `subscribe()` ❌ does not auto-bind

👉 If you use `subscribe`, you must:

```ts
const rule = client.withEvent(
  { type: 'status' },
  'status'
)

client.subscribe(rule, handler)
```

---

### 🔹 Stream Middleware

```ts
// Stream middleware
const Interceptor = (task, ctx, next) => {
  task.onData((e) => {})
  return next()
}
client.useStream([ Interceptor ])
```

---

### Protocol Configuration

`WSClient` accepts a framework-level `Protocol`:

```ts
type WSProtocol = {
  reconnectTimeout: number
  resolveEventType(name: string): Record<string, any>
} & Partial<ProxyProtocol>
```

#### Framework Options

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `reconnectTimeout` | `number` | Yes | Delay before reconnecting after unexpected disconnect (ms). |
| `resolveEventType` | `(name: string) => Record<string, any>` | Yes | Converts event names like `'message'` into a `RouteRule`, e.g. `{ type: 'message' }`. |

#### Proxy Options (from `ws-event-proxy`)

These options come from `ws-event-proxy` and are configured at the same level as
`reconnectTimeout` and `resolveEventType`. WSClient will split and pass them to
`ws-event-proxy` internally.

| Field | Type | Description |
| --- | --- | --- |
| `needReady` | `boolean` | If `true`, send operations wait until the proxy layer receives a ready event. |
| `logSend` | `boolean` | Enable `ws-event-proxy` send logging. |
| `logReceive` | `boolean` | Enable `ws-event-proxy` receive logging. |
| `buildRequest` | `(payload, ctx) => any` | Wrap/transform payload before send, typically for request IDs or envelopes. `ctx.getCBIndex()` can generate request IDs. |
| `getRequestId` | `(request) => string \| number \| null` | Extract request ID from outbound request, required for RPC. |
| `getResponseId` | `(response) => string \| number \| null` | Extract response ID from inbound response, required for RPC. |
| `isReadyEvent` | `(event) => boolean` | Detect ready event; typically used with `needReady`. |

#### Default Protocol

```ts
const WSProtocol = {
  reconnectTimeout: 2000,
  resolveEventType(name) {
    return { type: name }
  },
  ...BaseProtocol
}
```

This means:

- Auto-reconnect is enabled by default
- String events like `'chat:message'` match `{ type: 'chat:message' }`
- Without RPC-related config, `request()` will not work

---

### API Reference

#### `new WSClient(protocol?)`

Create a new client instance.

```ts
const client = new WSClient()
const typedClient = new WSClient<Events>(protocol)
```

Provide a custom protocol when you need `RPC` or non-default event mapping.

---

#### `WSClient.takeover(ws, protocol?)`

Take over an existing `WebSocket` instance and wrap it as `WSClient`.

```ts
const ws = new WebSocket('wss://example.com/ws')
await new Promise(resolve => ws.addEventListener('open', resolve, { once: true }))

const client = WSClient.takeover<Events>(ws, protocol)
```

Useful when connection creation is outside the framework. In takeover mode, URL is not stored, so external code manages reconnect rather than internal auto-reconnect.

---

#### `client.connect(url)`

Connect to the server. If already connecting or connected, returns the same Promise.

```ts
await client.connect('wss://example.com/ws')
```

---

#### `client.close()`

Close the connection and mark it as a manual close.

```ts
client.close()
```

Auto-reconnect is disabled until `connect()` is called again.

---

#### `client.use(middleware | middleware[])`

Register global inbound event middleware.

```ts
client.use(async (ctx, next) => {
  if (typeof ctx.data === 'object' && ctx.data) {
    ctx.meta = { ...ctx.meta, receivedAt: Date.now() }
  }
  await next()
})
```

You can also stop propagation to the final handler:

```ts
client.use(async (ctx) => {
  if (!ctx.data) {
    ctx.stop?.()
  }
})
```

The ctx object includes:

| Field | Type | Description |
| --- | --- | --- |
| `data` | `any` | Event payload |
| `raw` | `any` | Raw payload |
| `event` | `RouteRule` | Event matching rule |
| `proxy` | `WSProxy` | Proxy instance |
| `ws` | `WebSocket` | WebSocket instance |
| `state` | `WSState` | WSClient state |
| `ctx` | `WSContext` | Current context |
| `meta` | `Record<string, any>` | Custom metadata |
| `stop` | `() => void` | Stop propagation |

---

#### `client.useEvent(matcher, middleware | middleware[])`

Register middleware by event-name matcher.

```ts
client.useEvent('message', async (ctx, next) => {
  console.log('message event received')
  await next()
})

client.useEvent(/^chat:/, async (ctx, next) => {
  await next()
})
```

Supported matchers:

- Exact string
- `RegExp`
- Predicate `(eventName) => boolean`

Note:
This middleware matches by event name. `on()` automatically maps via `resolveEventType` and `withEvent()`.
`subscribe()` does not, so call `withEvent()` if you want `useEvent()` to apply.

---

#### `client.useSend(middleware | middleware[])`

Register global send middleware for both `send()` and `request()`.

```ts
client.useSend(async (ctx, next) => {
  ctx.data = {
    ...ctx.data,
    token: 'your-auth-token'
  }
  await next()
})
```

You can also block a send:

```ts
client.useSend(async (ctx) => {
  if (ctx.state !== 'OPEN') {
    ctx.stop?.()
  }
})
```

The ctx object includes:

| Field | Type | Description |
| --- | --- | --- |
| `data` | `any` | Outbound payload |
| `raw` | `any` | Raw payload |
| `proxy` | `WSProxy` | Proxy instance |
| `ws` | `WebSocket` | WebSocket instance |
| `state` | `WSState` | WSClient state |
| `ctx` | `WSContext` | Current context |
| `meta` | `Record<string, any>` | Custom metadata |
| `stop` | `() => void` | Stop propagation |

---

#### `client.useStream(middleware | middleware[])`

Register global stream middleware, executed when `watch()` creates a `StreamTask`.

```ts
client.useStream((task, ctx, next) => {
  task.onStop(() => {
    console.log('watch stopped', ctx.command)
  })
  return next(task)
})
```

Great place to wrap or enhance `StreamTask`, or even replace it.

The ctx object includes:

| Field | Type | Description |
| --- | --- | --- |
| `command` | `any` | Task command |
| `eventOrRule` | `string \| RouteRule` | Subscription rule |
| `options` | `WSWatchOptions` | Watch options |
| `wsContext` | `WSContext` | Current context |

---

#### `client.send(message, options?)`

Send a one-way message. It waits for WebSocket to be connected before sending.

```ts
await client.send({ type: 'ping' })
```

With local send middleware:

```ts
const middleware = async (ctx, next) => {
  ctx.data = { ...ctx.data, ts: Date.now() }
  await next()
}
```

```ts
const msg = { type: 'ping' }
const options = { sendMiddleware: [ middleware ] }

await client.send(msg, options)
```

Only local send middleware:

```ts
const msg = { type: 'ping' }
const options = {
  overrideSendMiddleware: true,
  sendMiddleware: [ middleware ]
}

await client.send(msg, options)
```

---

#### `client.request(message, options?)`

Send a request and await a response.

```ts
const protocol = {
  getRequestId: (msg) => msg.id,
  getResponseId: (msg) => msg.replyTo
}
const client = new WSClient(protocol)
```

```ts
type Msg = { type: 'getUser'; id: number }
type Reply = { replyTo: number; user: { id: number; name: string } }

const res = await client.request<Msg, Reply>(
  { type: 'getUser', id: 1 }
)
```

Local send middleware works the same as in `send()`.

Note: if your protocol cannot extract a request ID, `request()` will throw.

---

#### `client.withEvent(rule, eventName)`

Bind a `RouteRule` to an event name.

```ts
const rule = { type: 'message', roomId: 1 }
const ruleWithEvent = client.withEvent(rule, 'message')
```

This is especially important when:

- You pass rules directly to `subscribe()` or `subscribeEvent()`
- You want event-name middleware like `useEvent('message', ...)` or `useEvent(/^chat:/, ...)` to apply

> Because subscribe() can match multiple rules, call `withEvent()` to explicitly declare the event name (letting the framework guess is risky).

---

#### `client.on(eventName | eventName[], handler, options?)`

Subscribe by event name. Internally it converts using `protocol.resolveEventType`.

```ts
const protocol = {
  resolveEventType: (eventName) => {
    return { eventType: eventName }
  }
}
const client = new WSClient(protocol)
```

```ts
// Subscribe to { eventType: 'message' }
client.on('message', (data) => {
  console.log(data.text)
})
```

Subscribe to multiple events:

```ts
client.on(['message', 'done'], (data) => {
  console.log(data)
})
```

With local event middleware:

```ts
const middleware = async (ctx, next) => {
  ctx.meta = { ...ctx.meta, local: true }
  await next()
}
```

```ts
const options = {
  eventMiddleware: [ middleware ]
}

client.on('message', (data) => {
  console.log(data.text)
}, options)
```

Only local event middleware:

```ts
const options = {
  overrideEventMiddleware: true,
  eventMiddleware: [ middleware ]
}

client.on('message', (data) => {
  console.log(data.text)
}, options)
```

---

#### `client.once(eventName | eventName[], handler, options?)`

Like `on()`, but auto-unsubscribes after the first match and resolves the first event as a Promise.

```ts
const firstMessage = await client.once('message', (data) => {
  console.log('received first message', data.text)
})
```

---

#### `client.subscribe(rule, handler, options?)`

Subscribe directly with a `RouteRule` (for multi-rule matching).

```ts
const rule = { type: 'message', roomId: 1 }
const handler = (data) => {
  console.log(data.text)
}

const unsubscribe = client.subscribe(rule, handler)
// unsubscribe() cancel
```

Predicate rules:

```ts
const rule = {
  type: 'message',
  text: (value, event) => value.startsWith('[system]')
}

const unsubscribe = client.subscribe(rule, handler)
```

If you want `useEvent()` to apply, bind the event name first:

```ts
const rule = client.withEvent(
  { type: 'message', roomId: 1 },
  'message'
)

client.subscribe(rule, handler)
```

Local middleware works the same as in `on()`.

---

#### `client.watch(command, eventOrRule, options?)`

Create a `StreamTask`, immediately start subscribing, then send the command and return the task. After WS reconnect, the task is restored automatically and you do not need to call watch again.

With event name:

```ts
const command = { type: 'startMessages' }
const eventName = 'message'

const task = client.watch(command, eventName)

for await (const data of task) {
  console.log(data.text)
}
```

With route rule:

```ts
const command = { type: 'startMessages' }
const rule = { type: 'message', roomId: 1 }

const task = client.watch(command, rule)
```

Queue limit and built-in strategy:

```ts
const options = {
  maxQueueSize: 50,
  strategy: 'drop-head'
}

const task = client.watch(command, eventName, options)
```

Custom strategy:

```ts
const strategy = ({ queue, incoming }) => {
  return [...queue, incoming]
}
const options = {
  maxQueueSize: 50,
  strategy
}

const task = client.watch(command, eventName, options)
```

Local stream middleware for a single watch:

```ts
const middleware = async (task, ctx, next) => {
  task.onStop(() => console.log('task stopped'))
  return next(task)
}
const options = {
  streamMiddleware: [ middleware ]
}

const task = client.watch(command, eventName, options)
```

Stop signal:

```ts
const options = {
  signal: { type: 'done' }
}

const task = client.watch(command, eventName, options)
```

Notes for `watch`:

- The task subscribes first, then sends the command
- After reconnect, the task restarts and re-sends the original command
- If `eventOrRule` is a string, matching still depends on `resolveEventType`
- If `eventOrRule` is a rule and you want `useEvent()` to apply, call `withEvent()` first

---

#### `client.getWsInstance()`

Return the current underlying `WebSocket` instance; `null` if not connected.

```ts
const ws = client.getWsInstance()
```

Mainly for debugging or integrating with legacy APIs that still need direct socket access.

---

#### `client.getWsProxyInstance()`

Return the internal `WSProxy` instance.

```ts
const proxy = client.getWsProxyInstance()
```

Useful when you need lower-level features from `ws-event-proxy` that are not exposed by `WSClient`.

---

#### StreamTask

`watch()` returns a `StreamTask`. Calling `watch()` automatically runs `start()`; no need to start twice.

##### `task.start()`

Start subscribing if the task is not running.

```ts
task.start()
```

##### `task.stop()`

Stop subscribing, clear the buffer, mark pending async iterators as ended, and trigger stop handlers.

```ts
task.stop()
```

##### `task.restart()`

Reset internal state and restart.

```ts
task.restart()
```

##### `task.onData(handler)`

Register a callback for each inbound item.

```ts
const off = task.onData((data) => {
  console.log(data)
})
// off() removes handler
```

##### `task.onStop(handler)`

Register a callback when the task stops.

```ts
task.onStop(() => {
  console.log('stopped')
})
```

##### `task.next()`

Wait for the next item.

```ts
const item = await task.next()
```

If the task has ended, `next()` rejects with `Error('StreamTask ended')`.

##### `for await (const item of task)`

Consume the task as an async iterable.

```ts
for await (const item of task) {
  console.log(item)
}
```

---

### 🌐 Environment

- Browser: native WebSocket
- Node.js: auto-uses `ws`

---

### 📄 License

MIT

---

## 简体中文

一个轻量级、事件驱动的 WebSocket 客户端框架。

在保留 `ws-event-proxy` 的事件代理与 RPC 能力的基础上，WSClient 提供了：

- WebSocket 生命周期管理（自动重连）
- 类型安全的事件系统
- 多层中间件机制
- 流式任务抽象（StreamTask）

让 WebSocket 从“底层通信工具”升级为“可组合的数据流系统”。

---

### ✨ 特性

- 🔌 WebSocket 连接管理（自动重连）
- 🧩 协议无关（基于 `ws-event-proxy`）
- 🎯 事件驱动 API（`on / once`）
- 🧠 基于规则的订阅（`subscribe`）
- 🧵 流式任务系统（`watch → StreamTask`）
- ⚙️ 完整的中间件体系：
  - 全局事件中间件
  - 路由中间件（基于 event）
  - 发送中间件
  - 流任务中间件
- 🔄 自动恢复（重连后恢复 watch 任务）
- 🧼 协议层与运行时解耦
- 🧾 全链路类型推导

---

### 🧠 核心能力一览

| 能力 | API |
|------|-----|
| 连接管理 | `connect / close` |
| 消息发送 | `send / request` |
| 事件订阅 | `on / once` |
| 规则订阅 | `subscribe` |
| 流式任务 | `watch` |
| 中间件体系 | `use / useEvent / useSend / useStream` |

---

## 🏗️ 架构总览

```
                ┌────────────────────────────┐
                │        User Code           │
                │                            │
                │  on / send / watch / use   │
                └────────────┬───────────────┘
                             │
                             ▼
                ┌────────────────────────────┐
                │         WSClient           │
                │                            │
                │  - connection lifecycle    │
                │  - reconnect logic         │
                │  - task management         │
                └────────────┬───────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼

┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Event Pipeline  │   │ Send Pipeline   │   │ Stream Pipeline │
│                 │   │                 │   │                 │
│ use()           │   │ useSend()       │   │ useStream()     │
│ useEvent()      │   │                 │   │                 │
│ local middleware│   │ local middleware│   │ local middleware│
└───────┬─────────┘   └───────┬─────────┘   └───────┬─────────┘
        │                     │                     │
        ▼                     ▼                     ▼

                ┌────────────────────────────┐
                │       WSProxy Layer        │
                │      (ws-event-proxy)      │
                │                            │
                │  - RouteRule matching      │
                │  - RPC (request/response)  │
                └────────────┬───────────────┘
                             │
                             ▼
                ┌────────────────────────────┐
                │        WebSocket           │
                │   (Browser / Node ws)      │
                └────────────────────────────┘
```

---

### 🚀 快速开始

```bash
npm i @ws-flow/client
```

如果是node环境，请安装`ws`

```bash
npm i ws
```

实例化WSClient

```ts
import { WSClient } from '@ws-flow/client'

type Events = {
  message: { type: 'message'; text: string }
  status: { type: 'status'; status: 'online' | 'offline' }
}

const client = new WSClient<Events>()
await client.connect('ws://localhost:3000')
```

#### 1. 发送消息/请求

```ts
client.send({ type: 'ping' })

const res = await client.request({
  type: 'getStatus'
}) // { type: 'status', status: 'online' }
```

#### 2. 订阅事件

```ts
client.on('status', (e) => {
  console.log(e.status)
})

client.subscribe({
  type: 'status',
  status: (value) => value === 'online'
}, (e) => {})
```

#### 3. 流式任务

```ts
const command = { type: 'start' } // 启动任务命令
const eventType = 'message' // 订阅的事件，也可使用规则路由
const options = { signal: { type: 'end' } } // 结束事件

const task = client.watch(command, eventType, options)

for await (const e of task) {
  console.log(e.text)
}
```

---

### 🧩 中间件体系

中间件是 WSClient 的核心扩展机制。

#### 🧱 四层中间件

| 类型 | API | 作用 |
|------|-----|------|
| 事件中间件 | `use` | 所有入站消息 |
| 路由中间件 | `useEvent` | 按事件名匹配 |
| 发送中间件 | `useSend` | send / request |
| 流中间件 | `useStream` | watch |

#### 执行顺序

- 事件：global → route → local
- 发送：global → local
- 流：global → local

---

### 🔹 中间件执行规范（重要）

#### 推荐写法（异步）

```ts
const middleware = async (ctx, next) => {
  // 前置处理
  await next()
  // 后置处理（类似 koa）
}
```

#### 推荐写法（同步/透传）

```ts
const middleware = (ctx, next) => {
  return next()
}
```

#### ❗ 不要这样写（会吞掉链路）

```ts
const middleware = async (ctx, next) => {
  next() // ❌ 没有 await / return
}
```

---

### 🔹 事件中间件

```ts
// 全局事件中间件
const Interceptor = async (ctx, next) => {
  // 可处理所有接收的消息
  if (!ctx.data.author) ctx.stop()
  await next()
}
client.use([ Interceptor ])
```

---

### 🔹 发送中间件

```ts
// 全局发送中间件
const Interceptor = async (ctx, next) => {
  if (!ctx.data.token) ctx.stop()
  await next()
}
client.useSend([ Interceptor ])
```

---

### 🔹 路由中间件

```ts
// 路由中间件
const Interceptor = async (ctx, next) => {
  await next()
}
// 支持 String | RegExp | Predicate
client.useEvent('status', [ Interceptor ])
```

#### ⚠️ 重要限制

`useEvent` 依赖 **event 名**

- `on()` ✅ 自动绑定 event
- `subscribe()` ❌ 不会自动绑定

👉 如果你使用 `subscribe`，必须：

```ts
const rule = client.withEvent(
  { type: 'status' },
  'status'
)

client.subscribe(rule, handler)
```

---

### 🔹 流式任务中间件

```ts
// 流式任务中间件
const Interceptor = (task, ctx, next) => {
  // 可对 task 进行处理
  task.onData((e) => {})
  return next()
}
client.useStream([ Interceptor ])
```

---

### 协议配置

`WSClient` 接收的是一层框架级的 `Protocol`：

```ts
type WSProtocol = {
  reconnectTimeout: number
  resolveEventType(name: string): Record<string, any>
} & Partial<ProxyProtocol>
```

#### 框架层配置项

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `reconnectTimeout` | `number` | 是 | 非预期断开后，延迟多久再发起重连，单位毫秒。 |
| `resolveEventType` | `(name: string) => Record<string, any>` | 是 | 把 `'message'` 这样的事件名转换成真正用于匹配的 `RouteRule`，例如 `{ type: 'message' }`。 |

#### 代理协议配置项（来自 `ws-event-proxy`）

这些配置项来自 `ws-event-proxy`，与 `reconnectTimeout`、`resolveEventType` 处于同一层级。
WSClient 会在内部自动拆分并传给 `ws-event-proxy`。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `needReady` | `boolean` | 若为 `true`，则发送行为会等到代理层收到 ready 事件后才真正发出。 |
| `logSend` | `boolean` | 是否启用 `ws-event-proxy` 底层发送日志。 |
| `logReceive` | `boolean` | 是否启用 `ws-event-proxy` 底层接收日志。 |
| `buildRequest` | `(payload, ctx) => any` | 发送前对消息进行包装或转换，常用于注入请求 ID 或外层 envelope。ctx 提供 `getCBIndex()` 可用于生成请求 ID。 |
| `getRequestId` | `(request) => string \| number \| null` | 从出站请求中提取请求 ID，RPC 模式必需。 |
| `getResponseId` | `(response) => string \| number \| null` | 从入站响应中提取响应 ID，RPC 模式必需。 |
| `isReadyEvent` | `(event) => boolean` | 判断某个入站事件是否表示 ready，通常与 `needReady` 配套使用。 |

#### 默认协议

内置的 `WSProtocol` 行为如下：

```ts
const WSProtocol = {
  reconnectTimeout: 2000,
  resolveEventType(name) {
    return { type: name }
  },
  ...BaseProtocol
}
```

这意味着：

- 默认开启自动重连
- 字符串事件如 `'chat:message'` 会匹配 `{ type: 'chat:message' }`
- 如果不额外提供 RPC 相关配置，`request()` 无法正常工作

---

### API 参考

#### `new WSClient(protocol?)`

创建一个新的客户端实例。

```ts
const client = new WSClient()
const typedClient = new WSClient<Events>(protocol)
```

当你需要 `RPC` 能力，或者事件映射不是默认 `{ type: eventName }` 时，应该传入自定义协议。

---

#### `WSClient.takeover(ws, protocol?)`

接管一个已经创建好的 `WebSocket` 实例，并包装成 `WSClient`。

```ts
const ws = new WebSocket('wss://example.com/ws')
await new Promise(resolve => ws.addEventListener('open', resolve, { once: true }))

const client = WSClient.takeover<Events>(ws, protocol)
```

适合连接创建权不在框架内部的场景。由于 takeover 模式不会记录 URL，因此它更适合外部自己管理连接，而不是依赖内部自动重连流程。

---

#### `client.connect(url)`

连接到服务端。如果当前已经在连接中或已经连接完成，会直接返回同一个连接 Promise。

```ts
await client.connect('wss://example.com/ws')
```

---

#### `client.close()`

关闭当前连接，并把这次关闭标记为手动关闭。

```ts
client.close()
```

调用后不会自动重连，除非再次执行 `connect()`。

---

#### `client.use(middleware | middleware[])`

注册全局入站事件中间件。

```ts
client.use(async (ctx, next) => {
  if (typeof ctx.data === 'object' && ctx.data) {
    ctx.meta = { ...ctx.meta, receivedAt: Date.now() }
  }
  await next()
})
```

也可以阻止事件继续传给最终 handler：

```ts
client.use(async (ctx) => {
  if (!ctx.data) {
    ctx.stop?.()
  }
})
```

ctx 对象包含以下属性：

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `data` | `any` | 事件数据 |
| `raw` | `any` | 原始数据 |
| `event` | `RouteRule` | 事件匹配规则 |
| `proxy` | `WSProxy` | 代理实例 |
| `ws` | `WebSocket` | WebSocket 实例 |
| `state` | `WSState` | WSClient 状态 |
| `ctx` | `WSContext` | 当前上下文对象 |
| `meta` | `Record<string, any>` | 自定义元数据 |
| `stop` | `() => void` | 阻止事件继续传递 |

---

#### `client.useEvent(matcher, middleware | middleware[])`

注册按事件名匹配的中间件。

```ts
client.useEvent('message', async (ctx, next) => {
  console.log('message event received')
  await next()
})

client.useEvent(/^chat:/, async (ctx, next) => {
  await next()
})
```

支持的 matcher 包括：

- 精确字符串
- `RegExp`
- 谓词函数 `(eventName) => boolean`

注意：
这层中间件匹配的是“事件名”。`on()` 会自动通过 `resolveEventType` 和 `withEvent()` 建立映射。
`subscribe()` 不会自动做这件事，所以如果你希望 `useEvent()` 生效，需要手动先调用 `withEvent()`。

---

#### `client.useSend(middleware | middleware[])`

注册全局发送中间件，对 `send()` 和 `request()` 都会生效。

```ts
client.useSend(async (ctx, next) => {
  ctx.data = {
    ...ctx.data,
    token: 'your-auth-token'
  }
  await next()
})
```

也可以直接阻止一次发送：

```ts
client.useSend(async (ctx) => {
  if (ctx.state !== 'OPEN') {
    ctx.stop?.()
  }
})
```

ctx 对象包含以下属性：

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `data` | `any` | 发送的数据 |
| `raw` | `any` | 原始数据 |
| `proxy` | `WSProxy` | 代理实例 |
| `ws` | `WebSocket` | WebSocket 实例 |
| `state` | `WSState` | WSClient 状态 |
| `ctx` | `WSContext` | 当前上下文对象 |
| `meta` | `Record<string, any>` | 自定义元数据 |
| `stop` | `() => void` | 阻止事件继续传递 |

---

#### `client.useStream(middleware | middleware[])`

注册全局流中间件，在 `watch()` 创建 `StreamTask` 时执行。

```ts
client.useStream((task, ctx, next) => {
  task.onStop(() => {
    console.log('watch stopped', ctx.command)
  })
  return next(task)
})
```

适合在这里对 `StreamTask` 做包装、增强，甚至直接替换。

ctx 对象包含以下属性：

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `command` | `any` | task 任务命令 |
| `eventOrRule` | `string \| RouteRule` | 流任务订阅规则 |
| `options` | `WSWatchOptions` | 流任务配置项 |
| `wsContext` | `WSContext` | 当前上下文对象 |

---

#### `client.send(message, options?)`

发送一条单向消息。它会自动先等待 WebSocket 连接，再真正发送。

```ts
await client.send({ type: 'ping' })
```

配合局部发送中间件：

```ts
const middleware = async (ctx, next) => {
  ctx.data = { ...ctx.data, ts: Date.now() }
  await next()
}
```

```ts
const msg = { type: 'ping' }
const options = { sendMiddleware: [ middleware ] }

await client.send(msg, options)
```

仅使用局部发送中间件：

```ts
const msg = { type: 'ping' }
const options = {
  overrideSendMiddleware: true,
  sendMiddleware: [ middleware ]
}

await client.send(msg, options)
```

---

#### `client.request(message, options?)`

发送请求并等待响应。

```ts
const protocol = {
  getRequestId: (msg) => msg.id,
  getResponseId: (msg) => msg.replyTo
}
const client = new WSClient(protocol)
```

```ts
type Msg = { type: 'getUser'; id: number }
type Reply = { replyTo: number; user: { id: number; name: string } }

const res = await client.request<Msg, Reply>(
  { type: 'getUser', id: 1 }
)
```

它同样支持和 `send()` 一样的局部发送中间件写法。

注意：如果你的协议无法从出站消息中提取请求 ID，`request()` 会直接抛错。

---

#### `client.withEvent(rule, eventName)`

为一个 `RouteRule` 绑定对应的事件名。

```ts
const rule = { type: 'message', roomId: 1 }
const ruleWithEvent = client.withEvent(rule, 'message')
```

它在这些场景里尤其重要：

- 你通过 `subscribe()` 或 `subscribeEvent()` 直接传入匹配规则 rule
- 你希望 `useEvent('message', ...)` 或 `useEvent(/^chat:/, ...)` 这类按事件名的中间件生效

> 因为 subscribe() 需匹配多条规则，因此需要手动调用 `withEvent()` 来显性声明这项规则所对应的事件（让框架自动去“猜测”是十分危险的做法）

---

#### `client.on(eventName | eventName[], handler, options?)`

基于事件名订阅，内部会通过 `protocol.resolveEventType` 转成规则。

```ts
const protocol = {
  resolveEventType: (eventName) => {
    return { eventType: eventName }
  }
}
const client = new WSClient(protocol)
```

```ts
// 订阅 { eventType: 'message' } 事件
client.on('message', (data) => {
  console.log(data.text)
})
```

同时订阅多个事件名：

```ts
client.on(['message', 'done'], (data) => {
  console.log(data)
})
```

使用局部事件中间件：

```ts
const middleware = async (ctx, next) => {
  ctx.meta = { ...ctx.meta, local: true }
  await next()
}
```

```ts
const options = {
  eventMiddleware: [ middleware ]
}

client.on('message', (data) => {
  console.log(data.text)
}, options)
```

只使用局部事件中间件：

```ts
const options = {
  overrideEventMiddleware: true,
  eventMiddleware: [ middleware ]
}

client.on('message', (data) => {
  console.log(data.text)
}, options)
```

---

#### `client.once(eventName | eventName[], handler, options?)`

和 `on()` 类似，但首次命中后会自动取消订阅，并返回首个匹配数据的 Promise。

```ts
const firstMessage = await client.once('message', (data) => {
  console.log('received first message', data.text)
})
```

---

#### `client.subscribe(rule, handler, options?)`

直接通过 `RouteRule` 订阅，用于匹配多条规则的事件。

```ts
const rule = { type: 'message', roomId: 1 }
const handler = (data) => {
  console.log(data.text)
}

const unsubscribe = client.subscribe(rule, handler)
// unsubscribe() 取消订阅
```

使用谓词规则：

```ts
const rule = {
  type: 'message',
  text: (value, event) => value.startsWith('[system]')
}

const unsubscribe = client.subscribe(rule, handler)
```

如果想让 `useEvent()` 一起生效，需要先绑定事件名：

```ts
const rule = client.withEvent(
  { type: 'message', roomId: 1 },
  'message'
)

client.subscribe(rule, handler)
```

它同样支持和 `on()` 一样的局部中间件写法。

---

#### `client.watch(command, eventOrRule, options?)`

创建一个 `StreamTask`，立即启动订阅，然后发送命令，最后返回任务对象。WS 重连后会自动恢复任务，不需要重新调用 watch。

使用字符串事件名：

```ts
const command = { type: 'startMessages' }
const eventName = 'message'

const task = client.watch(command, eventName)

for await (const data of task) {
  console.log(data.text)
}
```

使用 route rule：

```ts
const command = { type: 'startMessages' }
const rule = { type: 'message', roomId: 1 }

const task = client.watch(command, rule)
```

设置队列上限和内置策略：

```ts
const options = {
  maxQueueSize: 50,
  strategy: 'drop-head'
}

const task = client.watch(command, eventName, options)
```

使用自定义策略：

```ts
const strategy = ({ queue, incoming }) => {
  return [...queue, incoming]
}
const options = {
  maxQueueSize: 50,
  strategy
}

const task = client.watch(command, eventName, options)
```

为单次 watch 增加流中间件：

```ts
const middleware = async (task, ctx, next) => {
  task.onStop(() => console.log('task stopped'))
  return next(task)
}
const options = {
  streamMiddleware: [ middleware ]
}

const task = client.watch(command, eventName, options)
```

使用停止信号：

```ts
const options = {
  signal: { type: 'done' }
}

const task = client.watch(command, eventName, options)
```

关于 `watch`，有几点值得特别注意：

- 任务会先开始订阅，再发送命令
- 重连后任务会自动重启，并重新发送原始命令
- 当 `eventOrRule` 是字符串时，匹配仍然依赖 `resolveEventType`
- 当 `eventOrRule` 是 rule，且你希望 `useEvent()` 生效时，需要先调用 `withEvent()`

---

#### `client.getWsInstance()`

返回当前底层 `WebSocket` 实例；若当前未连接，则返回 `null`。

```ts
const ws = client.getWsInstance()
```

主要用于调试，或和仍然需要直接操作 socket 的旧接口做集成。

---

#### `client.getWsProxyInstance()`

返回内部使用的 `WSProxy` 实例。

```ts
const proxy = client.getWsProxyInstance()
```

当你需要使用 `ws-event-proxy` 的更底层能力，而这些能力没有被 `WSClient` 二次封装时，这个方法会比较有用。

---

#### StreamTask

`watch()` 返回的是一个 `StreamTask`。调用 `watch()` 后会自动运行 `start()`，不需要重复启动。

##### `task.start()`

如果任务还没启动，则开始订阅。

```ts
task.start()
```

##### `task.stop()`

停止订阅，清空缓冲项，把等待中的异步迭代请求都标记为结束，并触发 stop handlers。

```ts
task.stop()
```

##### `task.restart()`

重置内部状态并重新开始。

```ts
task.restart()
```

##### `task.onData(handler)`

给每个入站数据注册一个回调。

```ts
const off = task.onData((data) => {
  console.log(data)
})
// off() 移除回调
```

##### `task.onStop(handler)`

注册任务停止时触发的回调。

```ts
task.onStop(() => {
  console.log('stopped')
})
```

##### `task.next()`

等待下一个数据项。

```ts
const item = await task.next()
```

如果任务已经结束，`next()` 会以 `Error('StreamTask ended')` 拒绝。

##### `for await (const item of task)`

把任务当成异步可迭代对象来消费。

```ts
for await (const item of task) {
  console.log(item)
}
```

---

### 🌐 环境说明

- 浏览器：使用原生 WebSocket
- Node.js：自动使用 `ws`

---

### 📄 License

MIT
