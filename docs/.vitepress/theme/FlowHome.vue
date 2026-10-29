<script setup lang="ts">
import { computed, onUnmounted, ref } from 'vue'
import { withBase } from 'vitepress'

const modes = [
  {
    name: '事件通知',
    method: 'emit',
    outbound: 'event: status',
    inbound: '订阅关心的事件',
    result: '每个订阅独立消费，消息自然分流。',
    code: "await client.emit('status', { ready: true })",
    label: '发送一条通知'
  },
  {
    name: '双向请求',
    method: 'request',
    outbound: 'request · #42',
    inbound: 'response · #42',
    result: '请求与结果自动关联，双方都能发起。',
    code: "const result = await client.request('double', 21)",
    label: '自动收回对应结果'
  },
  {
    name: '流式任务',
    method: 'requestStream',
    outbound: 'request · progress',
    inbound: 'start → chunk → end',
    result: '逐步接收结果，用异步迭代控制消费。',
    code: "for await (const chunk of client.requestStream('progress', null)) {\n  console.log(chunk)\n}",
    label: '让连续结果成为一次任务'
  }
]
const selected = ref(1)
const current = computed(() => modes[selected.value])
const copied = ref(false)
let copiedTimer: ReturnType<typeof setTimeout> | undefined
onUnmounted(() => clearTimeout(copiedTimer))

async function copyCommand() {
  try {
    await navigator.clipboard.writeText('npm install @ws-flow/client@^2')
    copied.value = true
    clearTimeout(copiedTimer)
    copiedTimer = setTimeout(() => (copied.value = false), 2000)
  } catch {
    copied.value = false
  }
}

const capabilities = [
  {
    n: '01',
    title: '只订阅你关心的',
    text: '从完整事件流中筛选消息。每个订阅独立消费，保持业务边界。',
    api: 'subscribe / on',
    link: '/guide/events'
  },
  {
    n: '02',
    title: '发起请求，等待答案',
    text: '让内核完成结果关联、超时与取消。Client 和 Server 同样自然。',
    api: 'request / handleRequest',
    link: '/guide/requests'
  },
  {
    n: '03',
    title: '让结果持续到达',
    text: '一次请求，逐步返回。用 for await 消费进度，用 break 提前退出。',
    api: 'requestStream / handleStream',
    link: '/guide/streams'
  },
  {
    n: '04',
    title: '主动推送一段数据流',
    text: '无需先发请求。把持续更新交给有开始、有终点的流生命周期。',
    api: 'stream / onStream',
    link: '/guide/streams'
  }
]
</script>

<template>
  <main class="flow-home">
    <section class="flow-hero" aria-labelledby="home-title">
      <div class="flow-intro">
        <p class="flow-eyebrow"><span></span> WEBSOCKET, WITH A FLOW.</p>
        <h1 id="home-title">让双向通信，<br /><em>有条不紊。</em></h1>
        <p class="flow-lead">
          事件各有所归，请求自有回应。<br />用一套清晰的模型，组织订阅、双向调用与流式任务。
        </p>
        <div class="flow-actions">
          <a class="flow-button primary" :href="withBase('/guide/quick-start')"
            >开始使用 <span aria-hidden="true">↗</span></a
          >
          <a class="flow-button secondary" :href="withBase('/examples/')"
            >查看完整案例 <span aria-hidden="true">→</span></a
          >
        </div>
        <div class="flow-platforms">
          <span>TypeScript</span><i></i><span>Node.js & Browser</span><i></i
          ><span>MIT</span>
        </div>
      </div>

      <div class="flow-console">
        <div class="flow-console-heading">
          <span class="flow-window-dots"><i></i><i></i><i></i></span
          ><span>ONE CONNECTION. BOTH DIRECTIONS.</span
          ><span class="flow-console-symbol">↔</span>
        </div>
        <div class="flow-mode-picker" aria-label="选择通信模式">
          <button
            v-for="(mode, index) in modes"
            :key="mode.method"
            :aria-pressed="selected === index"
            @click="selected = index"
          >
            {{ mode.name }}
          </button>
        </div>
        <div class="flow-diagram" :class="`mode-${selected}`">
          <div class="flow-endpoints">
            <span><b>C</b> Client</span><span>Server <b>S</b></span>
          </div>
          <div class="flow-lane outbound">
            <span class="flow-message">{{ current.outbound }}</span
            ><span class="flow-line"></span
            ><span class="flow-arrow" aria-hidden="true">→</span>
          </div>
          <div class="flow-lane inbound">
            <span class="flow-arrow" aria-hidden="true">←</span
            ><span class="flow-line"></span
            ><span class="flow-message">{{ current.inbound }}</span>
          </div>
          <div class="flow-diagram-caption">
            <span class="flow-status-dot"></span><span>消息路径示意</span
            ><span>01 / CONNECTION</span>
          </div>
        </div>
        <div class="flow-code" aria-live="polite">
          <div>
            <span>client.ts</span><span>{{ current.label }}</span>
          </div>
          <pre><code>{{ current.code }}</code></pre>
        </div>
        <p class="flow-console-note" aria-live="polite">{{ current.result }}</p>
      </div>
    </section>

    <div class="flow-principles">
      <span>一个连接，多种通信意图。</span>
      <p>独立的 Client <b>·</b> 共享的任务内核 <b>·</b> 按需扩展的处理管线</p>
    </div>

    <section class="flow-capabilities" aria-labelledby="capabilities-title">
      <div class="flow-section-heading">
        <div>
          <p class="flow-eyebrow">FOUR WAYS TO CONNECT</p>
          <h2 id="capabilities-title">按意图通信，<br />让代码贴近业务。</h2>
        </div>
        <p>无需手动在事件流里寻找响应。<br />选择合适的 API，其余交给任务模型。</p>
      </div>
      <div class="flow-capability-list">
        <a
          v-for="item in capabilities"
          :key="item.n"
          :href="withBase(item.link)"
          class="flow-capability"
          ><span class="flow-number">{{ item.n }}</span>
          <div>
            <h3>{{ item.title }}</h3>
            <p>{{ item.text }}</p>
          </div>
          <code>{{ item.api }}</code
          ><span class="flow-link-arrow" aria-hidden="true">↗</span></a
        >
      </div>
    </section>

    <section class="flow-paths" aria-labelledby="paths-title">
      <div class="flow-section-heading">
        <div>
          <p class="flow-eyebrow">START WHERE YOU ARE</p>
          <h2 id="paths-title">接入你的服务，<br />或从两端一起开始。</h2>
        </div>
      </div>
      <div class="flow-path-grid">
        <a :href="withBase('/examples/generic-client')" class="flow-path"
          ><div class="flow-path-top">
            <span>01 / CLIENT</span><span aria-hidden="true">↗</span>
          </div>
          <h3>已有 WebSocket 服务？</h3>
          <p>
            使用独立 Client
            保留原始消息，订阅关心的数据。需要请求或流语义时，再适配你的协议。
          </p>
          <div class="flow-path-bottom">
            <code>new WSClient()</code><span>查看通用客户端 →</span>
          </div></a
        >
        <a :href="withBase('/guide/quick-start')" class="flow-path featured"
          ><div class="flow-path-top">
            <span>02 / CLIENT + SERVER</span><span aria-hidden="true">↗</span>
          </div>
          <h3>构建完整的双向通信。</h3>
          <p>
            启用配套协议，直接使用事件、请求与流。两端共享同一套关联、取消和清理语义。
          </p>
          <div class="flow-path-bottom">
            <code>protocol: 'ws-flow'</code><span>启动第一个例子 →</span>
          </div></a
        >
      </div>
    </section>

    <section class="flow-design" aria-labelledby="design-title">
      <div>
        <p class="flow-eyebrow">CLEAR BOUNDARIES. ROOM TO GROW.</p>
        <h2 id="design-title">简单开始，按需深入。</h2>
        <p>
          协议解释消息，插件转换数据，调度器安排顺序。<br />共享内核负责完整的任务生命周期。
        </p>
        <a :href="withBase('/design/')">了解架构模型 <span aria-hidden="true">→</span></a>
      </div>
      <div class="flow-stack" aria-label="架构层次">
        <div><span>使用入口</span><b>Client · Server / Session</b></div>
        <div><span>任务内核</span><b>订阅 · 请求 · 流 · 生命周期</b></div>
        <div><span>扩展边界</span><b>Protocol · Pipeline · Scheduler</b></div>
        <div><span>环境接入</span><b>Browser WebSocket · Node ws</b></div>
      </div>
    </section>

    <section class="flow-start">
      <div>
        <p class="flow-eyebrow">MAKE THE FIRST CONNECTION</p>
        <h2>从一次真实通信开始。</h2>
        <p>安装 Client 接入你的服务，或搭配 Server 开始双向通信。</p>
      </div>
      <button
        class="flow-command"
        @click="copyCommand"
        :aria-label="copied ? '命令已复制' : '复制安装命令'"
      >
        <span aria-hidden="true">$</span><code>npm install @ws-flow/client@^2</code
        ><span class="flow-copy" aria-live="polite">{{
          copied ? '已复制 ✓' : '复制 ↗'
        }}</span>
      </button>
    </section>
  </main>
</template>
