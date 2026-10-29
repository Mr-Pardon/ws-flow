import { mkdtemp, mkdir, readFile, writeFile, readdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createRequire } from 'node:module'
import { spawnSync } from 'node:child_process'

const require = createRequire(import.meta.url)
const root = fileURLToPath(new URL('../', import.meta.url))
const pnpm = process.env.npm_execpath
if (!pnpm || !pnpm.includes('pnpm')) throw new Error('Run via pnpm run verify-packages')
const fixture = await mkdtemp(path.join(tmpdir(), 'ws-flow-package-check-'))
const archives = path.join(fixture, 'archives')
await mkdir(archives)
console.log(`Package verification directory: ${fixture}`)

function run(entry, args, cwd, quiet = false) {
  const result = spawnSync(process.execPath, [entry, ...args], {
    cwd,
    encoding: 'utf8',
    stdio: quiet ? 'pipe' : 'inherit'
  })
  if (result.error) throw result.error
  if (result.status !== 0) {
    if (quiet) console.error(result.stdout, result.stderr)
    throw new Error(`Verification command failed: ${entry} ${args.join(' ')}`)
  }
}

const dependencies = {}
const packageManifests = new Map()
const rootLicense = await readFile(path.join(root, 'LICENSE'))
for (const directory of await readdir(path.join(root, 'packages'))) {
  const packageRoot = path.join(root, 'packages', directory)
  const manifest = JSON.parse(
    await readFile(path.join(packageRoot, 'package.json'), 'utf8')
  )
  packageManifests.set(manifest.name, manifest)
  if (
    manifest.private === true ||
    !/^\d+\.\d+\.\d+$/.test(manifest.version) ||
    manifest.publishConfig?.access !== 'public'
  ) {
    throw new Error(
      `Package is not configured for a public stable release: ${manifest.name}`
    )
  }
  run(pnpm, ['pack', '--pack-destination', archives], packageRoot, true)
  const filename = `${manifest.name.replace('@', '').replace('/', '-')}-${manifest.version}.tgz`
  dependencies[manifest.name] = `file:./archives/${filename}`
}
await writeFile(
  path.join(fixture, 'package.json'),
  JSON.stringify(
    {
      name: 'ws-flow-package-fixture',
      private: true,
      type: 'module',
      dependencies: {
        ...dependencies,
        ws: packageManifests.get('@ws-flow/server').dependencies.ws
      },
      devDependencies: {
        '@types/ws': packageManifests.get('@ws-flow/server').dependencies['@types/ws']
      }
    },
    null,
    2
  )
)
// Resolve every internal dependency to this build's tarball, without falling
// back to registry versions, source directories or workspace symlinks.
await writeFile(
  path.join(fixture, 'pnpm-workspace.yaml'),
  'overrides:\n' +
    Object.entries(dependencies)
      .map(([name, archive]) => `  '${name}': '${archive}'`)
      .join('\n') +
    '\n'
)
run(pnpm, ['install', '--ignore-scripts'], fixture)

const documentationExamples = []
for (const name of Object.keys(dependencies)) {
  const installedRoot = path.join(fixture, 'node_modules', name)
  const manifest = JSON.parse(
    await readFile(path.join(installedRoot, 'package.json'), 'utf8')
  )
  const sourceManifest = packageManifests.get(name)
  if (manifest.name !== name || manifest.version !== sourceManifest.version) {
    throw new Error(`Installed package identity differs: ${name}`)
  }
  for (const field of ['description', 'author', 'license']) {
    if (typeof manifest[field] !== 'string' || !manifest[field].trim()) {
      throw new Error(`Missing package metadata ${field}: ${name}`)
    }
  }
  if (
    !Array.isArray(manifest.keywords) ||
    manifest.keywords.length === 0 ||
    manifest.keywords.some(value => typeof value !== 'string' || !value.trim()) ||
    typeof manifest.engines?.node !== 'string' ||
    !manifest.engines.node.trim()
  ) {
    throw new Error(`Missing package keywords or Node.js engine: ${name}`)
  }
  const license = await readFile(path.join(installedRoot, 'LICENSE'))
  if (!license.equals(rootLicense)) {
    throw new Error(`Installed LICENSE differs from root LICENSE: ${name}`)
  }
  for (const section of [
    'dependencies',
    'optionalDependencies',
    'peerDependencies',
    'devDependencies'
  ]) {
    const installedDependencies = manifest[section] ?? {}
    for (const [dependency, version] of Object.entries(installedDependencies)) {
      if (
        typeof version !== 'string' ||
        /^(workspace:|file:|link:)/i.test(version.trim())
      ) {
        throw new Error(`Development dependency reference in ${name}: ${dependency}`)
      }
      const internal = packageManifests.get(dependency)
      if (internal && version !== internal.version) {
        throw new Error(`Incorrect internal dependency version in ${name}: ${dependency}`)
      }
    }
    for (const dependency of Object.keys(sourceManifest[section] ?? {})) {
      const internal = packageManifests.get(dependency)
      if (internal && installedDependencies[dependency] !== internal.version) {
        throw new Error(`Missing internal dependency version in ${name}: ${dependency}`)
      }
    }
  }
  const readme = await readFile(path.join(installedRoot, 'README.md'), 'utf8')
  const directory = name.split('/')[1]
  const sourceReadme = await readFile(
    path.join(root, 'packages', directory, 'README.md'),
    'utf8'
  )
  if (readme !== sourceReadme) throw new Error(`Installed README differs: ${name}`)

  let index = 0
  for (const match of readme.matchAll(/```ts\r?\n([\s\S]*?)```/g)) {
    const filename = `readme-${directory}-${++index}.mts`
    await writeFile(path.join(fixture, filename), 'export {}\n' + match[1])
    documentationExamples.push(filename)
  }
}

const readmeExampleCount = documentationExamples.length

async function collectManualExamples(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const filename = path.join(directory, entry.name)
    if (entry.isDirectory()) {
      if (entry.name !== 'public') await collectManualExamples(filename)
      continue
    }
    if (!entry.name.endsWith('.md')) continue
    const source = await readFile(filename, 'utf8')
    const name = path
      .relative(path.join(root, 'docs', 'manual'), filename)
      .replaceAll(path.sep, '-')
      .replace(/\.md$/, '')
    let index = 0
    for (const match of source.matchAll(/```ts[^\r\n]*\r?\n([\s\S]*?)```/g)) {
      const example = `manual-${name}-${++index}.mts`
      await writeFile(path.join(fixture, example), 'export {}\n' + match[1])
      documentationExamples.push(example)
    }
  }
}

await collectManualExamples(path.join(root, 'docs', 'manual'))

const exports = {
  server: 'WSServer',
  client: 'WSClient',
  'runtime-core': 'RuntimeCore',
  envelope: 'createEnvelopeBuilder',
  pipeline: 'PipelineRegistry',
  scheduler: 'SessionScheduler',
  consumer: 'StreamConsumer'
}
const imports = Object.entries(exports)
  .map(([name, symbol]) => `import { ${symbol} } from '@ws-flow/${name}';`)
  .join('\n')
const adapterTypes = `
import { QueueCapacityError, type QueueLimits } from '@ws-flow/scheduler';
import { genericAdapter, wsFlowAdapter, type ProtocolAdapter } from '@ws-flow/runtime-core';
import { ByteBudget, TransportLimitError, TransportSendQueue, type ByteLimits, type SendTransportLimits, type PreparedTransportSend } from '@ws-flow/runtime-core';
import { ActiveTaskBudget, TaskCapacityError, TaskTimeoutError, type RuntimeTaskLimits, type RuntimeTaskTimeouts, type TaskFailure } from '@ws-flow/runtime-core';
const taskLimits: RuntimeTaskLimits = { maxActive: 512, maxActivePerPeer: 32, incomingRequests: { maxActive: 128 }, outgoingStreams: { maxActivePerPeer: 8 } };
const taskTimeouts: RuntimeTaskTimeouts = { incomingRequestMs: 5000, streamIdleMs: 1000 };
const activeTasks = new ActiveTaskBudget(taskLimits);
activeTasks.reserve({ id: 'active-peer' }, 'incomingStreams')();
const byteLimits: ByteLimits = { maxMessageBytes: 4096, maxPendingBytes: 65536, maxPendingBytesPerPeer: 16384 };
const sendLimits: SendTransportLimits = { ...byteLimits, highWaterMark: 16384, lowWaterMark: 8192, waitTimeoutMs: 1000, pollIntervalMs: 10 };
const budget = new ByteBudget(byteLimits);
budget.reserve({ id: 'budget-peer' }, 4)();
const prepared: PreparedTransportSend = { send: () => {}, release: () => {} };
const preparedRuntime = new RuntimeCore({ base: { transport: { send: () => {}, prepare: () => prepared } } });
preparedRuntime.useTransportErrorObserver(info => { const fatal: boolean = info.fatal; const error: TransportLimitError = info.error; });
const wireQueue = new TransportSendQueue<{ id: string }>();
const limits: QueueLimits = { maxQueued: 32, maxQueuedPerPeer: 8 };
const scheduler = new SessionScheduler(limits);
const queued: number = scheduler.queuedCount;
scheduler.schedule({ signal: new AbortController().signal, run: async () => {} });
const capacityError = new QueueCapacityError('peer', 8, 'peer');
const adapter: ProtocolAdapter = genericAdapter({
  event: input => typeof input === 'object' && input !== null && 'type' in input
    && typeof input.type === 'string' ? input.type : undefined,
});
const runtime = new RuntimeCore({ base: { protocol: adapter, transport: { send: () => {} } } });
runtime.send({ id: 'peer', state: 'open' }, { action: 'ping' });
const subscription = runtime.subscribe(raw => typeof raw === 'string', { capacity: 4 });
subscription.cancel();
wsFlowAdapter();
const standardRuntime = new RuntimeCore({ base: { transport: { send: () => {} } }, taskLimits, taskTimeouts });
standardRuntime.useTaskErrorObserver((info: TaskFailure) => { const error: TaskCapacityError | TaskTimeoutError = info.error; info.kind; });
const activeCount: number = standardRuntime.getActiveTaskCount(undefined, 'incomingStreams');
const signal = new AbortController().signal;
const sending = standardRuntime.createSendStream({ id: 'peer', state: 'open' }, 'values', { signal, idleTimeoutMs: null });
sending.cancel();
const server = new WSServer({ taskLimits, taskTimeouts, queueLimits: { receive: limits, send: limits }, transportLimits: { receive: byteLimits, send: sendLimits } });
server.onIssue('resource_error', issue => { const direction: 'send' | 'receive' = issue.direction; if ('limit' in issue.error) issue.error.limit; issue.taskKind; });
standardRuntime.useQueueErrorObserver(info => { const fatal: boolean = info.fatal; info.error.scope; });
const client = new WSClient({ taskLimits, taskTimeouts, protocol: 'ws-flow', reconnect: { maxAttempts: 2 }, queueLimits: { receive: limits, send: limits }, transportLimits: { receive: byteLimits, send: sendLimits } });
client.handleRequest<number, number>('double', ctx => ctx.data * 2);
client.handleStream<unknown, number>('numbers', async function* () { yield 1; });
client.on('notice', (data, message) => { message.peer.socket; });
client.onStream('upload', async ctx => { if (ctx.stream) for await (const chunk of ctx.stream) {} });
client.subscribe(() => true, { capacity: 8 }).cancel();
client.dispose();
server.on('notice', (data, message) => { message.peer.id; });
server.onStream('upload', async ctx => { if (ctx.stream) for await (const chunk of ctx.stream) {} });
server.subscribe(() => true).cancel();
server.all().requestAll<number>('double', 2).then(results => { for (const result of results) if (result.status === 'fulfilled') { const value: number = result.value; } });
server.handleRequest<{ value: number }, number>('double', ctx => ctx.data.value * 2);
server.handleStream<{ value: number }, number>('numbers', async function* (ctx) { yield ctx.data.value; });
const result: Promise<number> = standardRuntime.request<number>({ id: 'peer', state: 'open' }, 'double', { value: 2 }, { signal, timeoutMs: 1000 });
for (const session of server.sessions) {
  session.stream(async function* () { yield 1; }, { signal, idleTimeoutMs: null });
  const response: Promise<number> = session.request<number>('double', { value: 2 }, { signal, timeoutMs: null });
  session.handleRequest<{ value: number }, number>('double', ctx => ctx.data.value * 2);
  const chunks: AsyncIterable<number> = session.requestStream<number>('numbers', {}, { capacity: 16, signal, idleTimeoutMs: 1000 });
  session.handleStream<unknown, number>('local-stream', async function* () { yield 1; });
  session.subscribe(() => true).cancel();
  session.on('notice', (data, message) => { message.peer.id; });
}
`
await writeFile(path.join(fixture, 'types.mts'), imports + adapterTypes)
run(
  require.resolve('typescript/bin/tsc'),
  [
    '--noEmit',
    '--strict',
    '--skipLibCheck',
    'false',
    '--module',
    'NodeNext',
    '--moduleResolution',
    'NodeNext',
    '--target',
    'ES2020',
    'types.mts',
    ...documentationExamples
  ],
  fixture
)

const testFiles = (await readdir(path.join(root, 'tests'))).filter(name =>
  name.endsWith('.test.mjs')
)
for (const file of testFiles) {
  let source = await readFile(path.join(root, 'tests', file), 'utf8')
  for (const name of Object.keys(exports)) {
    source = source.replaceAll(`../packages/${name}/dist/index.js`, `@ws-flow/${name}`)
  }
  source = source.replace(
    "new URL('../packages/server/package.json', import.meta.url)",
    "import.meta.resolve('@ws-flow/server')"
  )
  source = source.replaceAll(
    '../packages/client/dist/browser.js',
    '@ws-flow/client/browser'
  )
  await writeFile(path.join(fixture, file), source)
}
const smoke = Object.keys(exports)
  .map(name => `await import('@ws-flow/${name}');`)
  .join('\n')
await writeFile(path.join(fixture, 'imports.mjs'), smoke)
run(path.join(fixture, 'imports.mjs'), [], fixture)
const result = spawnSync(process.execPath, ['--test', ...testFiles], {
  cwd: fixture,
  stdio: 'inherit'
})
if (result.error) throw result.error
if (result.status !== 0) throw new Error('Installed package transport tests failed')
console.log(
  `Verified installed licenses, metadata, dependency versions, README files, ${readmeExampleCount} README examples, ${documentationExamples.length - readmeExampleCount} manual examples, public types, exports and socket behavior.`
)
const browserOptions = process.argv.includes('--browser-manual')
  ? ['--manual']
  : process.argv.includes('--browser-bundle-only')
    ? ['--bundle-only']
    : []
run(path.join(root, 'scripts/verify-browser.mjs'), [fixture, ...browserOptions], root)
