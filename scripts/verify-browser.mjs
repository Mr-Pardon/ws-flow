import { build } from 'esbuild'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { mkdtemp, readFile, writeFile, access } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { spawn } from 'node:child_process'

// Accept the installed-tarball directory to verify actual package conditions.
const bundleOnly = process.argv.includes('--bundle-only')
const manual = process.argv.includes('--manual')
const fixture = process.argv.slice(2).find(argument => !argument.startsWith('--'))
const root = fileURLToPath(new URL('../', import.meta.url))
const output = await mkdtemp(path.join(tmpdir(), 'ws-flow-browser-'))
const clientModule = fixture
  ? '@ws-flow/client'
  : path.join(root, 'packages/client/dist/browser.js').replaceAll('\\', '/')
const source = `import { WSClient } from ${JSON.stringify(clientModule)};
const run = async () => {
 const client = new WSClient({ protocol: 'ws-flow', taskLimits: { outgoingRequests: { maxActivePerPeer: 1 } }, taskTimeouts: { streamIdleMs: 1000 }, transportLimits: { send: { highWaterMark: 65536, maxMessageBytes: 4096 } } });
 const issues = []; client.onIssue(e => issues.push(String(e)));
 client.handleRequest('reverse', ctx => ctx.data * 2);
 await client.connect('ws://' + location.host);
 const native = client.getWsInstance() instanceof WebSocket;
 const value = await client.request('double', 21);
 const reverse = await client.request('ask-client', 3);
 const values = []; for await (const n of client.requestStream('numbers', null)) values.push(n);
 if (!native || value !== 42 || reverse !== 6 || JSON.stringify(values) !== '[1,2]' || issues.length) throw new Error('Browser assertions failed');
 const stalled = client.request('stall', null, { timeoutMs: null }).then(() => undefined, error => error);
 let capacityError;
 try { await client.request('double', 3); } catch (error) { capacityError = error; }
 const executionError = await stalled;
 if (capacityError?.code !== 'CAPACITY_EXCEEDED' || executionError?.code !== 'EXECUTION_TIMEOUT' || client.state !== 'open' || issues.length !== 1) throw new Error('Browser task admission/deadline failed');
 const recovered = await client.request('double', 2);
 if (recovered !== 4) throw new Error('Browser task capacity did not recover');
 client.dispose();
 const bounded = new WSClient({ protocol: 'ws-flow', queueLimits: { receive: { maxQueued: 1 } } });
 const queueIssues = []; bounded.onIssue(error => queueIssues.push(error));
 let release; const gate = new Promise(resolve => { release = resolve });
 bounded.useReceive(async ctx => { if (ctx.envelope.event === 'blocked') await gate });
 let queueCloseCode;
 try {
   await bounded.connect('ws://' + location.host);
   const socket = bounded.getWsInstance();
   const closed = new Promise(resolve => socket.addEventListener('close', resolve, { once: true }));
   let rejection;
   try { await bounded.request('overload', null, { timeoutMs: null }); } catch (error) { rejection = error; }
   queueCloseCode = (await closed).code;
   if (rejection?.code !== 'QUEUE_CAPACITY_EXCEEDED' || queueCloseCode !== 4008 || queueIssues.length !== 1) throw new Error('Browser queue overflow cleanup failed');
 } finally { release(); bounded.dispose(); }
 const byteBounded = new WSClient({ protocol: 'ws-flow', transportLimits: { receive: { maxMessageBytes: 256 } } });
 const byteIssues = []; byteBounded.onIssue(error => byteIssues.push(error));
 let byteCloseCode;
 try {
   await byteBounded.connect('ws://' + location.host);
   const socket = byteBounded.getWsInstance();
   const closed = new Promise(resolve => socket.addEventListener('close', resolve, { once: true }));
   let rejection;
   try { await byteBounded.request('oversized', null, { timeoutMs: null }); } catch (error) { rejection = error; }
   byteCloseCode = (await closed).code;
   if (rejection?.code !== 'MESSAGE_TOO_LARGE' || byteCloseCode !== 4009 || byteIssues.length !== 1) throw new Error('Browser byte overflow cleanup failed');
 } finally { byteBounded.dispose(); }
 return { native, value, reverse, values, taskCapacityCode: capacityError.code, executionCode: executionError.code, recovered, queueCloseCode, byteCloseCode };
};
run().then(result => fetch('/result', { method: 'POST', body: JSON.stringify({ ok: true, result }) }), error => fetch('/result', { method: 'POST', body: JSON.stringify({ ok: false, error: String(error) }) }));`
const bundle = await build({
  stdin: {
    contents: source,
    resolveDir: fixture ?? root,
    sourcefile: 'browser-check.js'
  },
  bundle: true,
  platform: 'browser',
  format: 'esm',
  minify: true,
  metafile: true,
  write: false
})
const inputs = Object.keys(bundle.metafile.inputs)
if (
  inputs.some(input =>
    /(?:node_modules[\\/]ws[\\/]|ws-event-proxy|client[\\/]dist[\\/]node\.js)/.test(input)
  )
)
  throw new Error('Browser bundle includes a Node or legacy transport')
await writeFile(path.join(output, 'client.js'), bundle.outputFiles[0].contents)
console.log(
  `Browser bundle: ${bundle.outputFiles[0].contents.length} bytes minified; no ws or legacy proxy. ${output}`
)

if (bundleOnly) {
  console.log('Bundle-only verification passed; native browser handshake not run.')
  process.exit(0)
}

const candidates = [
  process.env.WS_FLOW_BROWSER,
  'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
  'C:/Program Files/Google/Chrome/Application/chrome.exe'
].filter(Boolean)
let browser
for (const candidate of candidates) {
  try {
    await access(candidate)
    browser = candidate
    break
  } catch {}
}
if (!browser && !manual) {
  console.log(
    'Native browser handshake skipped: set WS_FLOW_BROWSER to Chromium/Chrome/Edge executable. Bundle validation passed.'
  )
  process.exit(0)
}
// The server is ESM-only. Resolve its public import from an ESM module in the fixture.
if (fixture)
  await writeFile(
    path.join(fixture, 'browser-server.mjs'),
    "export const entry = import.meta.resolve('@ws-flow/server')\n"
  )
const serverEntry = fixture
  ? (await import(pathToFileURL(path.join(fixture, 'browser-server.mjs')))).entry
  : new URL('../packages/server/dist/index.js', import.meta.url)
const require = createRequire(serverEntry)
const { WebSocketServer } = require('ws')
const { WSServer } = await import(serverEntry)
let resolveResult
const result = new Promise(resolve => {
  resolveResult = resolve
})
const http = createServer(async (req, res) => {
  if (req.url === '/result' && req.method === 'POST') {
    let body = ''
    for await (const chunk of req) body += chunk
    resolveResult(JSON.parse(body))
    res.end('ok')
    return
  }
  if (req.url === '/client.js') {
    res.setHeader('Content-Type', 'text/javascript')
    res.end(await readFile(path.join(output, 'client.js')))
    return
  }
  res.setHeader('Content-Type', 'text/html')
  res.end(
    '<!doctype html><title>ws-flow browser verification</title><script type="module" src="/client.js"></script>'
  )
})
await new Promise(resolve => http.listen(0, '127.0.0.1', resolve))
const wss = new WebSocketServer({ server: http }),
  server = WSServer.takeover(wss, { taskTimeouts: { incomingRequestMs: 250 } })
server.handleRequest('double', ctx => ctx.data * 2)
server.handleRequest('stall', () => new Promise(() => {}))
server.handleRequest('ask-client', ctx => ctx.session.request('reverse', ctx.data))
server.onIssue(() => {})
server.handleRequest('overload', async ctx => {
  for (let index = 0; index < 4; index++) await ctx.session.emit('blocked', index)
  return 'done'
})
server.handleRequest('oversized', async ctx => {
  await ctx.session.emit('payload', '字'.repeat(100))
  return 'done'
})
server.handleStream('numbers', async function* () {
  yield 1
  yield 2
})
const processBrowser = manual
  ? undefined
  : spawn(
      browser,
      [
        '--headless',
        '--disable-gpu',
        '--no-first-run',
        '--no-default-browser-check',
        `--user-data-dir=${path.join(output, 'profile')}`,
        `http://127.0.0.1:${http.address().port}/`
      ],
      { windowsHide: true, stdio: 'ignore' }
    )
if (manual) {
  console.log(`Open in a browser to verify: http://127.0.0.1:${http.address().port}/`)
}
let timer
try {
  const outcome = await Promise.race([
    result,
    new Promise((_, reject) => {
      timer = setTimeout(
        () => reject(new Error('Native browser test timed out')),
        manual ? 180_000 : 20_000
      )
      processBrowser?.once('error', reject)
    })
  ])
  if (!outcome.ok) throw new Error(outcome.error)
  console.log(
    'Native browser WebSocket, bidirectional RPC, streaming, resource limits and execution deadline passed:',
    outcome.result
  )
} finally {
  clearTimeout(timer)
  processBrowser?.kill()
  server.close()
  for (const socket of wss.clients) socket.terminate()
  await new Promise(resolve => wss.close(resolve))
  http.closeAllConnections()
  await new Promise(resolve => http.close(resolve))
}
