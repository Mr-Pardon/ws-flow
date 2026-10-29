import { readdir, cp, rm } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createRequire } from 'node:module'
import { spawnSync } from 'node:child_process'

const require = createRequire(import.meta.url)
const packagesRoot = fileURLToPath(new URL('../packages/', import.meta.url))
const packageRoot = process.cwd()
const relative = path.relative(packagesRoot, packageRoot)
if (
  !relative ||
  relative.startsWith('..') ||
  path.isAbsolute(relative) ||
  relative.includes(path.sep)
) {
  throw new Error('Run this build script from a direct child of packages/')
}
// Only remove generated output inside the verified package directory.
const output = path.resolve(packageRoot, 'dist')
if (path.dirname(output) !== packageRoot) throw new Error('Invalid output directory')
await rm(output, { recursive: true, force: true })
const result = spawnSync(
  process.execPath,
  [require.resolve('typescript/bin/tsc'), '-p', 'tsconfig.json'],
  {
    stdio: 'inherit'
  }
)
if (result.error) throw result.error
if (result.status !== 0) process.exit(result.status ?? 1)

// tsc emits declarations for .ts, but does not copy hand-authored .d.ts files.
async function copyDeclarations(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const source = path.join(directory, entry.name)
    if (entry.isDirectory()) await copyDeclarations(source)
    else if (entry.name.endsWith('.d.ts')) {
      await cp(
        source,
        path.join(output, path.relative(path.join(packageRoot, 'src'), source))
      )
    }
  }
}

await copyDeclarations(path.join(packageRoot, 'src'))
