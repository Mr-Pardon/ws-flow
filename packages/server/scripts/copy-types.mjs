import { promises as fs } from 'node:fs'
import path from 'node:path'

const root = process.cwd()
const srcTypes = path.join(root, 'src', 'types')
const distTypes = path.join(root, 'dist', 'types')

await fs.mkdir(distTypes, { recursive: true })
await fs.cp(srcTypes, distTypes, { recursive: true, force: true })
