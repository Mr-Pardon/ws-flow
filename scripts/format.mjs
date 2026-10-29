import { readdir, readFile, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import * as prettier from 'prettier'
import ts from 'typescript'

const root = fileURLToPath(new URL('../', import.meta.url))
const check = process.argv.includes('--check')
const config = await prettier.resolveConfig(path.join(root, 'package.json'))

async function collect(directory) {
  const files = []

  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const filename = path.join(directory, entry.name)

    if (entry.isDirectory()) {
      if (!['node_modules', 'dist', 'cache'].includes(entry.name)) {
        files.push(...(await collect(filename)))
      }
    } else if (/\.(ts|mts|mjs|vue|css)$/.test(entry.name)) {
      files.push(filename)
    }
  }

  return files
}

// Prettier wraps code but does not insert separators between adjacent methods.
// Use syntax nodes so braces in strings and comments cannot confuse the rule.
function separateMembers(source, filename) {
  const tree = ts.createSourceFile(filename, source, ts.ScriptTarget.Latest, true)
  const edits = []
  const isMethod = node =>
    ts.isMethodDeclaration(node) ||
    ts.isConstructorDeclaration(node) ||
    ts.isGetAccessorDeclaration(node) ||
    ts.isSetAccessorDeclaration(node)

  function separate(nodes, shouldSeparate) {
    for (let index = 1; index < nodes.length; index++) {
      const previous = nodes[index - 1]
      const current = nodes[index]

      if (!shouldSeparate(previous, current)) continue

      const whitespace = /^\s*/.exec(source.slice(previous.end))[0]
      if (whitespace.includes('\n\n')) continue

      // Keep same-line trailing comments attached to the preceding member.
      if (!whitespace.includes('\n')) continue

      edits.push({
        start: previous.end,
        end: previous.end + whitespace.length,
        value: '\n\n' + whitespace.slice(whitespace.lastIndexOf('\n') + 1)
      })
    }
  }

  function visit(node) {
    if (ts.isClassDeclaration(node) || ts.isClassExpression(node)) {
      separate(
        node.members,
        (previous, current) => isMethod(previous) || isMethod(current)
      )
    }

    if (ts.isSourceFile(node)) {
      separate(node.statements, (previous, current) =>
        [previous, current].some(
          item =>
            ts.isClassDeclaration(item) ||
            ts.isFunctionDeclaration(item) ||
            ts.isInterfaceDeclaration(item) ||
            ts.isTypeAliasDeclaration(item)
        )
      )
    }

    ts.forEachChild(node, visit)
  }

  visit(tree)

  for (const edit of edits.sort((a, b) => b.start - a.start)) {
    source = source.slice(0, edit.start) + edit.value + source.slice(edit.end)
  }

  return source
}

const files = []
for (const directory of ['packages', 'scripts', 'tests', 'examples', 'docs/.vitepress']) {
  files.push(...(await collect(path.join(root, directory))))
}

let changed = 0
for (const filename of files) {
  const before = await readFile(filename, 'utf8')
  const options = { ...config, filepath: filename }
  const formatted = await prettier.format(before, options)
  const after = /\.(ts|mts|mjs)$/.test(filename)
    ? await prettier.format(separateMembers(formatted, filename), options)
    : formatted

  if (before === after) continue

  changed++
  if (check) console.error(path.relative(root, filename))
  else await writeFile(filename, after)
}

console.log(
  `${check ? 'Unformatted' : 'Formatted'}: ${changed}; checked: ${files.length}`
)
if (check && changed) process.exitCode = 1
