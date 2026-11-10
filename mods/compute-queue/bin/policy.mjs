// Reuse the mod's pure policy with Node's built-in TypeScript stripping.
// Node >=24.14 is required; no transpiler or parallel command policy lives here.
import { registerHooks } from 'node:module'
import { readFileSync } from 'node:fs'

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === './classify' && context.parentURL?.endsWith('/hooks/allow.ts')) {
      return nextResolve('./classify.ts', context)
    }
    return nextResolve(specifier, context)
  },
})

try {
  const [major, minor] = process.versions.node.split('.').map(Number)
  if (major < 24 || (major === 24 && minor < 14)) throw new Error('Node >=24.14 is required for compute policy TypeScript support')
  const { computeAllowed, suggestRule } = await import('../hooks/allow.ts')
  const { classifyBash } = await import('../hooks/classify.ts')
  const request = JSON.parse(readFileSync(0, 'utf8'))
  if (typeof request.command !== 'string') throw new Error('command must be a string')
  const allowed = computeAllowed(request.command)
  process.stdout.write(JSON.stringify({ allowed, classification: classifyBash(request.command), rule: suggestRule(request.command) }) + '\n')
} catch (error) {
  process.stdout.write(JSON.stringify({ allowed: { ok: false, why: String(error.message ?? error) } }) + '\n')
  process.exitCode = 1
}
