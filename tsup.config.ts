import { defineConfig } from 'tsup'

export default defineConfig({
  entry:     ['src/index.ts'],
  format:    ['cjs', 'esm'],
  dts:       true,
  clean:     true,
  sourcemap: true,
  target:    'node20',
  // zod is an optional peer dep referenced only via `import type` in
  // src/utils/zod.ts. It is stripped at compile time and never appears
  // at runtime. `external` is kept as a defensive no-op in case a future
  // change accidentally adds a runtime zod import.
  external:  ['zod'],
})
