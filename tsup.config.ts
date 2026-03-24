import { defineConfig } from 'tsup'

export default defineConfig({
  entry:     ['src/index.ts', 'src/zod.ts'],
  format:    ['cjs', 'esm'],
  dts:       true,
  clean:     true,
  sourcemap: true,
  target:    'node20',
  // zod is an optional peer dep — mark it external so it is never bundled.
  external:  ['zod'],
})
