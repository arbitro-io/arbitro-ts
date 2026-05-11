import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/0[7-9]-*.test.ts', 'tests/1[0-9]-*.test.ts'],
    globalSetup: ['tests/helpers/globalSetup.ts'],
    hookTimeout: 30_000,
    globals: false,
  },
})
