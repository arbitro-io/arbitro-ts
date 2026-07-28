import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/0[7-9]-*.test.ts', 'tests/[1-9][0-9]-*.test.ts'],
    globalSetup: ['tests/helpers/globalSetup.ts'],
    hookTimeout: 30_000,
    // Queue tests wait on real broker round-trips and redelivery deadlines,
    // which outlast the 5s default.
    testTimeout: 30_000,
    globals: false,
  },
})
