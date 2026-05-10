import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: [
      'tests/integration/*.test.ts',
      'tests/client/*.test.ts',
      'tests/consumer/*.test.ts',
      'tests/stream/*.test.ts',
      'tests/e2e.test.ts',
    ],
    globalSetup: ['tests/helpers/globalSetup.ts'],
    hookTimeout: 30_000,
    globals:     false,
  },
})
