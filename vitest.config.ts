import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: [
      'tests/proto/*.test.ts',
      'tests/unit/*.test.ts',
      'tests/utils/*.test.ts',
      'tests/topic/*.test.ts',
    ],
    globals: false,
  },
})
