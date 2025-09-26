import type { ConfigExternal } from '@orval/core';
import { defineConfig } from 'orval';

const apiUrl = 'http://localhost:3000';

const config = {
  embucket: {
    input: `${apiUrl}/ui_openapi.json`,
    // input: `./ui_openapi.json`,
    output: {
      mode: 'tags',
      target: './src/orval/api.ts',
      client: 'react-query',
      schemas: './src/orval/models',
      prettier: true,
      override: {
        query: {
          useQuery: true,
        },
        mutator: {
          path: 'src/lib/axiosMutator.ts',
          name: 'useAxiosMutator',
        },
      },
    },
  },
} satisfies ConfigExternal;

export default defineConfig(config);
