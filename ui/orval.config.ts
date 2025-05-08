import type { ConfigExternal } from '@orval/core';
import dotenv from 'dotenv';
import { defineConfig } from 'orval';

dotenv.config();

// eslint-disable-next-line n/no-process-env
const apiUrl = process.env.VITE_API_URL!;

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
          useInfinite: true,
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
