import { defineConfig } from '@julr/vite-plugin-validate-env';
import { z } from 'zod';

export default defineConfig({
  validator: 'standard',
  schema: {
    VITE_API_URL: z.string(),
  },
});
