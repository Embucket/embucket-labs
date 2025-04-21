import { ValidateEnv } from '@julr/vite-plugin-validate-env';
import tailwindcss from '@tailwindcss/vite';
import { TanStackRouterVite } from '@tanstack/router-plugin/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';
import tsconfigPaths from 'vite-tsconfig-paths';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    tailwindcss(),
    TanStackRouterVite({
      target: 'react',
      // https://tanstack.com/router/latest/docs/framework/react/guide/automatic-code-splitting#automatic-code-splitting
      autoCodeSplitting: true,
      routesDirectory: 'src/app/routes',
      generatedRouteTree: 'src/app/routeTree.gen.ts',
      semicolons: true,
    }),
    react(), // Make sure to add this plugin after the TanStack Router Bundler plugin
    tsconfigPaths(),
    ValidateEnv(),
  ],
});
