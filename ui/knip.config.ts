import type { KnipConfig } from 'knip';

const config: KnipConfig = {
  entry: ['src/app/routes/**/*.tsx', 'src/app/main.tsx'],
  ignore: [
    'src/app/routeTree.gen.ts',
    'env.ts',
    'orval.config.ts',
    '**/*.d.ts',
    'src/orval/**/*.ts',
    'dist/**/*.js',
    'src/components/ui/**/*.tsx',
    'src/mocks/**/*.ts',
    'seed.js',
  ],
  ignoreDependencies: ['@orval/core', 'tailwindcss'],
  ignoreBinaries: ['only-allow'],
  paths: {
    '@/*': ['./src/*'],
  },
};

export default config;
