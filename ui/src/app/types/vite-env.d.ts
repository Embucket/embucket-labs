/* eslint-disable @typescript-eslint/consistent-type-imports */
/// <reference types="vite/client" />

type ImportMetaEnvAugmented = import('@julr/vite-plugin-validate-env').ImportMetaEnvAugmented<
  typeof import('../../env').default
>;

interface ImportMetaEnv extends ImportMetaEnvAugmented {}
