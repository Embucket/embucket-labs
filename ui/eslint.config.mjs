// @ts-check

import { join } from 'path';

import eslint from '@eslint/js';
import pluginQuery from '@tanstack/eslint-plugin-query';
import checkFile from 'eslint-plugin-check-file';
import nodePlugin from 'eslint-plugin-n';
import react from 'eslint-plugin-react';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import { defineConfig } from 'eslint/config';
// import tailwindcss from 'eslint-plugin-tailwindcss';
import tseslint from 'typescript-eslint';

// https://eslint.org/docs/latest/use/configure/configuration-files
export default defineConfig([
  {
    ignores: [
      '**/orval/**',
      '**/dist/**',
      '**/node_modules/**',
      'eslint.config.mjs',
      '.lintstagedrc.mjs',
      '.prettierrc.mjs',
      'postcss.config.mjs',
    ],
  },

  // https://github.com/eslint/eslint
  {
    extends: [eslint.configs.recommended],
    rules: {
      'no-console': 'warn',
    },
  },

  // https://github.com/typescript-eslint/typescript-eslint
  {
    extends: [...tseslint.configs.strictTypeChecked, ...tseslint.configs.stylisticTypeChecked],
    plugins: {
      '@typescript-eslint': tseslint.plugin,
    },
    rules: {
      '@typescript-eslint/restrict-template-expressions': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-confusing-void-expression': 'off',
      '@typescript-eslint/no-empty-object-type': 'off',
      '@typescript-eslint/only-throw-error': 'off',
      '@typescript-eslint/no-unused-vars': [
        'warn',
        {
          argsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/consistent-type-imports': [
        'warn',
        {
          prefer: 'type-imports',
          fixStyle: 'separate-type-imports',
        },
      ],
      '@typescript-eslint/no-floating-promises': 'off',
      '@typescript-eslint/no-misused-promises': [
        'error',
        {
          checksVoidReturn: {
            attributes: false,
          },
        },
      ],
    },
  },

  // https://github.com/jsx-eslint/eslint-plugin-react
  {
    extends: [
      react.configs.flat.recommended,
      // should go after react.configs.flat.recommended
      react.configs.flat['jsx-runtime'],
    ],
    plugins: {
      react,
    },
  },
  // https://github.com/shadcn-ui/ui/issues/120
  {
    files: ['**/components/ui/*.tsx'],
    rules: {
      'react/prop-types': 'off',
      'react-refresh/only-export-components': 'off',
    },
  },

  // https://www.npmjs.com/package/eslint-plugin-react-hooks
  {
    plugins: {
      'react-hooks': reactHooks,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
    },
  },

  // https://github.com/francoismassart/eslint-plugin-tailwindcss
  {
    // extends: [...tailwindcss.configs['flat/recommended']],
  },

  // https://www.npmjs.com/package/@tanstack/eslint-plugin-query
  ...pluginQuery.configs['flat/recommended'],

  // https://github.com/eslint-community/eslint-plugin-n
  {
    plugins: {
      n: nodePlugin,
    },
    rules: {
      'n/no-process-env': 'error',
    },
  },

  // https://github.com/dukeluo/eslint-plugin-check-file
  {
    plugins: {
      'check-file': checkFile,
    },
    rules: {
      'check-file/filename-naming-convention': [
        'error',
        {
          '(!routes)/*.{ts,tsx}': 'KEBAB_CASE',
        },
        {
          // ignore the middle extensions of the filename to support filename like vite.config.ts
          ignoreMiddleExtensions: true,
        },
      ],
      'check-file/folder-naming-convention': [
        'error',
        {
          // all folders within src (except types)should be named in kebab-case
          '(!types)/**': 'KEBAB_CASE',
        },
      ],
    },
  },

  // https://github.com/ArnaudBarre/eslint-plugin-react-refresh
  {
    plugins: {
      'react-refresh': reactRefresh,
    },
    rules: {
      'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
    },
  },

  {
    settings: {
      react: {
        version: 'detect',
      },
      tailwindcss: {
        callees: ['classnames', 'cn', 'cva', 'clsx', 'meta'],
        config: join(import.meta.dirname, './tailwind.config.ts'),
      },
    },
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        project: ['./tsconfig.app.json', './tsconfig.node.json'],
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
]);
