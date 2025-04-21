# Embucket

## About

The following diagram illustrates the fundamental communication architecture:

![diagram-export-24 09 2024-02_31_36](https://github.com/user-attachments/assets/3ddd4026-876a-4667-9c11-21f2f32760c5)

1. **Rust Backend**: Implements core backend logic and generates an OpenAPI contract for API specifications.
2. **OpenAPI Contract**: Provides API documentation derived from the Rust backend.
3. **Codegen**: Transforms the OpenAPI contract into corresponding TypeScript types, typed Api request functions and React Query hooks.
4. **React App**: The frontend application that communicates with the Rust backend through the generated Api client.

### Project structure (main files and folders)

```text
.vscode                                         # Recommended VSCode extensions and settings
docs                                            # Additional repo documentation
.env                                            # Project environment variables
env.ts                                          # Environment variables zod validation
src
  ├─ app                                        # Defines top-level app configuration, including main entry, i18n, providers, routers, global styles, type declarations and app layout components.
  │   ├─ i18n                                   # Internationalization files
  |   |   └─ locales                            # Locales folder
  |   |       └─ en
  |   |           ├─ components.ts              # Shared components translations
  |   |           ├─ auth.ts
  |   |           └─ ...
  │   ├─ layout                                 # Main layout components (header, content, sidebar, etc.)
  │   ├─ providers                              # Global app providers
  │   ├─ routes                                 # TanStack file-based routing (page entries)
  │   ├─ types                                  # Global type declarations
  │   ├─ global.css                             # Global styles
  │   ├─ main.tsx                               # App entry file
  │   └─ ...
  ├─ components                                 # Shared components (used in multiple consumers)
  │   ├─ ui                                     # Shadcn UI components
  │   └─ ...
  ├─ hooks                                      # Shared hooks (used in multiple consumers)
  ├─ lib                                        # Various lib instances and utility functions
  ├─ orval                                      # Orval generated files (Types, API client, React Query hooks)
  ├─ modules                                    # Modules folder includes all pages and their related components
  │   ├─ sql-editor
  │   └─ ...
  └─ ...
```

## Prerequisites

Make sure you have the following installed on your machine:

1. [Node.js LTS](https://nodejs.org)
2. [pnpm](https://pnpm.io)

## Quick Start

You suppose to run all the commands from the root folder.

### 1. Create `.env` file and fill it with the required environment variables

```bash
# Use `.env.example` as a reference.
cp .env.example .env
```

### 2. Setup dependencies

```bash
pnpm i
```

### 3. Start the development server

```bash
pnpm dev
```

### 4. Open the app in your browser

```bash
open http://localhost:3000
```

## Common scripts

| Script      | Description                               |
| ----------- | ----------------------------------------- |
| `dev`       | Run the development server                |
| `codegen`   | Run codegen                               |
| `lint`      | Lint with ESLint + fix errors             |
| `format`    | Format with Prettier + fix errors         |
| `typecheck` | Run tcs                                   |
| `build`     | Run the build outputting to `dist` folder |
| `ncu`       | Update repo dependencies                  |

### Tech stack

- [TypeScript](https://www.typescriptlang.org)
- [React](https://react.dev)
- [Vite](https://vitejs.dev)
- [Tailwind](https://tailwindcss.com)
- [TanStack Query](https://tanstack.com/query)
- [TanStack Router](https://tanstack.com/router)
- [TanStack Table](https://tanstack.com/table)
- [Orval](https://orval.dev)
- [React-hook-form](https://react-hook-form.com)
- [React-i18next](https://react.i18next.com)
- [Shadcn UI](https://ui.shadcn.com)
- [CVA](https://cva.style/docs)
- [Radix](https://www.radix-ui.com)
- [Zod](https://zod.dev)
- [Axios](https://axios-http.com)
- [ESLint](https://eslint.org)
- [Prettier](https://prettier.io)
