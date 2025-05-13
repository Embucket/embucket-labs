# Embucket UI

## About

Main frontend application for the Embucket backend.

## Local development prerequisites

Before you begin, make sure you have the following installed on your machine:

- **Node.js** (LTS version) - [Download](https://nodejs.org)
- **pnpm** (Package Manager) - [Installation Guide](https://pnpm.io)

## Quick Start

Follow these steps to get the application up and running in your local development environment.

### 1. Environment variables setup

- **Setup BE .env file (`root` folder)**

#####

```bash
# Use .env.example as a reference
cp .env.example .env

# Configure CORS to match dev server
CORS_ENABLED=true
CORS_ALLOW_ORIGIN=http://localhost:5173
```

- **Setup FE .env file (`./ui` folder)**

#####

```bash
# Use .env.example as a reference
cp .env.example .env
```

### 2. FE Installation and Setup (`./ui` folder)

- **Install Dependencies**

  ```bash
  pnpm install
  ```

- **Start the Development Server**

  ```bash
  pnpm dev
  ```

### 3. Run BE server (`root` folder)

#####

```bash
cargo run
```

### 4. Verification

To ensure everything is working correctly:

- The backend server should be running on http://localhost:3000.
- The frontend development server should be running on http://localhost:5173.
- You should be able to log in using the demo credentials from your `.env` file.

## BE -> FE communication

The following diagram illustrates the fundamental communication architecture:

![diagram-export-24 09 2024-02_31_36](https://github.com/user-attachments/assets/3ddd4026-876a-4667-9c11-21f2f32760c5)

1. **BE**: Implements core backend logic and generates an OpenAPI contract for API specifications.
2. **OpenAPI Contract**: Provides API documentation derived from the backend.
3. **Codegen**: Transforms the OpenAPI contract into corresponding TypeScript types, typed API request functions and React Query hooks.
4. **React App**: The frontend application that communicates with the BE through the generated Api client.

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
- [CVA](https://cva.style/docs)
- [Radix](https://www.radix-ui.com)
- [Zod](https://zod.dev)
- [Axios](https://axios-http.com)
- [ESLint](https://eslint.org)
- [Prettier](https://prettier.io)
