# Embucket docs

## About

`Astro` Starlight frontend app for the Embucket docs.

## Local development prerequisites

Before you begin, make sure you have the following installed on your machine:

- **Node.js** (LTS version) - [Download](https://nodejs.org)
- **`pnpm`** (Package Manager) - [Installation Guide](https://pnpm.io)

## Quick start

Follow these steps to get the app up and running in your local development environment.

### 1. FE Installation and Setup (`./docs` folder)

- **Install Dependencies**

  ```bash
  pnpm install
  ```

- **Start the Development Server**

  ```bash
  pnpm dev
  ```

### 2. Verification

To ensure everything is working correctly:

- The frontend development server should be running on http://localhost:4321.

### Project structure

```
.
├── public/
├── src/
│   ├── assets/
│   ├── components/
│   ├── content/
│   │   ├── docs/
│   └── content.config.ts
└── astro.config.mjs
```

Starlight looks for `.md` or `.mdx` files in the `src/content/docs/` directory. Each file is exposed as a route based on its filename.

Images can be added to `src/assets/` and embedded in Markdown with a relative link.

Static assets, like `favicons`, can be placed in the `public/` directory.

## Common scripts

| Command          | Action                                             |
| :--------------- | :------------------------------------------------- |
| `pnpm install`   | Installs dependencies                              |
| `pnpm dev`       | Starts local `dev` server at `localhost:4321`      |
| `pnpm build`     | Build your production site to `./dist/`            |
| `pnpm preview`   | Preview your build locally, before deploying       |
| `pnpm astro ...` | Run `CLI` commands like `astro add`, `astro check` |
| `pnpm format`    | Format with Prettier + fix errors                  |
| `pnpm ncu`       | Update `repo` dependencies                         |

### Tech stack

- [`Astro` Starlight](https://starlight.astro.build)
- [Tailwind](https://tailwindcss.com)
- [Prettier](https://prettier.io)
- [Vale](https://vale.sh)
