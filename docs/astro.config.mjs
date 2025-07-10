// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';
import mermaid from 'astro-mermaid';

// https://astro.build/config
export default defineConfig({
  integrations: [
    mermaid(), // Must come BEFORE starlight
    starlight({
      title: '',
      logo: {
        src: './src/assets/logo.svg',
      },
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/Embucket/embucket' }],
      sidebar: [
        {
          label: 'Start Here',
          items: [
            // Each item here is one entry in the navigation menu.
            { label: 'Getting Started', link: '/' },
          ],
        },
        {
          label: 'Essentials',
          autogenerate: { directory: 'essentials' },
        },
        {
          label: 'Guides',
          autogenerate: { directory: 'guides' },
        },
        {
          label: 'Development',
          autogenerate: { directory: 'development' },
        },
      ],
      customCss: ['./src/styles/global.css'],
      components: {
        ThemeSelect: './src/components/Empty.astro',
      },
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
