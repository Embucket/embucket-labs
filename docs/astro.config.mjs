// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';

// https://astro.build/config
export default defineConfig({
  integrations: [
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
          label: 'Guides',
          autogenerate: { directory: 'guides' },
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
