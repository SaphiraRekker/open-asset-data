// @ts-check
import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';
import svelte from '@astrojs/svelte';

export default defineConfig({
  site: 'https://open-assets.ai',
  output: 'static',
  integrations: [tailwind(), svelte()],
});
