import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';

const page = (name: string) => fileURLToPath(new URL(name, import.meta.url));

// Self-hosted, cookieless Umami, exactly as privacy.html describes it: it counts only on pipelino.dev, honours
// Do Not Track and drops query strings and fragments. Without a website ID no page gets the script.
const UMAMI_WEBSITE_ID = 'e93d7234-94de-4ecd-9292-23a861dc0131';
const umami = {
  defer: true,
  src: 'https://analytics.smart-byte.com/script.js',
  'data-website-id': UMAMI_WEBSITE_ID,
  'data-domains': 'pipelino.dev',
  'data-do-not-track': 'true',
  'data-exclude-search': 'true',
  'data-exclude-hash': 'true',
};

export default defineConfig({
  plugins: [{ name: 'umami', transformIndexHtml: () => (UMAMI_WEBSITE_ID ? [{ tag: 'script', attrs: umami, injectTo: 'head' }] : []) }],
  build: { rolldownOptions: { input: ['index.html', 'privacy.html', 'legal-notice.html'].map(page) } },
});
