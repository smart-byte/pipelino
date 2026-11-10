# Contributing

Thanks for helping out. pipelino is small on purpose; the bar for a change is "makes the pipeline
easier to use or more correct without adding a dependency".

## Setup

```bash
bun install          # or npm install; the dev tools need Node >= 22.12 (see .nvmrc)
npm test             # vitest
npm run type-check   # tsc --noEmit
npm run lint         # prettier --check; `npm run format` fixes
npm run build        # tsc -> dist/
npm run bench        # bench/run.mjs against p-queue, fastq, async and p-limit
bash bench/size.sh   # bundle sizes
npm run site         # landing page with the live demo on http://localhost:4320
npm run site:build   # bundle the landing page into site-dist/
```

## Pull requests

- One change per PR. Add or update a test in `src/pipeline.spec.ts` for every behavioural change.
- Keep the public API in `src/index.ts` documented in `README.md`.
- No runtime dependencies. If you think one is needed, open an issue first.
- Run `npm run lint && npm run type-check && npm test` before pushing; CI runs the same on Node 22, 24 and 26,
  and runs the build (`node bench/smoke.mjs`) on Node 18 and 20.

## Reporting bugs

Open an issue with a minimal `createPipeline` snippet that reproduces it. Stage definitions plus the
sequence of `push` / `pause` / `cancel` calls is usually enough.
