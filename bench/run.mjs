// Throughput of a 3-stage pipeline (concurrency 8 per stage) over N items, pipelino vs. the usual suspects.
// Every stage is an async no-op (one microtask), so the numbers measure scheduler overhead, not work.
// Run: npm run bench   (builds dist/ first). Results are printed as a markdown table.
import { performance } from 'node:perf_hooks';
import { existsSync, readFileSync } from 'node:fs';
import os from 'node:os';

const version = (pkg) =>
  JSON.parse(readFileSync(new URL(pkg === 'pipelino' ? '../package.json' : `../node_modules/${pkg}/package.json`, import.meta.url), 'utf8')).version;
if (!existsSync(new URL('../dist/index.js', import.meta.url))) throw new Error('run `npm run build` first');
const { createPipeline } = await import('../dist/index.js');
const { default: PQueue } = await import('p-queue');
const { default: fastq } = await import('fastq');
const { default: async } = await import('async');
const { default: pLimit } = await import('p-limit');

const N = Number(process.env.N ?? 20_000);
const STAGES = 3;
const CONCURRENCY = 8;
const ROUNDS = Number(process.env.ROUNDS ?? 7);
const work = async () => {}; // one microtask

const items = () => Array.from({ length: N }, (_, i) => ({ id: String(i) }));

const contenders = {
  pipelino: () =>
    new Promise((resolve) => {
      const p = createPipeline({
        stages: Array.from({ length: STAGES }, (_, i) => ({ name: `s${i}`, concurrency: CONCURRENCY, run: work })),
      });
      p.on('done', () => resolve());
      p.push(items());
    }),

  'p-queue ×3 (chained)': () =>
    new Promise((resolve) => {
      const queues = Array.from({ length: STAGES }, () => new PQueue({ concurrency: CONCURRENCY }));
      let done = 0;
      const step = (item, i) => () =>
        work(item).then(() => {
          if (i + 1 < STAGES) queues[i + 1].add(step(item, i + 1));
          else if (++done === N) resolve();
        });
      for (const item of items()) queues[0].add(step(item, 0));
    }),

  'fastq ×3 (chained)': () =>
    new Promise((resolve) => {
      let done = 0;
      const queues = [];
      for (let i = 0; i < STAGES; i++) {
        queues.push(
          fastq.promise(async (item) => {
            await work(item);
            if (i + 1 < STAGES) queues[i + 1].push(item);
            else if (++done === N) resolve();
          }, CONCURRENCY),
        );
      }
      for (const item of items()) queues[0].push(item);
    }),

  'async.queue ×3 (chained)': () =>
    new Promise((resolve) => {
      let done = 0;
      const queues = [];
      for (let i = 0; i < STAGES; i++) {
        queues.push(
          async.queue(async (item) => {
            await work(item);
            if (i + 1 < STAGES) queues[i + 1].push(item);
            else if (++done === N) resolve();
          }, CONCURRENCY),
        );
      }
      for (const item of items()) queues[0].push(item);
    }),

  'p-limit ×3 (per item chain)': async () => {
    const limits = Array.from({ length: STAGES }, () => pLimit(CONCURRENCY));
    await Promise.all(
      items().map(async (item) => {
        for (const limit of limits) await limit(() => work(item));
      }),
    );
  },
};

const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const rows = [];
for (const [name, run] of Object.entries(contenders)) {
  await run(); // warm-up
  const times = [];
  for (let r = 0; r < ROUNDS; r++) {
    const t0 = performance.now();
    await run();
    times.push(performance.now() - t0);
  }
  const ms = median(times);
  rows.push({ name, ms, rate: (N * STAGES) / (ms / 1000), version: version(name.split(/[ .]/)[0]) });
  console.error(`${name.padEnd(30)} ${ms.toFixed(1).padStart(8)} ms`);
}
const base = rows[0].ms;
console.log(`\n${N.toLocaleString()} items × ${STAGES} stages, concurrency ${CONCURRENCY}, async no-op stages, median of ${ROUNDS} runs.`);
console.log(`Node ${process.version}, ${os.cpus()[0]?.model ?? 'unknown CPU'}, ${os.platform()} ${os.release()}.\n`);
console.log('| library | version | wall time | stage-runs / s | relative |');
console.log('|---|---|---:|---:|---:|');
for (const r of rows)
  console.log(`| ${r.name} | ${r.version} | ${r.ms.toFixed(0)} ms | ${Math.round(r.rate).toLocaleString()} | ${(r.ms / base).toFixed(2)}× |`);
