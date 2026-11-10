# pipelino

A staged pipeline for async work in the browser and Node: per-stage concurrency, batching, pause and cancel, live
progress. Zero dependencies, 2.6 KB gzipped.

[![npm](https://img.shields.io/npm/v/pipelino)](https://www.npmjs.com/package/pipelino)
[![CI](https://github.com/smart-byte/pipelino/actions/workflows/ci.yml/badge.svg)](https://github.com/smart-byte/pipelino/actions/workflows/ci.yml)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**[Live demo at pipelino.dev](https://pipelino.dev)**

```bash
npm install pipelino
```

```ts
import { createPipeline } from 'pipelino';

const pipeline = createPipeline<Photo>({
  stages: [
    { name: 'inspect', concurrency: 8, run: async (photo) => ({ width: await readWidth(photo.file) }) },
    { name: 'thumbnail', concurrency: 4, run: async (photo) => ({ thumb: await makeThumb(photo.file) }) },
    {
      name: 'upload',
      barrier: true, // start only once every photo has its thumbnail
      batch: { size: 10 },
      runBatch: (photos, { signal }) => upload(photos, signal),
    },
  ],
});

pipeline.on('progress', ({ overall }) => render(overall.percent));
pipeline.on('done', ({ done, failed }) => console.log(`${done.length} ok, ${failed.length} failed`));

pipeline.push(photos); // any objects with a unique string `id`
```

- **Stages, not queues.** One object describes the whole flow. Items move from stage to stage on their own.
- **Results merge into the item.** Return an object from a stage and its fields are assigned to the item: no second
  data structure to keep in sync.
- **Streaming or barrier.** An item enters the next stage the moment it leaves the previous one, unless that stage
  is a `barrier`, which waits until no item is still behind it.
- **Batching by count and weight.** `batch: { size, weight, weigh }` groups ready items, for example by bytes. A
  batch never waits to fill up.
- **Control while it runs.** Pause, resume, cancel with an `AbortSignal`, remove items, push more, or append stages.
- **Never the bottleneck.** 20,000 items through 3 stages in 10 ms of scheduler time; see
  [BENCHMARKS.md](BENCHMARKS.md).

## Usage

```ts
import { createPipeline } from 'pipelino';

interface Item {
  id: string;
  file: File;
  width?: number;
  thumb?: Blob;
  uploaded?: boolean;
}

const pipeline = createPipeline<Item>({
  stages: [
    { name: 'inspect', concurrency: 8, run: async (item) => ({ width: (await dims(item.file)).width }) },
    {
      name: 'thumbnail',
      concurrency: 8,
      skip: (item) => item.width! < 512, // small images pass through without a thumbnail
      run: async (item) => ({ thumb: await makeThumb(item.file) }),
    },
    {
      name: 'upload',
      barrier: true,
      concurrency: 3,
      batch: { size: 10, weight: 5e6, weigh: (item) => (item.thumb ?? item.file).size }, // at most 10 items or 5 MB
      runBatch: async (items, { signal }) => {
        await fetch('/upload', { method: 'POST', body: form(items), signal });
        return items.map(() => ({ uploaded: true })); // one patch per item, matched by index
      },
    },
  ],
});

pipeline.on('stage:error', ({ stage, item, error }) => console.warn(stage, item.id, error));

pipeline.push(files.map((file) => ({ id: crypto.randomUUID(), file })));

pipeline.pause(); // running work finishes, nothing new starts
pipeline.resume();
pipeline.cancel(); // aborts the signal, drops queued and running items, keeps finished ones
```

## API

### `createPipeline<T extends { id: string }>({ stages })`

Returns a `Pipeline<T>`. It needs at least one stage; a stage is an item stage (`run`) or a batch stage (`batch` with
`runBatch`).

| stage field   | type                                             | default  |                                                                           |
| ------------- | ------------------------------------------------ | -------- | ------------------------------------------------------------------------- |
| `name`        | `string`                                         | required | unique within the pipeline                                                |
| `run`         | `(item, ctx) => Patch<T> \| void` (or a promise) |          | item stage: one item per run                                              |
| `batch`       | `{ size, weight?, weigh? }`                      |          | batch stage: at most `size` items and `weight` summed `weigh(item)` a run |
| `runBatch`    | `(items, ctx) => (Patch<T> \| void)[] \| void`   |          | required with `batch`; results are matched to items by index              |
| `concurrency` | `number`                                         | `4`      | concurrent runs of this stage: a whole number, or `Infinity`              |
| `barrier`     | `boolean`                                        | `false`  | wait until no live item is still behind this stage                        |
| `skip`        | `(item) => boolean`                              |          | `true` passes the item through without running the stage                  |

`ctx` is `{ signal: AbortSignal, stage: string }`. `Patch<T>` is `Partial<Omit<T, 'id'>>`.

### `Pipeline<T>`

| member                                 |                                                                                              |
| -------------------------------------- | -------------------------------------------------------------------------------------------- |
| `push(items, { startAt? })` → `number` | queues one item or an array and returns how many were added; `startAt` names the entry stage |
| `remove(ids)` → `number`               | drops queued, running or finished items and returns how many were dropped                    |
| `addStages(stages)`                    | appends stages; finished items re-enter at the first new one, failed items stay failed       |
| `pause()` / `resume()`                 | stops and continues scheduling; running work finishes                                        |
| `cancel()`                             | aborts `ctx.signal`, drops queued and running items, keeps finished ones; no `done` event    |
| `reset()`                              | cancels and forgets every item                                                               |
| `progress()` → `Progress`              | `{ overall, stages: { [name]: { done, failed, running, total, percent } } }`, always current |
| `items()` → `ItemSnapshot<T>[]`        | `{ item, state, stage, error? }` per item, in push order                                     |
| `status`                               | `'idle'`, `'running'` or `'paused'`                                                          |
| `stages`                               | the stages, read-only                                                                        |
| `on(event, listener)` → unsubscribe    | see the events below                                                                         |

### Events

| event         | payload                  | fires when                                                               |
| ------------- | ------------------------ | ------------------------------------------------------------------------ |
| `stage:done`  | `{ stage, item }`        | an item finished a stage it ran (not for a skipped stage)                |
| `stage:error` | `{ stage, item, error }` | a stage failed an item                                                   |
| `item:done`   | `{ item }`               | an item finished the last stage                                          |
| `progress`    | `Progress`               | counts changed; coalesced per scheduler tick                             |
| `status`      | `Status`                 | the status changed                                                       |
| `done`        | `{ done, failed }`       | every live item is done or failed; fires again after later pushes settle |

`createPipeline` is the only runtime export. Types: `Pipeline`, `Stage`, `ItemStage`, `BatchStage`, `BatchOptions`,
`Patch`, `StageContext`, `Progress`, `StageProgress`, `ItemSnapshot`, `ItemState`, `Status`, `PipelineEvents`,
`PipelineOptions`, `PushOptions`.

### Rules

- **Order.** Items enter the first stage in push order and later stages in the order they left the previous one.
- **Failures.** A stage that throws marks the item `failed`: it skips the remaining stages, `stage:error` fires and the
  item is listed in `done.failed`. Other items are unaffected. A throwing `runBatch` fails every item of that batch. A
  `skip` or `weigh` that throws fails that item, and so does a `weigh` that returns anything but a number ≥ 0.
- **Results.** The returned object is `Object.assign`ed onto the item; `id` is never changed. Return an object or
  nothing: a string, an array or a number fails the item, and so does a patch that cannot be applied (a frozen item).
- **Ids.** Every item needs an `id`; `push` throws for one without. An id that is queued or running is ignored. A
  finished or failed id runs again from the start and moves to the end of `items()`.
- **Batches.** An item heavier than `weight` runs alone. `skip` and `weigh` are called when an item is picked up, so
  they run again for an item that did not fit the previous batch.
- **Pause.** If the work that was running leaves nothing queued, the pipeline goes `idle` and `done` fires without a
  `resume()`.
- **Cancel.** Results of runs that were in flight are discarded. Those runs keep their concurrency slot until they
  settle, so the limit holds across a cancel: pass `ctx.signal` to `fetch` and workers so they settle quickly.
- **Listeners.** A listener that throws is reported through `reportError()` in browsers, Bun and Deno; the other
  listeners still run. Node has no `reportError()`: there it is an uncaught exception, which ends the process unless
  you handle `uncaughtException`.
- **Progress.** Under load `progress` still fires roughly once per finished run; throttle with
  `requestAnimationFrame` before painting.

## Requirements

ESM only, compiled to ES2022: current browsers (Chrome 94+, Firefox 93+, Safari 16.4+), Node 18 or later, Deno and
Bun. The types use the global `AbortSignal`, so a TypeScript project needs the DOM lib or `@types/node`.

## Compared with

The libraries below are queues or limiters. A pipeline can be built on any of them; pipelino is that pipeline, with
the item bookkeeping, batching and progress written and tested.

|                                        | pipelino |        p-queue         |   fastq   | async.queue / cargo |     p-limit      |     RxJS      |
| -------------------------------------- | :------: | :--------------------: | :-------: | :-----------------: | :--------------: | :-----------: |
| several stages in one object           |    ✅    |           —            |     —     |          —          |        —         | via operators |
| barrier stages                         |    ✅    |           —            |     —     |          —          |        —         |    manual     |
| batching by count                      |    ✅    |           —            |     —     |     ✅ `cargo`      |        —         | `bufferCount` |
| batching by weight (bytes)             |    ✅    |           —            |     —     |          —          |        —         |    manual     |
| concurrency per stage                  |    ✅    |       per queue        | per queue |      per queue      |    per limit     |  `mergeMap`   |
| pause and resume                       |    ✅    |           ✅           |    ✅     |         ✅          |        —         |    manual     |
| cancel with an `AbortSignal`           |    ✅    | `clear()` + own signal | `kill()`  |      `kill()`       |        —         |  unsubscribe  |
| skip, enter mid-flow, add stages later |    ✅    |           —            |     —     |          —          |        —         |       —       |
| results merged into items              |    ✅    |    promise per task    | callback  |      callback       | promise per task |    stream     |
| progress per stage and overall         |    ✅    |        counters        | counters  |      counters       |     counters     |    manual     |
| runtime dependencies                   |    0     |           2            |     1     |          0          |        1         |       1       |
| min + gzip                             |  2.6 KB  |         4.4 KB         |  1.2 KB   | 8.1 KB (whole lib)  |      0.9 KB      |    ~18 KB     |

p-queue 9.3.3, fastq 1.20.3, async 3.2.6, p-limit 7.3.2; sizes from `bash bench/size.sh` (esbuild bundle with
dependencies, gzip -9). The RxJS size is bundlephobia's for a typical set of operators.

## Benchmark

20,000 items through 3 stages, concurrency 8, no-op stages, Node 22 on an Apple M3 Max:

| library        | wall time | stage runs / s |
| -------------- | --------: | -------------: |
| **pipelino**   | **10 ms** |      **5.8 M** |
| async.queue ×3 |     16 ms |          3.9 M |
| fastq ×3       |     19 ms |          3.2 M |
| p-limit ×3     |     30 ms |          2.0 M |
| p-queue ×3     |     39 ms |          1.6 M |

Setup, caveats and how to reproduce: [BENCHMARKS.md](BENCHMARKS.md).

## Develop

Setup and tests are in [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MIT](LICENSE).
