# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-10

First public release.

### Added

- `createPipeline({ stages })`: items move through the stages in order. A stage is an item stage (`run`) or a batch
  stage (`batch` with `runBatch`).
- Per-stage `concurrency`, `barrier` stages that wait until no item is still behind them, and `skip` per item.
- Batching by count and by weight (`batch.size`, `batch.weight`, `batch.weigh`).
- Results merge into the item: a stage returns a `Patch<T>` that is assigned onto it.
- `push` (with `startAt`), `remove`, `addStages`, `pause`, `resume`, `cancel` (aborts `ctx.signal`) and `reset`.
- Events `stage:done`, `stage:error`, `item:done`, `progress`, `status` and `done`; `progress()` and `items()` snapshots.
- A scheduler with one FIFO per stage and incremental counters: O(1) per item step, progress events coalesced per
  tick (see BENCHMARKS.md).
