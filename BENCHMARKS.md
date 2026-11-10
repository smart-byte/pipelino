# Benchmarks

Scheduler overhead measured on 2026-09-20 with `npm run bench`, bundle sizes on 2026-11-09 with
`bash bench/size.sh`, both from this repository.

|          |                                                                 |
| -------- | --------------------------------------------------------------- |
| Machine  | MacBook Pro, Apple M3 Max (12P + 4E cores), 48 GB, macOS 26.5.1 |
| Runtime  | Node v22.12.0                                                   |
| pipelino | 0.1.0 (`dist/` built with `tsc`)                                |

## Scheduler overhead

20,000 items pushed at once through 3 stages with concurrency 8 per stage. Every stage is an async
no-op (one microtask), so the numbers measure pure scheduling cost: how fast each library can move an item
from one stage to the next. Median of 7 runs after a warm-up run. The other libraries do not have a
"pipeline" primitive; the bench chains one queue per stage and re-enqueues each item on completion, which
is what you would write by hand.

| library                     | version | wall time | stage-runs / s | relative |
| --------------------------- | ------- | --------: | -------------: | -------: |
| pipelino                    | 0.1.0   |     10 ms |      5,763,827 |    1.00× |
| async.queue ×3 (chained)    | 3.2.6   |     16 ms |      3,850,288 |    1.50× |
| fastq ×3 (chained)          | 1.20.3  |     19 ms |      3,187,307 |    1.81× |
| p-limit ×3 (per item chain) | 7.3.2   |     30 ms |      1,968,001 |    2.93× |
| p-queue ×3 (chained)        | 9.3.3   |     39 ms |      1,558,350 |    3.70× |

pipelino keeps a FIFO per stage plus per-stage counters, so scheduling, barrier checks, `progress()` and
the `done` check never scan the item list. Progress events are coalesced to one per scheduler tick.

## Bundle size

Main entry bundled with esbuild (`--bundle --minify --format=esm`, dependencies included), then gzip -9.

| package  | version |      min |  min+gz |
| -------- | ------- | -------: | ------: |
| pipelino | 0.1.0   |  7,258 B | 2,575 B |
| p-limit  | 7.3.2   |  1,684 B |   859 B |
| fastq    | 1.20.3  |  2,966 B | 1,182 B |
| p-queue  | 9.3.3   | 12,244 B | 4,382 B |
| async    | 3.2.6   | 21,970 B | 8,086 B |

p-limit and fastq are smaller because they do less (a single concurrency limit / a single queue). The
pipelino number includes stages, barriers, batching, pause/cancel, per-item results and progress.

## Caveats

- One machine, one Node version, synthetic no-op stages. Real stages (network, workers) dominate wall time
  in practice; the point of this bench is that the scheduler never becomes the bottleneck.
- The chained-queue variants are fair but not the only way to use those libraries; a hand-rolled scheduler
  on top of them could close the gap.
- Run it yourself: `N=100000 ROUNDS=5 npm run bench`.
