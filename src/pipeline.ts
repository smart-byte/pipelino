import { Emitter } from './emitter.js';

/** 'idle': nothing queued or running. 'paused': `pause()` was called and work is left. */
export type Status = 'idle' | 'running' | 'paused';

export interface StageContext {
  /** Aborted by `pipeline.cancel()`. Pass it to fetch/workers. */
  signal: AbortSignal;
  stage: string;
}

interface StageBase<T> {
  name: string;
  /** Max concurrent runs of this stage (a batch counts as one run): a whole number >= 1, or Infinity. Default 4. */
  concurrency?: number;
  /** Start only once no live item is still behind this stage. Default false = streaming. */
  barrier?: boolean;
  /** Return true to pass the item through without running the stage. A throwing `skip` fails that item.
   *  Evaluated when the item is picked up, so it runs again for an item that did not fit the previous run. */
  skip?: (item: T) => boolean;
}

/** What a stage may return: fields to merge into the item. The id is never patchable. Any other value that is not
 *  nothing (a string, an array, a number) fails the item instead of being merged. */
export type Patch<T> = Partial<Omit<T, 'id'>>;

export interface ItemStage<T> extends StageBase<T> {
  batch?: undefined;
  runBatch?: undefined;
  run: (item: T, ctx: StageContext) => Promise<Patch<T> | void> | Patch<T> | void;
}

export interface BatchOptions<T> {
  /** Max items per run: a whole number >= 1, or Infinity. */
  size: number;
  /** Max summed weight per run (e.g. bytes), > 0. Requires `weigh`. */
  weight?: number;
  /** Weight of one item, >= 0. A `weigh` that throws or returns anything else fails that item. Runs again for an item
   *  that did not fit the previous run. */
  weigh?: (item: T) => number;
}

export interface BatchStage<T> extends StageBase<T> {
  batch: BatchOptions<T>;
  run?: undefined;
  /** Results (if returned) are matched to items by index. */
  runBatch: (items: T[], ctx: StageContext) => Promise<(Patch<T> | void)[] | void> | (Patch<T> | void)[] | void;
}

export type Stage<T> = ItemStage<T> | BatchStage<T>;

export interface StageProgress {
  /** Items that are past this stage. Overall: items that finished every stage. */
  done: number;
  /** Items that failed at this stage or an earlier one. Overall: all failed items. */
  failed: number;
  /** Items running in this stage right now. */
  running: number;
  /** All live items: queued, running, done and failed. */
  total: number;
  /** 0-100, done + failed over total. */
  percent: number;
}

export interface Progress {
  overall: StageProgress;
  /** Keyed by stage name. */
  stages: Record<string, StageProgress>;
}

export type ItemState = 'queued' | 'running' | 'done' | 'failed';

export interface ItemSnapshot<T> {
  item: T;
  state: ItemState;
  /** Stage the item is at / waiting for; null once done. */
  stage: string | null;
  /** What the stage threw, for a failed item. */
  error?: unknown;
}

export type PipelineEvents<T> = {
  /** An item finished a stage it ran. Not fired for a stage the item skipped. */
  'stage:done': { stage: string; item: T };
  /** A stage failed an item; the item skips the remaining stages. */
  'stage:error': { stage: string; item: T; error: unknown };
  /** An item finished the last stage. */
  'item:done': { item: T };
  /** Coalesced: everything that changes while a scheduler tick is pending produces one event. */
  progress: Progress;
  status: Status;
  /** Every live item is done or failed. Fires again after later pushes settle. */
  done: { done: T[]; failed: T[] };
};

export interface PipelineOptions<T> {
  stages: Stage<T>[];
}

export interface PushOptions {
  /** Enter items at this stage (earlier stages count as already done). */
  startAt?: string;
}

interface Entry<T> {
  item: T;
  /** Index of the stage the item is at (queued for or running); `stages.length` once done. */
  stageIndex: number;
  state: ItemState;
  error?: unknown;
}

const DEFAULT_CONCURRENCY = 4;

export class Pipeline<T extends { id: string }> extends Emitter<PipelineEvents<T>> {
  /** The stages in order, including those added with `addStages()`. */
  readonly stages: readonly Stage<T>[] = [];
  private _status: Status = 'idle';

  get status(): Status {
    return this._status;
  }

  /** Insertion order = FIFO order for `items()` and `done`. */
  private entries = new Map<string, Entry<T>>();
  /** Per-stage FIFO of entries waiting to run there. Entries are validated lazily (see `take`). */
  private queues: Entry<T>[][] = [];
  private heads: number[] = [];
  /** Runs (not items) in flight per stage. */
  private running: number[] = [];
  // Live counters so progress(), barrier checks and settle() never scan all entries.
  // Index = stage; index `stages.length` = finished.
  private atCount: number[] = [0];
  private failedAt: number[] = [0];
  private runningAt: number[] = [0];
  private abort = new AbortController();
  private epoch = 0;
  /** Counts `drop()` calls, so a run being assembled can tell that a callback removed entries (see `take`). */
  private drops = 0;
  private scheduled = false;
  private dirty = false;
  private progressDirty = false;

  constructor(opts: PipelineOptions<T>) {
    super();
    if (!opts.stages.length) throw new Error('pipelino: needs at least one stage');
    this.appendStages(opts.stages);
  }

  /** Queue items. Items whose id is already queued/running are ignored. Returns how many were added. */
  push(items: T | T[], opts: PushOptions = {}): number {
    const list = Array.isArray(items) ? items : [items];
    // Items are told apart by id alone: without one they would silently replace each other.
    if (list.some((item) => item?.id == null)) throw new TypeError('pipelino: every item needs an id');
    const at = opts.startAt ? this.indexOf(opts.startAt) : 0;
    let added = 0;
    for (const item of list) {
      const cur = this.entries.get(item.id);
      if (cur && (cur.state === 'queued' || cur.state === 'running')) continue;
      if (cur) this.drop(cur); // re-pushed items go to the back of the queue
      const entry: Entry<T> = { item, stageIndex: at, state: 'queued' };
      this.entries.set(item.id, entry);
      this.track(entry, 1);
      this.queues[at]!.push(entry);
      added++;
    }
    if (added) {
      this.dirty = true;
      if (this.status === 'idle') this.setStatus('running');
      this.emitProgress();
      this.schedule();
    }
    return added;
  }

  /** Drop items (queued, running or finished). A running item's result is discarded. Returns how many were dropped. */
  remove(ids: string | string[]): number {
    let removed = 0;
    for (const id of Array.isArray(ids) ? ids : [ids]) {
      const entry = this.entries.get(id);
      if (!entry) continue;
      this.drop(entry);
      removed++;
    }
    if (removed) {
      this.emitProgress();
      this.schedule();
    }
    return removed;
  }

  /** Append stages to a running or finished pipeline. Items that were done re-enter at the first new stage. */
  addStages(stages: Stage<T>[]): void {
    if (!stages.length) return;
    const first = this.stages.length;
    this.appendStages(stages); // validates every stage before anything changes
    // Finished items sit at index `first` (the old "finished" slot), which is now the first new stage: same
    // index, so the counters already agree; only the state changes.
    let requeued = 0;
    for (const e of this.entries.values()) {
      if (e.state !== 'done') continue;
      e.state = 'queued';
      this.queues[first]!.push(e);
      requeued++;
    }
    if (requeued) {
      this.dirty = true;
      if (this.status === 'idle') this.setStatus('running');
    }
    this.emitProgress();
    this.schedule();
  }

  /** Stop starting new runs. Running work finishes; if that leaves nothing to do, the pipeline goes idle and `done` fires. */
  pause(): void {
    if (this.status === 'running') this.setStatus('paused');
  }

  /** Continue after `pause()`. */
  resume(): void {
    if (this.status !== 'paused') return;
    this.setStatus('running');
    this.schedule();
  }

  /** Abort `ctx.signal`, drop queued + running items. Finished items are kept; no `done` event fires. */
  cancel(): void {
    this.abort.abort();
    this.abort = new AbortController();
    this.epoch++;
    for (const e of this.entries.values()) if (e.state === 'queued' || e.state === 'running') this.drop(e);
    // Runs of the old epoch are still executing until they settle: they keep their concurrency slot until then (so
    // the limit holds for a stage that ignores the signal) and their results are discarded (see exec).
    this.queues = this.stages.map(() => []);
    this.heads = this.stages.map(() => 0);
    this.dirty = false;
    this.setStatus('idle');
    this.emitProgress();
  }

  /** Cancel and forget all items. */
  reset(): void {
    for (const e of this.entries.values()) this.drop(e);
    this.cancel();
  }

  /** A snapshot per live item, in push order (a re-pushed item moves to the end). */
  items(): ItemSnapshot<T>[] {
    return [...this.entries.values()].map((e) => ({
      item: e.item,
      state: e.state,
      stage: e.state === 'done' ? null : this.stages[e.stageIndex]!.name,
      error: e.error,
    }));
  }

  /** Counts per stage and overall. Always current, also between `progress` events. */
  progress(): Progress {
    const total = this.entries.size;
    const n = this.stages.length;
    const stages: Record<string, StageProgress> = {};
    // Walk the stages back to front: `beyond` = items past stage i (done, or failed at a later stage),
    // `failed` = items that failed at stage i or earlier. Both come from the counters, no item scan.
    const failedTotal = this.failedAt.reduce((a, b) => a + b, 0);
    let beyond = this.atCount[n]!;
    let failed = failedTotal;
    for (let i = n - 1; i >= 0; i--) {
      const stage = this.stages[i]!;
      stages[stage.name] = { done: beyond, failed, running: this.runningAt[i]!, total, percent: pct(beyond + failed, total) };
      beyond += this.atCount[i]!;
      failed -= this.failedAt[i]!;
    }
    const done = this.atCount[n]!;
    const running = this.runningAt.reduce((a, b) => a + b, 0);
    return { overall: { done, failed: failedTotal, running, total, percent: pct(done + failedTotal, total) }, stages };
  }

  private appendStages(stages: Stage<T>[]): void {
    const names = new Set(this.stages.map((s) => s.name));
    for (const s of stages) {
      if (!s.name) throw new Error('pipelino: stage needs a name');
      if (s.name === '__proto__') throw new Error('pipelino: a stage cannot be named "__proto__"'); // not a key progress().stages can hold
      if (names.has(s.name)) throw new Error(`pipelino: duplicate stage "${s.name}"`);
      names.add(s.name);
      if (s.concurrency != null && !isCount(s.concurrency)) throw new Error(`pipelino: stage "${s.name}" concurrency must be a whole number >= 1`);
      if (s.batch && !isCount(s.batch.size)) throw new Error(`pipelino: stage "${s.name}" batch.size must be a whole number >= 1`);
      if (s.batch?.weight != null && !(s.batch.weight > 0)) throw new Error(`pipelino: stage "${s.name}" batch.weight must be > 0`);
      if (s.batch?.weight != null && !s.batch.weigh) throw new Error(`pipelino: stage "${s.name}" batch.weight requires batch.weigh`);
      if (s.batch ? typeof s.runBatch !== 'function' : typeof s.run !== 'function') {
        throw new Error(`pipelino: stage "${s.name}" needs ${s.batch ? 'runBatch' : 'run'}()`);
      }
    }
    for (const s of stages) {
      (this.stages as Stage<T>[]).push(s);
      this.running.push(0);
      this.queues.push([]);
      this.heads.push(0);
      // New "finished" slot; the old one becomes this stage's slot (see addStages).
      this.atCount.push(0);
      this.failedAt.push(0);
      this.runningAt.push(0);
    }
  }

  private indexOf(name: string): number {
    const i = this.stages.findIndex((s) => s.name === name);
    if (i < 0) throw new Error(`pipelino: unknown stage "${name}"`);
    return i;
  }

  /** Add (+1) or remove (-1) an entry's current (stageIndex, state) from the counters. */
  private track(e: Entry<T>, delta: 1 | -1): void {
    this.atCount[e.stageIndex]! += delta;
    if (e.state === 'failed') this.failedAt[e.stageIndex]! += delta;
    else if (e.state === 'running') this.runningAt[e.stageIndex]! += delta;
  }

  /** Change an entry's (stageIndex, state) keeping the counters in sync. */
  private move(e: Entry<T>, stageIndex: number, state: ItemState): void {
    this.track(e, -1);
    e.stageIndex = stageIndex;
    e.state = state;
    this.track(e, 1);
  }

  /** Forget an entry. Queue slots are left behind and skipped by `take`. */
  private drop(e: Entry<T>): void {
    if (this.entries.get(e.item.id) !== e) return;
    this.entries.delete(e.item.id);
    this.track(e, -1);
    this.drops++;
  }

  private setStatus(status: Status): void {
    if (this._status === status) return;
    this._status = status;
    this.emit('status', status);
  }

  /** Progress events are coalesced: at most one per scheduler tick. */
  private emitProgress(): void {
    this.progressDirty = true;
    if (!this.scheduled) this.flushProgress();
  }

  private flushProgress(): void {
    if (!this.progressDirty) return;
    this.progressDirty = false;
    if (this.has('progress')) this.emit('progress', this.progress());
  }

  private schedule(): void {
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => this.tick());
  }

  private tick(): void {
    this.scheduled = false;
    if (this.status === 'running') {
      for (let i = 0; i < this.stages.length; i++) {
        const stage = this.stages[i]!;
        if (stage.barrier && this.anyBehind(i)) continue;
        const limit = stage.concurrency ?? DEFAULT_CONCURRENCY;
        while (this.running[i]! < limit) {
          const ready = this.take(i, stage);
          if (!ready.length) break;
          this.running[i]!++;
          void this.exec(stage, i, ready);
        }
      }
    }
    this.settle();
    this.flushProgress();
  }

  private anyBehind(i: number): boolean {
    for (let k = 0; k < i; k++) if (this.atCount[k]! - this.failedAt[k]! > 0) return true;
    return false;
  }

  /** Pull the next run's worth of queued items for stage i, passing skipped items straight through. */
  private take(i: number, stage: Stage<T>): Entry<T>[] {
    const out: Entry<T>[] = [];
    const queue = this.queues[i]!;
    const max = stage.batch?.size ?? 1;
    const maxWeight = stage.batch?.weight ?? Infinity;
    const weigh = stage.batch?.weigh ?? (() => 0);
    const drops = this.drops;
    let weight = 0;
    let head = this.heads[i]!;
    for (; head < queue.length; head++) {
      const e = queue[head]!;
      if (e.state !== 'queued' || e.stageIndex !== i || this.entries.get(e.item.id) !== e) continue; // stale slot
      let skipped = false;
      let w = 0;
      let failure: { error: unknown } | undefined;
      try {
        skipped = !!stage.skip?.(e.item);
        if (!skipped) {
          w = weigh(e.item);
          // NaN would switch the weight cap off for the rest of the run, a negative weight would make room that is not there.
          if (!(w >= 0)) throw new TypeError(`pipelino: stage "${stage.name}" weigh() must return a number >= 0, got ${w}`);
        }
      } catch (error) {
        failure = { error };
      }
      if (this.entries.get(e.item.id) !== e) continue; // skip() or weigh() removed or cancelled this very item
      if (failure) {
        this.fail(e, stage.name, failure.error);
        continue;
      }
      if (skipped) {
        this.advance(e);
        continue;
      }
      if (out.length && weight + w > maxWeight) break; // leave it for the next run
      out.push(e);
      weight += w;
      if (out.length >= max || weight >= maxWeight) {
        head++;
        break;
      }
    }
    if (head * 2 >= queue.length) {
      queue.splice(0, head); // consumed slots never outnumber live ones
      head = 0;
    }
    this.heads[i] = head;
    // skip(), weigh() and the listeners fired above (stage:done, item:done, stage:error) may have removed or cancelled
    // entries that are already in `out`.
    return this.drops === drops ? out : out.filter((e) => this.entries.get(e.item.id) === e);
  }

  private async exec(stage: Stage<T>, i: number, batch: Entry<T>[]): Promise<void> {
    const epoch = this.epoch;
    const ctx: StageContext = { signal: this.abort.signal, stage: stage.name };
    for (const e of batch) this.move(e, i, 'running');
    let results: (Patch<T> | void)[] | void;
    let error: unknown;
    let failed = false;
    try {
      results = stage.batch
        ? await stage.runBatch(
            batch.map((e) => e.item),
            ctx,
          )
        : [await stage.run(batch[0]!.item, ctx)];
    } catch (e) {
      failed = true;
      error = e;
    }
    this.running[i]!--;
    if (epoch !== this.epoch) return this.schedule(); // cancelled while running: the items are gone, only the slot comes back
    batch.forEach((e, k) => {
      if (this.entries.get(e.item.id) !== e) return; // removed while running
      if (failed) return this.fail(e, stage.name, error);
      const patch: unknown = results?.[k];
      if (patch) {
        try {
          // Object.assign would spread a string or an array over the item index by index.
          if (typeof patch !== 'object' || Array.isArray(patch)) {
            const what = Array.isArray(patch) ? 'an array' : `a ${typeof patch}`;
            throw new TypeError(`pipelino: stage "${stage.name}" returned ${what}; return an object to merge into the item, or nothing`);
          }
          const id = e.item.id;
          Object.assign(e.item, patch);
          if (e.item.id !== id) e.item.id = id; // a patch cannot rename the item (see Patch<T>)
        } catch (err) {
          return this.fail(e, stage.name, err); // frozen item, throwing setter, ...
        }
      }
      this.advance(e, stage.name);
    });
    this.emitProgress();
    this.schedule();
  }

  private fail(e: Entry<T>, stage: string, error: unknown): void {
    e.error = error;
    this.move(e, e.stageIndex, 'failed');
    this.emit('stage:error', { stage, item: e.item, error });
  }

  /** Move an entry to the next stage. `stage` is the name of the stage it just ran (absent when skipped). */
  private advance(e: Entry<T>, stage?: string): void {
    const next = e.stageIndex + 1;
    this.move(e, next, next >= this.stages.length ? 'done' : 'queued');
    if (e.state === 'queued') this.queues[next]!.push(e);
    if (stage) this.emit('stage:done', { stage, item: e.item });
    if (e.state === 'done') this.emit('item:done', { item: e.item });
  }

  private settle(): void {
    if (this.anyBehind(this.stages.length)) return;
    this.setStatus('idle');
    if (!this.dirty) return;
    this.dirty = false;
    const done: T[] = [];
    const failed: T[] = [];
    for (const e of this.entries.values()) (e.state === 'failed' ? failed : done).push(e.item);
    this.emit('done', { done, failed });
  }
}

/** Creates a pipeline; nothing runs until items are pushed. Throws for an empty or invalid stage list. */
export function createPipeline<T extends { id: string }>(opts: PipelineOptions<T>): Pipeline<T> {
  return new Pipeline<T>(opts);
}

function pct(n: number, total: number): number {
  return total ? Math.round((n / total) * 100) : 0;
}

/** A whole number >= 1, or Infinity for "no limit": a fraction would quietly round up to one run or item more. */
function isCount(n: number): boolean {
  return n >= 1 && (Number.isInteger(n) || n === Infinity);
}
