import { describe, expect, it, vi } from 'vitest';
import { createPipeline, type ItemStage, type Pipeline, type Stage } from './pipeline.js';

interface Item {
  id: string;
  bytes?: number;
  log?: string[];
  [k: string]: unknown;
}

function defer<T = void>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

// The scheduler runs on microtasks, so one macrotask turn drains it completely; a few turns also cover stages that
// await a macrotask themselves. Deterministic, not a timing guess.
const tick = () => new Promise((r) => setTimeout(r, 0));
const settle = async (n = 5) => {
  for (let i = 0; i < n; i++) await tick();
};

const items = (n: number, bytes = 0): Item[] => Array.from({ length: n }, (_, i) => ({ id: `i${i}`, bytes, log: [] }));

const logStage = (name: string, extra: Partial<ItemStage<Item>> = {}): ItemStage<Item> => ({
  name,
  run: (item: Item) => {
    item.log!.push(name);
  },
  ...extra,
});

describe('pipelino', () => {
  it('runs stages in order, merges results, emits item:done and done', async () => {
    const p = createPipeline<Item>({
      stages: [
        { name: 'a', run: async () => ({ a: 1 }) },
        { name: 'b', run: async (it) => ({ b: (it.a as number) + 1 }) },
      ],
    });
    const complete = vi.fn();
    const done = vi.fn();
    p.on('item:done', complete);
    p.on('done', done);
    p.push(items(3));
    expect(p.status).toBe('running');
    await settle();
    expect(complete).toHaveBeenCalledTimes(3);
    expect(done).toHaveBeenCalledTimes(1);
    expect(done.mock.calls[0]![0].done.map((i: Item) => i.b)).toEqual([2, 2, 2]);
    expect(p.status).toBe('idle');
  });

  it('streams items through stages without waiting for siblings', async () => {
    const gate = defer();
    const order: string[] = [];
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          run: async (it) => {
            if (it.id === 'i1') await gate.promise;
            order.push(`a:${it.id}`);
          },
        },
        { name: 'b', run: (it) => void order.push(`b:${it.id}`) },
      ],
    });
    p.push(items(2));
    await settle();
    expect(order).toEqual(['a:i0', 'b:i0']); // i0 reached b while i1 still in a
    gate.resolve();
    await settle();
    expect(order).toEqual(['a:i0', 'b:i0', 'a:i1', 'b:i1']);
  });

  it('barrier stage waits until every live item passed the previous stage', async () => {
    const gate = defer();
    const order: string[] = [];
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          run: async (it) => {
            if (it.id === 'i1') await gate.promise;
            order.push(`a:${it.id}`);
          },
        },
        { name: 'b', barrier: true, run: (it) => void order.push(`b:${it.id}`) },
      ],
    });
    p.push(items(2));
    await settle();
    expect(order).toEqual(['a:i0']);
    gate.resolve();
    await settle();
    expect(order).toEqual(['a:i0', 'a:i1', 'b:i0', 'b:i1']);
  });

  it('failed items skip remaining stages and are reported', async () => {
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          run: (it) => {
            if (it.id === 'i1') throw new Error('boom');
          },
        },
        logStage('b'),
      ],
    });
    const err = vi.fn();
    const done = vi.fn();
    p.on('stage:error', err);
    p.on('done', done);
    p.push(items(3));
    await settle();
    expect(err).toHaveBeenCalledTimes(1);
    expect(err.mock.calls[0]![0].stage).toBe('a');
    expect(err.mock.calls[0]![0].error.message).toBe('boom');
    const { done: ok, failed } = done.mock.calls[0]![0];
    expect(ok.map((i: Item) => i.id)).toEqual(['i0', 'i2']);
    expect(failed.map((i: Item) => i.id)).toEqual(['i1']);
    expect(failed[0].log).toEqual([]);
    expect(p.items().find((s) => s.item.id === 'i1')?.state).toBe('failed');
  });

  it('batches by size and bytes', async () => {
    const batches: string[][] = [];
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'up',
          concurrency: 1,
          batch: { size: 3, weight: 10, weigh: (it) => it.bytes! },
          runBatch: async (its) => {
            batches.push(its.map((i) => i.id));
            return its.map(() => ({ up: true }));
          },
        },
      ],
    });
    p.push([
      { id: 'a', bytes: 4 },
      { id: 'b', bytes: 4 },
      { id: 'c', bytes: 4 }, // would exceed 10 -> new batch
      { id: 'd', bytes: 1 },
      { id: 'e', bytes: 1 }, // size 3 reached
      { id: 'f', bytes: 50 }, // oversize item still goes alone
    ]);
    await settle();
    expect(batches).toEqual([['a', 'b'], ['c', 'd', 'e'], ['f']]);
    expect(p.items().every((s) => s.item.up === true)).toBe(true);
  });

  it('respects per-stage concurrency', async () => {
    let active = 0;
    let peak = 0;
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          concurrency: 2,
          run: async () => {
            active++;
            peak = Math.max(peak, active);
            await tick();
            active--;
          },
        },
      ],
    });
    p.push(items(6));
    await settle(20);
    expect(peak).toBe(2);
    expect(p.progress().overall.done).toBe(6);
  });

  it('skip passes items through untouched', async () => {
    const p = createPipeline<Item>({
      stages: [logStage('a', { skip: (it) => it.id === 'i0' }), logStage('b')],
    });
    p.push(items(2));
    await settle();
    const logs = Object.fromEntries(p.items().map((s) => [s.item.id, s.item.log]));
    expect(logs).toEqual({ i0: ['b'], i1: ['a', 'b'] });
  });

  it('startAt enters items mid-pipeline', async () => {
    const p = createPipeline<Item>({ stages: [logStage('a'), logStage('b'), logStage('c')] });
    p.push(items(1), { startAt: 'b' });
    await settle();
    expect(p.items()[0]!.item.log).toEqual(['b', 'c']);
    expect(p.progress().stages.a!.done).toBe(1); // counted as already passed
  });

  it('remove drops queued items and discards results of running ones', async () => {
    const gate = defer();
    const ran: string[] = [];
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          concurrency: 1,
          run: async (it) => {
            ran.push(it.id);
            await gate.promise;
            return { touched: true };
          },
        },
      ],
    });
    const complete = vi.fn();
    p.on('item:done', complete);
    p.push(items(3));
    await settle();
    p.remove(['i0', 'i2']); // i0 running, i2 queued
    gate.resolve();
    await settle();
    expect(ran).toEqual(['i0', 'i1']);
    expect(complete).toHaveBeenCalledTimes(1);
    expect(complete.mock.calls[0]![0].item.id).toBe('i1');
    expect(p.items().map((s) => s.item.id)).toEqual(['i1']);
  });

  it('pause stops scheduling, resume continues', async () => {
    const ran: string[] = [];
    const p = createPipeline<Item>({
      stages: [{ name: 'a', concurrency: 1, run: async (it) => void ran.push(it.id) }],
    });
    p.pause(); // no-op while idle
    p.push(items(3));
    p.pause();
    await settle();
    expect(ran.length).toBeLessThanOrEqual(1);
    expect(p.status).toBe('paused');
    p.resume();
    await settle();
    expect(ran).toEqual(['i0', 'i1', 'i2']);
  });

  it('cancel aborts the signal, drops in-flight and queued work, keeps finished items', async () => {
    const gate = defer();
    let signal: AbortSignal | undefined;
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          concurrency: 1,
          run: async (it, ctx) => {
            signal = ctx.signal;
            if (it.id !== 'i0') await gate.promise;
          },
        },
      ],
    });
    const done = vi.fn();
    p.on('done', done);
    p.push(items(3));
    await settle();
    p.cancel();
    expect(signal?.aborted).toBe(true);
    expect(p.status).toBe('idle');
    expect(p.items().map((s) => s.item.id)).toEqual(['i0']);
    gate.resolve();
    await settle();
    expect(done).not.toHaveBeenCalled();
    expect(p.progress().overall).toMatchObject({ done: 1, total: 1 });
  });

  it('addStages re-enters finished items at the new stage', async () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    p.push(items(2));
    await settle();
    expect(p.status).toBe('idle');
    const done = vi.fn();
    p.on('done', done);
    p.addStages([logStage('b', { barrier: true })]);
    p.push(items(1).map((i) => ({ ...i, id: 'late' })));
    await settle();
    expect(p.items().map((s) => s.item.log)).toEqual([
      ['a', 'b'],
      ['a', 'b'],
      ['a', 'b'],
    ]);
    expect(done).toHaveBeenCalledTimes(1);
    expect(() => p.addStages([logStage('a')])).toThrow(/duplicate/);
  });

  it('reports progress per stage and overall', async () => {
    const gate = defer();
    const p = createPipeline<Item>({
      stages: [
        { name: 'a', run: async (it) => void (it.id === 'i2' && (await gate.promise)) },
        {
          name: 'b',
          run: (it) => {
            if (it.id === 'i0') throw new Error('x');
          },
        },
      ],
    });
    const progress = vi.fn();
    p.on('progress', progress);
    p.push(items(3));
    await settle();
    let s = p.progress();
    expect(s.stages.a).toMatchObject({ done: 2, running: 1, failed: 0, total: 3, percent: 67 });
    expect(s.stages.b).toMatchObject({ done: 1, failed: 1, total: 3, percent: 67 });
    expect(s.overall).toMatchObject({ done: 1, failed: 1, running: 1, total: 3 });
    gate.resolve();
    await settle();
    s = p.progress();
    expect(s.overall).toMatchObject({ done: 2, failed: 1, running: 0, total: 3, percent: 100 });
    expect(progress).toHaveBeenCalled();
  });

  it('ignores a push for an id that is queued or running', async () => {
    const gate = defer();
    const p = createPipeline<Item>({ stages: [{ name: 'a', run: () => gate.promise }] });
    expect(p.push(items(1))).toBe(1);
    expect(p.push(items(1))).toBe(0);
    gate.resolve();
    await settle();
    expect(p.push(items(1))).toBe(1); // finished items may be re-pushed
  });

  it('exposes status and stages read-only', () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    // @ts-expect-error status is read-only
    expect(() => void (p.status = 'paused')).toThrow(TypeError);
    // @ts-expect-error stages is a read-only array
    const mutable: Stage<Item>[] = p.stages;
    expect(mutable).toHaveLength(1);
    // @ts-expect-error unsubscribing is the function returned by on()
    expect(p.off).toBeUndefined();
  });

  it('exports one runtime entry point', async () => {
    const api = await import('./index.js');
    expect(Object.keys(api)).toEqual(['createPipeline']);
  });

  it('validates stages', () => {
    expect(() => createPipeline<Item>({ stages: [{ name: '', run: () => {} }] })).toThrow(/name/);
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', concurrency: 0, run: () => {} }] })).toThrow(/concurrency/);
    // @ts-expect-error batch stages need runBatch
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', batch: { size: 2 }, run: () => {} }] })).toThrow(/runBatch/);
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', run: () => {} }] }).push(items(1), { startAt: 'zz' })).toThrow(/unknown stage/);
  });

  it('a throwing listener is reported and does not stall the pipeline', async () => {
    const reportError = vi.fn();
    vi.stubGlobal('reportError', reportError);
    try {
      const p = createPipeline<Item>({ stages: [{ name: 'a', concurrency: 1, run: async () => {} }] });
      const done = vi.fn();
      p.on('stage:done', () => {
        throw new Error('listener boom');
      });
      p.on('done', done);
      p.push(items(3));
      await settle();
      expect(done).toHaveBeenCalledTimes(1);
      expect(p.progress().overall.done).toBe(3);
      expect(reportError.mock.calls.map((c) => c[0].message)).toEqual(['listener boom', 'listener boom', 'listener boom']);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('a throwing skip() or weigh() fails that item instead of wedging the stage', async () => {
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          skip: (it) => {
            if (it.id === 'i0') throw new Error('skip boom');
            return false;
          },
          run: async () => {},
        },
        {
          name: 'b',
          batch: {
            size: 2,
            weight: 10,
            weigh: (it) => {
              if (it.id === 'i1') throw new Error('weigh boom');
              return 1;
            },
          },
          runBatch: async () => {},
        },
      ],
    });
    const err = vi.fn();
    const done = vi.fn();
    p.on('stage:error', err);
    p.on('done', done);
    p.push(items(3));
    await settle();
    expect(err.mock.calls.map((c) => [c[0].stage, c[0].item.id, c[0].error.message])).toEqual([
      ['a', 'i0', 'skip boom'],
      ['b', 'i1', 'weigh boom'],
    ]);
    expect(done.mock.calls[0]![0].done.map((i: Item) => i.id)).toEqual(['i2']);
    expect(p.status).toBe('idle');
  });

  it('a patch cannot change the item id', async () => {
    const p = createPipeline<Item>({
      stages: [{ name: 'a', run: async () => ({ id: 'renamed' }) }, logStage('b')],
    });
    const done = vi.fn();
    p.on('done', done);
    p.push(items(1));
    await settle();
    expect(done).toHaveBeenCalledTimes(1);
    expect(p.items().map((s) => [s.item.id, s.state])).toEqual([['i0', 'done']]);
  });

  it('rejects batch.weight without weigh', () => {
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', batch: { size: 2, weight: 1 }, runBatch: () => {} }] })).toThrow(/weigh/);
  });

  it('a throwing batch fails every item in it', async () => {
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          batch: { size: 2 },
          runBatch: async () => {
            throw new Error('batch boom');
          },
        },
      ],
    });
    const done = vi.fn();
    p.on('done', done);
    p.push(items(2));
    await settle();
    expect(done.mock.calls[0]![0].failed.map((i: Item) => i.id)).toEqual(['i0', 'i1']);
    expect(p.items().every((s) => s.state === 'failed' && (s.error as Error).message === 'batch boom')).toBe(true);
  });

  it('failed items can be re-pushed and run again from the start', async () => {
    let fail = true;
    const p = createPipeline<Item>({
      stages: [
        logStage('a', {
          run: (it) => {
            it.log!.push('a');
            if (fail) throw new Error('once');
          },
        }),
        logStage('b'),
      ],
    });
    p.push(items(1));
    await settle();
    expect(p.items()[0]!.state).toBe('failed');
    fail = false;
    expect(p.push(items(1))).toBe(1);
    await settle();
    expect(p.items().map((s) => [s.state, s.item.log])).toEqual([['done', ['a', 'b']]]);
  });

  it('reset forgets every item and unsubscribing stops events', async () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    const progress = vi.fn();
    const off = p.on('progress', progress);
    p.push(items(2));
    await settle();
    p.reset();
    expect(p.items()).toEqual([]);
    expect(p.progress().overall).toEqual({ done: 0, failed: 0, running: 0, total: 0, percent: 0 });
    const calls = progress.mock.calls.length;
    off();
    p.push(items(1));
    await settle();
    expect(progress).toHaveBeenCalledTimes(calls);
  });

  it('a run left over from before cancel() cannot touch a re-pushed item', async () => {
    const gate = defer();
    let runs = 0;
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          run: async (it) => {
            if (runs++ === 0) {
              it.log!.push('old');
              await gate.promise;
              return { stale: true };
            }
            it.log!.push('new');
          },
        },
      ],
    });
    p.push(items(1));
    await settle();
    p.cancel();
    const fresh = items(1);
    p.push(fresh);
    await settle();
    gate.resolve();
    await settle();
    expect(p.items().map((s) => [s.state, s.item.log, s.item.stale])).toEqual([['done', ['new'], undefined]]);
  });
});

describe('progress counters', () => {
  it('match a brute-force recount after random pushes, removes, failures, skips and addStages', async () => {
    let seed = 42;
    const rnd = () => (seed = (seed * 1664525 + 1013904223) % 4294967296) / 4294967296;
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          concurrency: 3,
          run: async (it) => {
            if (rnd() < 0.1) throw new Error(it.id);
          },
        },
        { name: 'b', concurrency: 2, skip: () => rnd() < 0.3, run: async () => {} },
        { name: 'c', batch: { size: 4 }, runBatch: async (its) => its.map(() => ({ c: 1 })) },
      ],
    });
    const recount = () => {
      const snaps = p.items();
      const names = p.stages.map((s) => s.name);
      const at = (s: (typeof snaps)[number]) => (s.stage == null ? names.length : names.indexOf(s.stage));
      const total = snaps.length;
      const got = p.progress();
      names.forEach((name, i) => {
        let done = 0,
          failed = 0,
          running = 0;
        for (const s of snaps) {
          if (s.state === 'failed' && at(s) <= i) failed++;
          else if (at(s) > i) done++;
          else if (s.state === 'running' && at(s) === i) running++;
        }
        expect(got.stages[name]).toEqual({ done, failed, running, total, percent: total ? Math.round(((done + failed) / total) * 100) : 0 });
      });
      const done = snaps.filter((s) => s.state === 'done').length;
      const failed = snaps.filter((s) => s.state === 'failed').length;
      const running = snaps.filter((s) => s.state === 'running').length;
      expect(got.overall).toEqual({ done, failed, running, total, percent: total ? Math.round(((done + failed) / total) * 100) : 0 });
    };
    let n = 0;
    for (let round = 0; round < 40; round++) {
      p.push(items(1 + Math.floor(rnd() * 5)).map((i) => ({ ...i, id: `r${round}-${i.id}` })));
      n++;
      if (rnd() < 0.3)
        p.remove(
          p
            .items()
            .filter(() => rnd() < 0.2)
            .map((s) => s.item.id),
        );
      if (round === 20) p.addStages([{ name: 'd', barrier: true, run: async () => {} }]);
      await tick();
      recount();
    }
    await settle();
    recount();
    expect(p.status).toBe('idle');
    expect(p.progress().overall.running).toBe(0);
  });
});

describe('listeners re-entering the scheduler', () => {
  it('a listener that removes a sibling during take() keeps it out of the run', async () => {
    const ran: string[] = [];
    const p = createPipeline<Item>({
      stages: [{ name: 'a', batch: { size: 3 }, skip: (it) => it.id === 'i1', runBatch: async (its) => void ran.push(...its.map((i) => i.id)) }],
    });
    p.on('item:done', ({ item }) => void (item.id === 'i1' && p.remove('i0')));
    p.push(items(3));
    await settle();
    expect(ran).toEqual(['i2']);
    expect(p.progress().overall.running).toBe(0);
  });

  it('cancel() from a stage:error listener during take() discards the half-built batch', async () => {
    const ran: string[][] = [];
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          batch: { size: 3 },
          skip: (it) => {
            if (it.id === 'i1') throw new Error('x');
            return false;
          },
          runBatch: async (its) => void ran.push(its.map((i) => i.id)),
        },
      ],
    });
    p.on('stage:error', () => p.cancel());
    p.push(items(4));
    await settle();
    expect(ran).toEqual([]);
    p.push([{ id: 'x' }, { id: 'y' }]);
    await settle();
    expect(ran).toEqual([['x', 'y']]);
    expect(p.progress().overall).toMatchObject({ done: 2, failed: 1, running: 0 });
    expect(p.status).toBe('idle');
  });
});

describe('edge cases', () => {
  it('a patch leaves a getter-only id alone', async () => {
    class Photo {
      constructor(readonly name: string) {}
      get id() {
        return this.name;
      }
      w?: number;
    }
    const p = createPipeline<Photo>({ stages: [{ name: 'a', run: async () => ({ w: 1 }) }] });
    p.push(new Photo('a'));
    await settle();
    expect(p.items().map((s) => [s.state, s.item.w])).toEqual([['done', 1]]);
    expect(p.status).toBe('idle');
  });

  it('a patch that cannot be applied fails the item instead of wedging the batch', async () => {
    const p = createPipeline<Item>({ stages: [{ name: 'a', batch: { size: 2 }, runBatch: async (its) => its.map(() => ({ w: 1 })) }] });
    const err = vi.fn();
    p.on('stage:error', err);
    p.push([Object.freeze({ id: 'frozen' }), { id: 'ok' }]);
    await settle();
    expect(err).toHaveBeenCalledTimes(1);
    expect(err.mock.calls[0]![0].error).toBeInstanceOf(TypeError);
    expect(p.items().map((s) => [s.item.id, s.state])).toEqual([
      ['frozen', 'failed'],
      ['ok', 'done'],
    ]);
    expect(p.status).toBe('idle');
  });

  it('a rejected addStages leaves the pipeline untouched', async () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    p.push(items(2));
    await settle();
    expect(() => p.addStages([logStage('b'), logStage('a')])).toThrow(/duplicate/);
    expect(p.stages.map((s) => s.name)).toEqual(['a']);
    expect(p.progress().overall).toMatchObject({ done: 2, total: 2, percent: 100 });
  });

  it('addStages with nothing to re-run emits no status change', async () => {
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          run: () => {
            throw new Error('x');
          },
        },
      ],
    });
    p.push(items(1));
    await settle();
    const status = vi.fn();
    p.on('status', status);
    p.addStages([logStage('b')]);
    await settle();
    expect(status).not.toHaveBeenCalled();
    expect(p.status).toBe('idle');
  });

  it('rejects NaN concurrency and batch.size', () => {
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', concurrency: NaN, run: () => {} }] })).toThrow(/concurrency/);
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', batch: { size: NaN }, runBatch: () => {} }] })).toThrow(/batch\.size/);
  });

  it('rejects an empty stage list', () => {
    expect(() => createPipeline<Item>({ stages: [] })).toThrow(/stage/);
  });

  it('remove returns how many were dropped and is silent for unknown ids', async () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    p.push(items(2));
    await settle();
    const progress = vi.fn();
    p.on('progress', progress);
    expect(p.remove('nope')).toBe(0);
    expect(progress).not.toHaveBeenCalled();
    expect(p.remove(['i0', 'nope'])).toBe(1);
    expect(progress).toHaveBeenCalledTimes(1);
  });

  it('work that finishes while paused settles the pipeline', async () => {
    const gate = defer();
    const p = createPipeline<Item>({ stages: [{ name: 'a', run: () => gate.promise }] });
    const done = vi.fn();
    p.on('done', done);
    p.push(items(1));
    await settle();
    p.pause();
    gate.resolve();
    await settle();
    expect(p.status).toBe('idle');
    expect(done).toHaveBeenCalledTimes(1);
  });

  it('progress is not computed when nobody listens', async () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    const spy = vi.spyOn(p, 'progress');
    p.push(items(2));
    await settle();
    expect(spy).not.toHaveBeenCalled();
  });

  it('a stage that never drains does not keep consumed queue slots', async () => {
    const p = createPipeline<Item>({ stages: [{ name: 'a', concurrency: 1, batch: { size: 4 }, runBatch: async () => void (await tick()) }] });
    for (let round = 0; round < 100; round++) {
      p.push(items(5).map((i) => ({ ...i, id: `${round}-${i.id}` })));
      await tick();
    }
    // Reaching into the queue: the invariant is "consumed slots never outnumber live ones".
    const queue = (p as unknown as { queues: unknown[][] }).queues[0]!;
    const head = (p as unknown as { heads: number[] }).heads[0]!;
    expect(head === 0 || head * 2 < queue.length).toBe(true);
  });
});

describe('skip() and weigh() re-entering the scheduler', () => {
  it('an item removed from inside skip() of a later item does not run', async () => {
    const runs: string[][] = [];
    const p: Pipeline<Item> = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          batch: { size: 3 },
          skip: (item) => (item.id === 'i1' && p.remove('i0'), false),
          runBatch: (batch) => void runs.push(batch.map((i) => i.id)),
        },
      ],
    });
    p.push(items(3));
    await settle();
    expect(runs).toEqual([['i1', 'i2']]);
    expect(p.progress().overall).toMatchObject({ done: 2, running: 0, total: 2 });
  });

  it('an item that removes itself from inside skip() is gone and the pipeline still settles', async () => {
    const p: Pipeline<Item> = createPipeline<Item>({ stages: [{ name: 'a', skip: (item) => (p.remove(item.id), true), run: () => {} }, logStage('b')] });
    const done = vi.fn();
    p.on('item:done', done);
    p.push(items(2));
    await settle();
    expect(p.status).toBe('idle');
    expect(p.items()).toEqual([]);
    expect(done).not.toHaveBeenCalled();
  });

  it('cancel() from inside weigh() runs nothing and leaves no running count behind', async () => {
    const runs: string[][] = [];
    const p: Pipeline<Item> = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          batch: { size: 3, weight: 100, weigh: (item) => (item.id === 'i2' && p.cancel(), 1) },
          runBatch: (batch) => void runs.push(batch.map((i) => i.id)),
        },
      ],
    });
    p.push(items(3));
    await settle();
    expect(runs).toEqual([]);
    expect(p.progress().overall).toMatchObject({ running: 0, total: 0 });
  });
});

describe('concurrency across cancel()', () => {
  it('a run that ignores the abort signal keeps its slot until it settles', async () => {
    const gates = [defer(), defer()];
    let inFlight = 0;
    let most = 0;
    let started = 0;
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          concurrency: 1,
          run: async () => {
            most = Math.max(most, ++inFlight);
            await gates[started++]!.promise;
            inFlight--;
          },
        },
      ],
    });
    p.push({ id: 'old' });
    await settle();
    p.cancel();
    p.push({ id: 'new' });
    await settle();
    expect(started).toBe(1); // 'new' waits for the cancelled run, which is still going
    gates[0]!.resolve();
    await settle();
    expect(started).toBe(2);
    gates[1]!.resolve();
    await settle();
    expect(most).toBe(1);
    expect(p.items().map((s) => [s.item.id, s.state])).toEqual([['new', 'done']]);
  });
});

describe('values a stage may not return', () => {
  it('fails the item when a stage returns a string, an array or a number instead of merging it by index', async () => {
    const bad = ['xy', [9, 9], 42];
    const p = createPipeline<Item>({ stages: [{ name: 'a', run: (item) => bad[Number(item.id.slice(1))] as never }] });
    const errors = vi.fn();
    p.on('stage:error', errors);
    p.push(items(3));
    await settle();
    expect(p.items().map((s) => [Object.keys(s.item).sort(), s.state])).toEqual(Array(3).fill([['bytes', 'id', 'log'], 'failed']));
    expect(errors.mock.calls[0]![0].error).toBeInstanceOf(TypeError);
  });

  it('accepts null like nothing at all', async () => {
    const p = createPipeline<Item>({ stages: [{ name: 'a', run: () => null as never }] });
    p.push(items(1));
    await settle();
    expect(p.items()[0]!.state).toBe('done');
  });
});

describe('arguments the scheduler cannot work with', () => {
  it('rejects fractional concurrency and batch.size, and accepts Infinity', () => {
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', concurrency: 2.5, run: () => {} }] })).toThrow(/concurrency/);
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', batch: { size: 2.5 }, runBatch: () => {} }] })).toThrow(/batch\.size/);
    expect(() => createPipeline<Item>({ stages: [{ name: 'a', concurrency: Infinity, batch: { size: Infinity }, runBatch: () => {} }] })).not.toThrow();
  });

  it('rejects a batch.weight that is not a positive number', () => {
    for (const weight of [NaN, 0, -1]) {
      expect(() => createPipeline<Item>({ stages: [{ name: 'a', batch: { size: 2, weight, weigh: () => 1 }, runBatch: () => {} }] })).toThrow(/batch\.weight/);
    }
  });

  it('fails an item whose weigh() returns NaN or a negative number instead of letting it disable the cap', async () => {
    const runs: string[][] = [];
    const weights = [4, NaN, 4, -100, 4];
    const p = createPipeline<Item>({
      stages: [
        {
          name: 'a',
          concurrency: 1,
          batch: { size: 10, weight: 10, weigh: (item) => weights[Number(item.id.slice(1))]! },
          runBatch: (batch) => void runs.push(batch.map((i) => i.id)),
        },
      ],
    });
    p.push(items(5));
    await settle();
    expect(runs).toEqual([['i0', 'i2'], ['i4']]);
    expect(
      p
        .items()
        .filter((s) => s.state === 'failed')
        .map((s) => s.item.id),
    ).toEqual(['i1', 'i3']);
  });

  it('rejects items without an id before adding any of them', () => {
    const p = createPipeline<Item>({ stages: [logStage('a')] });
    expect(() => p.push([{ id: 'ok', log: [] }, { log: [] } as unknown as Item])).toThrow(TypeError);
    expect(p.items()).toEqual([]);
    expect(p.status).toBe('idle');
  });

  it('rejects a stage named __proto__, which could not be listed in progress().stages', () => {
    expect(() => createPipeline<Item>({ stages: [logStage('__proto__')] })).toThrow(/__proto__/);
  });
});
