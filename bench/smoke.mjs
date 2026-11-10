// Runs the built package on plain Node, without the test runner: CI uses it on the Node versions Vitest no longer supports.
import assert from 'node:assert/strict';
import { createPipeline } from '../dist/index.js';

const pipeline = createPipeline({
  stages: [
    { name: 'double', run: async (item) => ({ value: item.value * 2 }) },
    {
      name: 'pair',
      barrier: true,
      batch: { size: 2 },
      runBatch: async (items) => items.map(() => ({ batchOf: items.length })),
    },
  ],
});

const { done, failed } = await new Promise((resolve) => {
  pipeline.on('done', resolve);
  pipeline.push([1, 2, 3].map((value) => ({ id: String(value), value })));
});

assert.deepEqual(
  done.map((item) => [item.value, item.batchOf]),
  [
    [2, 2],
    [4, 2],
    [6, 1],
  ],
);
assert.equal(failed.length, 0);
console.log(`pipelino runs on Node ${process.version}`);
