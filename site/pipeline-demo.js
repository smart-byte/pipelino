import { createPipeline } from '../src/index.ts';

const $ = (id) => document.getElementById(id);
const TOTAL = 24;
const STAGES = ['inspect', 'thumbnail', 'upload'];
const stageIndex = new Map(STAGES.map((name, index) => [name, index]));
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
const stacked = matchMedia('(max-width: 900px)'); // the graph runs top to bottom below this width (see pipeline.css)
const demo = $('pipeline-demo');
let mode = 'barrier';
let pipe;
let generation = 0;
let timer;
let startedAt = 0;
let paintFrame = 0;
let autoStartTimer;
let interacted = false;
let completed = false;

// The demo uses the real scheduler. Only the file-processing work is simulated.
function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const timeout = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, ms);
    function abort() {
      clearTimeout(timeout);
      reject(signal.reason);
    }
    signal.addEventListener('abort', abort, { once: true });
  });
}

const stageElements = STAGES.map((name) => {
  const node = $(`${name}-node`);
  const grid = node.querySelector('.item-grid');
  const cells = Array.from({ length: TOTAL }, () => {
    const cell = document.createElement('i');
    cell.className = 'item';
    grid.append(cell);
    return cell;
  });
  return {
    node,
    cells,
    count: node.querySelector('.stage-done'),
    state: node.querySelector('.stage-state'),
    bar: node.querySelector('.progress-track i'),
  };
});

function build() {
  return createPipeline({
    stages: [
      {
        name: 'inspect',
        concurrency: 4,
        run: async (photo, { signal }) => {
          await delay(430 + ((photo.index * 37) % 350), signal);
          return { width: 4032, height: 3024, format: 'JPEG' };
        },
      },
      {
        name: 'thumbnail',
        concurrency: 3,
        run: async (photo, { signal }) => {
          await delay(680 + ((photo.index * 53) % 500), signal);
          return { thumb: { width: 320, height: 240, bytes: 64000 } };
        },
      },
      {
        name: 'upload',
        barrier: mode === 'barrier',
        concurrency: 2,
        batch: { size: 6 },
        runBatch: async (photos, { signal }) => {
          await delay(850 + photos.length * 80, signal);
          return photos.map(() => ({ uploaded: true }));
        },
      },
    ],
  });
}

function schedulePaint() {
  if (!paintFrame) paintFrame = requestAnimationFrame(paint);
}

function paint() {
  paintFrame = 0;
  if (!pipe) return;
  const items = pipe.items();
  const progress = pipe.progress();
  const byId = new Map(items.map((snapshot) => [snapshot.item.index, snapshot]));
  const running = pipe.status === 'running';
  const paused = pipe.status === 'paused';
  demo.dataset.status = pipe.status;
  demo.dataset.complete = String(completed);

  stageElements.forEach((stage, index) => {
    const stats = progress.stages[STAGES[index]];
    const behind = items.filter((snapshot) => snapshot.state !== 'failed' && snapshot.stage !== null && stageIndex.get(snapshot.stage) < index).length;
    const waiting = index === 2 && mode === 'barrier' && behind > 0;
    stage.node.classList.toggle('is-running', stats.running > 0);
    stage.count.textContent = stats.done;
    stage.bar.style.width = `${stats.percent}%`;
    stage.state.classList.toggle('waiting', waiting);
    stage.state.textContent = stats.running
      ? `${stats.running} running`
      : paused
        ? 'Paused'
        : stats.total && stats.done === stats.total
          ? 'Done'
          : waiting
            ? 'Waiting for thumbnails'
            : running
              ? 'Waiting'
              : 'Ready';

    stage.cells.forEach((cell, itemIndex) => {
      const snapshot = byId.get(itemIndex);
      let state = '';
      if (snapshot) {
        const at = snapshot.stage === null ? 3 : stageIndex.get(snapshot.stage);
        if (snapshot.state === 'failed') state = at <= index ? 'failed' : 'done';
        else if (at > index) state = 'done';
        else if (at === index) state = snapshot.state;
      }
      cell.className = `item ${state}`;
    });
  });

  $('output-count').textContent = progress.overall.done;
  $('output-ring').style.setProperty('--progress', `${progress.overall.percent}%`);
  $('output-caption').textContent = `${progress.overall.percent}%`;
  $('input-node').querySelector(':scope > span').textContent = !items.length
    ? 'Ready'
    : progress.stages.inspect.done === TOTAL
      ? 'All inspected'
      : `${TOTAL - progress.stages.inspect.done} to inspect`;
  $('pause').disabled = pipe.status === 'idle';
  $('pause').textContent = paused ? 'Resume' : 'Pause';
  $('reset').disabled = !items.length;
  $('run').querySelector('span').textContent = running || paused ? 'Restart' : completed ? 'Run again' : 'Run';
  const status = completed ? `${TOTAL} of ${TOTAL} done` : paused ? 'Paused' : running ? 'Running' : 'Ready';
  // Keep screen-reader announcements to meaningful state changes.
  if ($('demo-status').textContent !== status) $('demo-status').textContent = status;
  updateConnections();
}

function tickClock() {
  $('elapsed').textContent = `${((performance.now() - startedAt) / 1000).toFixed(1)}s`;
}

function markInteraction() {
  interacted = true;
  clearTimeout(autoStartTimer);
}

function start() {
  generation += 1;
  const currentGeneration = generation;
  if (pipe) pipe.cancel();
  clearInterval(timer);
  completed = false;
  pipe = build();
  pipe.on('progress', schedulePaint);
  pipe.on('status', schedulePaint);
  pipe.on('stage:done', ({ stage, item }) => {
    if (generation !== currentGeneration) return;
    const patch = stage === 'inspect' ? '{ width: 4032 }' : stage === 'thumbnail' ? '{ thumb: "320 × 240" }' : '{ uploaded: true }';
    $('demo-log').textContent = `${stage} → ${item.id}.jpg · ${patch}`;
  });
  pipe.on('done', ({ done, failed }) => {
    if (generation !== currentGeneration) return;
    completed = true;
    clearInterval(timer);
    tickClock();
    $('demo-log').textContent = `done → ${done.length} uploaded, ${failed.length} failed`;
    schedulePaint();
  });
  $('demo-log').textContent = 'pipeline.push(photos)';
  startedAt = performance.now();
  timer = setInterval(tickClock, 100);
  tickClock();
  pipe.push(Array.from({ length: TOTAL }, (_, index) => ({ id: `photo-${String(index + 1).padStart(2, '0')}`, index })));
  schedulePaint();
}

function reset() {
  generation += 1;
  pipe.reset();
  clearInterval(timer);
  completed = false;
  $('elapsed').textContent = '0.0s';
  $('demo-log').textContent = 'pipeline.push(photos)';
  schedulePaint();
}

$('run').addEventListener('click', () => {
  markInteraction();
  start();
});
$('pause').addEventListener('click', () => {
  markInteraction();
  if (pipe.status === 'paused') {
    pipe.resume();
    $('demo-log').textContent = 'pipeline.resume()';
  } else {
    pipe.pause();
    $('demo-log').textContent = 'pipeline.pause() → running work finishes, queued items wait';
  }
  schedulePaint();
});
$('reset').addEventListener('click', () => {
  markInteraction();
  reset();
});

document.querySelectorAll('.mode-control button').forEach((button) => {
  button.addEventListener('click', () => {
    if (button.dataset.mode === mode) return;
    markInteraction();
    mode = button.dataset.mode;
    demo.dataset.mode = mode;
    document.querySelectorAll('.mode-control button').forEach((option) => {
      option.classList.toggle('active', option.dataset.mode === mode);
      option.setAttribute('aria-pressed', String(option.dataset.mode === mode));
    });
    $('mode-description').textContent = mode === 'barrier' ? 'Upload waits until every thumbnail exists.' : 'Upload starts as soon as thumbnails are ready.';
    $('example-barrier').textContent = String(mode === 'barrier');
    start();
  });
});

// Draw the connectors from the rendered nodes so the diagram follows the layout.
const svgNamespace = 'http://www.w3.org/2000/svg';
let connectionElements = [];
function drawConnections() {
  const bounds = $('pipeline-graph').getBoundingClientRect();
  const vertical = stacked.matches;
  const nodes = ['input', ...STAGES, 'output'].map((name) => $(`${name}-node`).getBoundingClientRect());
  const fragment = document.createDocumentFragment();
  connectionElements = [];
  for (let index = 0; index < nodes.length - 1; index += 1) {
    const from = nodes[index];
    const to = nodes[index + 1];
    const x1 = (vertical ? from.left + from.width / 2 : from.right) - bounds.left;
    const x2 = (vertical ? to.left + to.width / 2 : to.left) - bounds.left;
    const y1 = (vertical ? from.bottom : from.top + from.height / 2) - bounds.top;
    const y2 = (vertical ? to.top : to.top + to.height / 2) - bounds.top;
    for (const animated of [false, true]) {
      const path = document.createElementNS(svgNamespace, 'path');
      path.setAttribute('d', `M ${x1} ${y1} L ${x2} ${y2}`);
      path.setAttribute('class', animated ? 'connection-packet' : 'connection-line');
      fragment.append(path);
      if (animated) connectionElements.push({ path, index });
    }
  }
  $('connection-paths').replaceChildren(fragment);
  updateConnections();
}
function updateConnections() {
  if (!pipe) return;
  const progress = pipe.progress();
  for (const { path, index } of connectionElements) {
    const active = pipe.status === 'running' && progress.stages[STAGES[Math.min(index, 2)]].running > 0;
    path.classList.toggle('active', active);
  }
}
new ResizeObserver(drawConnections).observe($('pipeline-graph'));
document.fonts.ready.then(drawConnections);

async function copyText(text, button) {
  const feedback = button.querySelector('.copy-feedback') || button.querySelector('span');
  // Remember the original label once; a second click inside the feedback window would otherwise capture "Copied".
  const label = (feedback.dataset.label ??= feedback.textContent);
  try {
    await navigator.clipboard.writeText(text);
    button.classList.add('copied');
    feedback.textContent = 'Copied';
    $('announcement').textContent = 'Copied to clipboard.';
    setTimeout(() => {
      button.classList.remove('copied');
      feedback.textContent = label;
      $('announcement').textContent = '';
    }, 1800);
  } catch {
    const range = document.createRange();
    range.selectNodeContents(button.dataset.copy ? button.querySelector('code') : $('pipeline-example'));
    const selection = getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
    $('announcement').textContent = 'Clipboard unavailable. Text selected; use your keyboard to copy.';
  }
}
document.querySelectorAll('[data-copy]').forEach((button) => {
  button.addEventListener('click', () => copyText(button.dataset.copy, button));
});
$('copy-code').addEventListener('click', () => copyText($('pipeline-example').textContent, $('copy-code')));

const root = document.documentElement;
function paintTheme() {
  $('theme-toggle').setAttribute('aria-pressed', String(root.dataset.theme === 'dark'));
  document.querySelector('meta[name="theme-color"]').content = getComputedStyle(root).getPropertyValue('--bg').trim();
}
$('theme-toggle').addEventListener('click', () => {
  root.dataset.theme = root.dataset.theme === 'dark' ? 'light' : 'dark';
  try {
    localStorage.setItem('theme', root.dataset.theme);
  } catch {
    // Storage is blocked (private mode): the choice lasts for this page view.
  }
  paintTheme();
});
paintTheme();

pipe = build();
paint();
const intersectionObserver = new IntersectionObserver(
  ([entry]) => {
    if (!entry.isIntersecting || reducedMotion.matches || interacted) return;
    intersectionObserver.disconnect();
    autoStartTimer = setTimeout(() => {
      if (!interacted && !reducedMotion.matches && !document.hidden) start();
    }, 1200);
  },
  { threshold: 0.35 },
);
intersectionObserver.observe(demo);
reducedMotion.addEventListener('change', () => {
  if (reducedMotion.matches) clearTimeout(autoStartTimer);
});
