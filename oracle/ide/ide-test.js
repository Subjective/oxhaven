#!/usr/bin/env node
// The IDE acceptance test: open the Deephaven web IDE that a server serves, log in, get a
// table on screen, and check the values the grid paints.
//
//   node oracle/ide/ide-test.js --url <server> [--psk <key>] [--routes panels,iframe,jsapi]
//                               [--expected <file>] [--out <dir>] [--timeout <ms>]
//                               [--deny Service/Method,...] [--empty Service/Method,...]
//                               [--restart <command>]
//
// <server> is http://host:port or https://host:port (with --psk, or without for anonymous
// authentication), or the oracle's form dh+psk://<key>@host:port and dh://host:port (plain http).
//
// Routes, each in a fresh browser profile:
//   panels   /ide/, log in through the login form, open the tables from the Panels list
//   iframe   /iframe/table/?name=<table>&psk=<key>
//   jsapi    /jsapi/dh-core.js in an empty page: log in, fetch the tables, read a viewport
//   console  /ide/?psk=<key>, type the commands of expected.json into the console
//   rollup   /ide/?psk=<key>, make a rollup in the Rollup Rows panel, expand a node, sort, filter
//   tree     /ide/?psk=<key>, make a tree in the console, expand a row, sort, filter
//   notebook, log, grid, errors, reconnect
//            what a user does around the console (lib/flows.js); reconnect restarts the
//            server with the shell command of --restart
// The default is panels,iframe,jsapi: what a server without a console can do.
//
// --deny and --empty are for finding out what the IDE needs, not for the gate: the named
// calls are answered in the browser, with UNIMPLEMENTED or with one empty message, and never
// reach the server (lib/faults.js). The trace of such a run does not show the websocket.
//
// Exit status: 0 every route passed, 1 a route failed, 2 the test could not run.
// README.md describes how the values are read and what is written to --out.
'use strict';

const fs = require('fs');
const path = require('path');
const { Trace, describeCalls } = require('./lib/trace');
const faults = require('./lib/faults');
const { routeRollup, routeTree } = require('./lib/rollup');

const PSK_HANDLER = 'io.deephaven.authentication.psk.PskAuthenticationHandler';
const ALL_ROUTES = ['panels', 'iframe', 'jsapi', 'console', 'rollup', 'tree', 'notebook', 'log', 'grid', 'errors', 'reconnect', 'format'];

function usage(message) {
  if (message) console.error(`error: ${message}`);
  console.error(
    'usage: node oracle/ide/ide-test.js --url <http(s)://host:port | dh+psk://key@host:port | dh://host:port>\n' +
      '         [--psk <key>] [--routes ' + ALL_ROUTES.join(',') + ']\n' +
      '         [--expected <file>] [--out <dir>] [--timeout <ms>] [--restart <command>]\n' +
      '         [--deny Service/Method,...] [--empty Service/Method,...]',
  );
  process.exit(2);
}

function parseArgs(argv) {
  const opts = {
    routes: ['panels', 'iframe', 'jsapi'],
    expected: path.join(__dirname, 'expected.json'),
    out: path.join(__dirname, '.out'),
    timeout: 30000,
    deny: [],
    empty: [],
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const value = () => {
      if (i + 1 >= argv.length) usage(`${a} needs a value`);
      return argv[++i];
    };
    if (a === '--url') opts.url = value();
    else if (a === '--psk') opts.psk = value();
    else if (a === '--routes') opts.routes = value().split(',').filter(Boolean);
    else if (a === '--expected') opts.expected = value();
    else if (a === '--out') opts.out = value();
    else if (a === '--timeout') opts.timeout = Number(value());
    else if (a === '--restart') opts.restart = value();
    else if (a === '--deny' || a === '--empty') {
      try {
        opts[a.slice(2)] = faults.parseRules(value());
      } catch (e) {
        usage(e.message);
      }
    }
    else usage(`unknown argument ${a}`);
  }
  if (!opts.url) usage('--url is required');
  const dh = /^dh(\+psk)?:\/\/(?:([^@/]*)@)?([^/]+)\/?$/.exec(opts.url);
  if (dh) {
    if (dh[1] && opts.psk === undefined) opts.psk = dh[2];
    opts.url = `http://${dh[3]}`;
  }
  if (!/^https?:\/\//.test(opts.url)) usage(`cannot read the server URL ${opts.url}`);
  opts.url = opts.url.replace(/\/+$/, '');
  for (const r of opts.routes) if (!ALL_ROUTES.includes(r)) usage(`unknown route ${r}`);
  if (!opts.routes.length) usage('no route selected');
  if (!Number.isFinite(opts.timeout) || opts.timeout <= 0) usage('--timeout must be a positive number');
  return opts;
}

class Failure extends Error {}

// ---------------------------------------------------------------------------------------
// Reading the grid. The IDE grid is one <canvas>, so there is no DOM to read cells from.
// The page is given a hook on CanvasRenderingContext2D.fillText before any of its scripts
// run; it keeps, per canvas, the text of the frame being painted. The grid starts every
// frame by filling the whole canvas, which is where a frame begins here. What the test
// compares is therefore exactly the strings the IDE painted, at the positions it painted them.
// ---------------------------------------------------------------------------------------
function gridHook() {
  const records = [];
  const byCanvas = new WeakMap();
  const record = (canvas) => {
    let r = byCanvas.get(canvas);
    if (!r) {
      r = { canvas, frame: [], frames: 0 };
      byCanvas.set(canvas, r);
      records.push(r);
    }
    return r;
  };
  const proto = CanvasRenderingContext2D.prototype;
  const fillText = proto.fillText;
  proto.fillText = function (text, x, y, ...rest) {
    const m = this.getTransform();
    record(this.canvas).frame.push({ text: String(text), x: m.e + x * m.a, y: m.f + y * m.d, fill: String(this.fillStyle) });
    return fillText.call(this, text, x, y, ...rest);
  };
  const fillRect = proto.fillRect;
  proto.fillRect = function (x, y, w, h) {
    const m = this.getTransform();
    if (m.e + x * m.a <= 0 && m.f + y * m.d <= 0 && w * m.a >= this.canvas.width && h * m.d >= this.canvas.height) {
      const r = record(this.canvas);
      r.frame = [];
      r.frames += 1;
    }
    return fillRect.call(this, x, y, w, h);
  };
  window.__ideTestGrids = () =>
    records
      .filter((r) => r.canvas.isConnected)
      .map((r) => {
        const box = r.canvas.getBoundingClientRect();
        return { frames: r.frames, texts: r.frame.filter((t) => t.text !== ''), box: { x: box.x, y: box.y } };
      });
}

// The painted texts of one canvas as rows of strings: one row per distinct y, top to
// bottom, each left to right.
function toRows(texts) {
  const byY = new Map();
  for (const t of texts) {
    const y = Math.round(t.y);
    if (!byY.has(y)) byY.set(y, []);
    byY.get(y).push(t);
  }
  return [...byY.keys()]
    .sort((a, b) => a - b)
    .map((y) => byY.get(y).sort((a, b) => a.x - b.x).map((t) => t.text));
}

const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// As toRows, keeping where each text was painted: rows of { text, x, y }, in the canvas.
function toCells(texts) {
  const byY = new Map();
  for (const t of texts) {
    const y = Math.round(t.y);
    if (!byY.has(y)) byY.set(y, []);
    byY.get(y).push(t);
  }
  return [...byY.keys()].sort((a, b) => a - b).map((y) => byY.get(y).sort((a, b) => a.x - b.x));
}

// Where the grid whose header row is `columns` painted its texts, in page coordinates:
// rows of { text, x, y }, the header first; undefined if no canvas shows it. A click at
// such a point lands in the cell (the grid paints a text inside its cell).
async function locateGrid(page, columns) {
  const grids = await page.evaluate(() => window.__ideTestGrids());
  for (const g of grids) {
    const rows = toCells(g.texts);
    if (rows.length && same(rows[0].map((t) => t.text), columns)) {
      return rows.map((row) => row.map((t) => ({ text: t.text, x: g.box.x + t.x, y: g.box.y + t.y })));
    }
  }
  return undefined;
}

// The grid whose header row is `columns`: its data rows, or undefined if no canvas shows it.
async function readGrid(page, columns) {
  const grids = await page.evaluate(() => window.__ideTestGrids());
  for (const g of grids) {
    const rows = toRows(g.texts);
    if (rows.length && same(rows[0], columns)) return rows.slice(1);
  }
  return undefined;
}

async function poll(timeout, what, fn) {
  const end = Date.now() + timeout;
  let last;
  for (;;) {
    last = await fn();
    if (last && last.done) return last.value;
    if (Date.now() > end) throw new Failure(`${what}${last && last.why ? `: ${last.why}` : ''}`);
    await new Promise((r) => setTimeout(r, 200));
  }
}

async function expectStaticGrid(page, spec, timeout) {
  await poll(timeout, `the grid of ${spec.name} did not show the expected cells`, async () => {
    const rows = await readGrid(page, spec.columns);
    if (rows === undefined) return { why: `no grid with the header ${JSON.stringify(spec.columns)} was painted` };
    if (!same(rows, spec.displayed)) return { why: `painted ${JSON.stringify(rows)}, expected ${JSON.stringify(spec.displayed)}` };
    return { done: true };
  });
}

const number = (s) => Number(String(s).replace(/,/g, ''));

// Checks one state of the ticking table (rows of [N, Sq]); returns the largest N.
function checkTickingState(state, spec) {
  if (state.length !== spec.rows) return { skip: `${state.length} rows` };
  for (let i = 0; i < state.length; i++) {
    const [n, sq] = state[i];
    if (!Number.isInteger(n) || !Number.isInteger(sq)) return { bad: `row ${i} is not numeric: ${JSON.stringify(state[i])}` };
    if (sq !== n * n) return { bad: `row ${i}: ${spec.square}=${sq} is not ${spec.counter}=${n} squared` };
    if (i > 0 && n !== state[i - 1][0] + 1) return { bad: `${spec.counter} is not consecutive: ${state.map((r) => r[0])}` };
  }
  return { top: state[state.length - 1][0] };
}

// Follows the ticking table until two valid states with a growing counter were seen.
// `read` returns the current state as rows of [N, Sq] numbers, or undefined.
async function expectTicking(spec, timeout, read) {
  let first;
  return poll(timeout, `${spec.name} did not show changing values`, async () => {
    const state = await read();
    if (state === undefined) return { why: `no grid with the header ${JSON.stringify(spec.columns)} was painted` };
    const check = checkTickingState(state, spec);
    if (check.bad) throw new Failure(`${spec.name}: ${check.bad}`);
    if (check.skip) return { why: `last state had ${check.skip}, expected ${spec.rows}` };
    if (first === undefined) {
      first = check.top;
      return { why: `${spec.counter} stayed at ${first}` };
    }
    if (check.top < first) throw new Failure(`${spec.name}: ${spec.counter} went back from ${first} to ${check.top}`);
    if (check.top === first) return { why: `${spec.counter} stayed at ${first}` };
    return { done: true, value: { from: first, to: check.top } };
  });
}

function tickingReader(page, spec) {
  const ni = spec.columns.indexOf(spec.counter);
  const si = spec.columns.indexOf(spec.square);
  return async () => {
    const rows = await readGrid(page, spec.columns);
    return rows === undefined ? undefined : rows.map((r) => [number(r[ni]), number(r[si])]);
  };
}

// ---------------------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------------------

async function waitForIde(page, timeout) {
  const panels = page.getByRole('button', { name: 'Panels' });
  try {
    await panels.waitFor({ timeout });
  } catch (_) {
    const text = (await page.evaluate(() => document.body.innerText)).replace(/\s+/g, ' ').slice(0, 300);
    const login = await page.locator('#auth-psk-token-input').count();
    throw new Failure(
      login
        ? `the IDE stayed on the login form (the key was not accepted); page says: ${text}`
        : `the IDE did not load its dashboard; page says: ${text}`,
    );
  }
}

// Opens a table from the Panels menu. Three things make one click not enough while the IDE
// starts: the login screen fades out over the dashboard and takes clicks until it is gone;
// the menu closes itself when the console takes the focus; and a menu opened before the
// server's field list arrived keeps showing what it had. So: open, try, close, again.
async function openFromPanels(page, name, timeout) {
  const menu = page.locator('.panels-menu-popper');
  const item = page.getByTestId(`panel-list-item-${name}-button`);
  const end = Date.now() + timeout;
  let listed = [];
  for (;;) {
    try {
      if (!(await menu.isVisible())) await page.getByRole('button', { name: 'Panels' }).click({ timeout: 2000 });
      await item.click({ timeout: 1500 });
      return;
    } catch (_) {
      if (await menu.isVisible()) {
        listed = (await page.locator('.widget-list li').allInnerTexts()).map((s) => s.trim());
        await page.keyboard.press('Escape');
        await menu.waitFor({ state: 'hidden', timeout: 2000 }).catch(() => {});
      }
    }
    if (Date.now() > end) {
      throw new Failure(`${name} could not be opened from the Panels list; it lists: ${JSON.stringify(listed)}`);
    }
  }
}

async function routePanels(ctx) {
  const { page, opts, expected, shot } = ctx;
  await page.goto(`${opts.url}/ide/`);
  if (opts.psk !== undefined) {
    const token = page.locator('#auth-psk-token-input');
    try {
      await token.waitFor({ timeout: opts.timeout });
    } catch (_) {
      throw new Failure('the IDE did not show the pre-shared key login form');
    }
    await token.fill(opts.psk);
    await page.getByTestId('btn-login').click();
  }
  await waitForIde(page, opts.timeout);
  ctx.trace.mark('open static');
  await openFromPanels(page, expected.static.name, opts.timeout);
  await expectStaticGrid(page, expected.static, opts.timeout);
  await shot('static');
  ctx.trace.mark('open ticking');
  await openFromPanels(page, expected.ticking.name, opts.timeout);
  const moved = await expectTicking(expected.ticking, opts.timeout, tickingReader(page, expected.ticking));
  await shot('ticking');
  return `grid cells of ${expected.static.name} match; ${expected.ticking.name} ${expected.ticking.counter} went ${moved.from} -> ${moved.to}`;
}

async function routeIframe(ctx) {
  const { page, opts, expected, shot } = ctx;
  const target = (name) =>
    `${opts.url}/iframe/table/?name=${encodeURIComponent(name)}` +
    (opts.psk !== undefined ? `&psk=${encodeURIComponent(opts.psk)}` : '');
  await page.goto(target(expected.static.name));
  await expectStaticGrid(page, expected.static, opts.timeout);
  await shot('static');
  ctx.trace.mark('open ticking');
  await page.goto(target(expected.ticking.name));
  const moved = await expectTicking(expected.ticking, opts.timeout, tickingReader(page, expected.ticking));
  await shot('ticking');
  return `grid cells of ${expected.static.name} match; ${expected.ticking.name} ${expected.ticking.counter} went ${moved.from} -> ${moved.to}`;
}

// Runs in the page. Reads through the JS API what the grid would ask for: the columns and
// a viewport of rows; for the ticking table the viewport after each update.
async function jsapiInPage({ origin, psk, handler, staticName, tickingName, counter, square, timeout }) {
  const withTimeout = (promise, what) =>
    Promise.race([
      promise,
      new Promise((_, reject) => setTimeout(() => reject(new Error(`timed out: ${what}`)), timeout)),
    ]);
  const plain = (v) => {
    if (v === null || v === undefined) return null;
    if (typeof v === 'object') return typeof v.asNumber === 'function' ? v.asNumber() : String(v);
    return v;
  };
  // The API rejects with its own event objects, which print as `Event_0`.
  const named = (promise, what) =>
    withTimeout(promise, what).catch((e) => {
      if (e instanceof Error) throw e;
      let detail;
      try {
        detail = JSON.stringify(e && e.detail !== undefined ? e.detail : e);
      } catch (_) {
        detail = String(e);
      }
      throw new Error(`${what} was rejected: ${e && e.type ? `${e.type} ` : ''}${detail}`);
    });
  const dh = (await import(`${origin}/jsapi/dh-core.js`)).default;
  const client = new dh.CoreClient(origin);
  await named(
    client.login(psk === null ? { type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS } : { type: handler, token: psk }),
    'login',
  );
  const connection = await named(client.getAsIdeConnection(), 'getAsIdeConnection');
  const out = {};

  const table = await named(connection.getObject({ type: 'Table', name: staticName }), `getObject ${staticName}`);
  out.columns = table.columns.map((c) => c.name);
  out.types = table.columns.map((c) => c.type);
  out.size = table.size;
  table.setViewport(0, 99);
  const data = await named(table.getViewportData(), `viewport of ${staticName}`);
  out.rows = data.rows.map((r) => table.columns.map((c) => plain(r.get(c))));

  const ticking = await named(connection.getObject({ type: 'Table', name: tickingName }), `getObject ${tickingName}`);
  out.tickingColumns = ticking.columns.map((c) => c.name);
  const n = ticking.findColumn(counter);
  const sq = ticking.findColumn(square);
  out.states = [];
  await withTimeout(
    new Promise((resolve) => {
      ticking.addEventListener(dh.Table.EVENT_UPDATED, (event) => {
        const state = event.detail.rows.map((r) => [plain(r.get(n)), plain(r.get(sq))]);
        const last = out.states[out.states.length - 1];
        if (JSON.stringify(last) !== JSON.stringify(state)) out.states.push(state);
        if (out.states.length >= 4) resolve();
      });
      ticking.setViewport(0, 99);
    }),
    `updates of ${tickingName}`,
  );
  table.close();
  ticking.close();

  const cell = (v) => `<td style="border:1px solid #888;padding:2px 8px">${String(v)}</td>`;
  document.body.innerHTML =
    `<h3>${staticName} through the JS API</h3><table>` +
    `<tr>${out.columns.map(cell).join('')}</tr>` +
    out.rows.map((r) => `<tr>${r.map(cell).join('')}</tr>`).join('') +
    `</table><h3>${tickingName}: ${counter}, ${square} per update</h3><pre>` +
    out.states.map((s) => JSON.stringify(s)).join('\n') +
    '</pre>';
  return out;
}

async function routeJsapi(ctx) {
  const { page, opts, expected, shot } = ctx;
  // Any document of the server's origin will do as the page that loads the API; a page
  // made up here would not be allowed to connect to a loopback server (Chromium's local
  // network access check). The manifest is small and the IDE needs it anyway.
  const response = await page.goto(`${opts.url}/ide/manifest.json`);
  if (!response || !response.ok()) {
    throw new Failure(`GET /ide/manifest.json answered ${response ? response.status() : 'nothing'}`);
  }
  let out;
  try {
    out = await page.evaluate(jsapiInPage, {
      origin: opts.url,
      psk: opts.psk === undefined ? null : opts.psk,
      handler: PSK_HANDLER,
      staticName: expected.static.name,
      tickingName: expected.ticking.name,
      counter: expected.ticking.counter,
      square: expected.ticking.square,
      timeout: opts.timeout,
    });
  } catch (e) {
    throw new Failure(`the JS API failed: ${String(e.message || e).split('\n')[0]}`);
  }
  await shot('tables');
  const s = expected.static;
  if (!same(out.columns, s.columns)) throw new Failure(`${s.name} has columns ${JSON.stringify(out.columns)}, expected ${JSON.stringify(s.columns)}`);
  if (!same(out.types, s.types)) throw new Failure(`${s.name} has column types ${JSON.stringify(out.types)}, expected ${JSON.stringify(s.types)}`);
  if (out.size !== s.rows.length) throw new Failure(`${s.name} has size ${out.size}, expected ${s.rows.length}`);
  if (!same(out.rows, s.rows)) throw new Failure(`${s.name} has rows ${JSON.stringify(out.rows)}, expected ${JSON.stringify(s.rows)}`);
  const t = expected.ticking;
  if (!same(out.tickingColumns, t.columns)) throw new Failure(`${t.name} has columns ${JSON.stringify(out.tickingColumns)}, expected ${JSON.stringify(t.columns)}`);
  const tops = [];
  for (const state of out.states) {
    const check = checkTickingState(state, t);
    if (check.bad) throw new Failure(`${t.name}: ${check.bad}`);
    if (check.top !== undefined) tops.push(check.top);
  }
  if (tops.length < 2) throw new Failure(`${t.name}: fewer than two updates with ${t.rows} rows: ${JSON.stringify(out.states)}`);
  for (let i = 1; i < tops.length; i++) {
    if (tops[i] <= tops[i - 1]) throw new Failure(`${t.name}: ${t.counter} did not grow: ${tops}`);
  }
  return `rows of ${s.name} match; ${t.name} ${t.counter} went ${tops[0]} -> ${tops[tops.length - 1]} over ${tops.length} updates`;
}

async function routeConsole(ctx) {
  const { page, opts, expected, shot } = ctx;
  await page.goto(`${opts.url}/ide/` + (opts.psk !== undefined ? `?psk=${encodeURIComponent(opts.psk)}` : ''));
  await waitForIde(page, opts.timeout);
  const editor = page.locator('.console-input .editor-container[data-mode-id]');
  try {
    await editor.waitFor({ timeout: opts.timeout });
  } catch (_) {
    throw new Failure('the IDE shows no console input (the server offers no console type)');
  }
  const language = await editor.getAttribute('data-mode-id');
  const commands = expected.console[language];
  if (!commands) throw new Failure(`expected.json has no console commands for the console type ${language}`);
  const run = async (command) => {
    await page.locator('.console-input .monaco-editor').first().click();
    await page.keyboard.insertText(command);
    await page.keyboard.press('Enter');
  };
  ctx.trace.mark('run static');
  await run(commands.static);
  await expectStaticGrid(page, Object.assign({}, expected.static, { name: expected.console.staticName }), opts.timeout);
  await shot('static');
  ctx.trace.mark('run ticking');
  await run(commands.ticking);
  const spec = Object.assign({}, expected.ticking, { name: expected.console.tickingName });
  const moved = await expectTicking(spec, opts.timeout, tickingReader(page, spec));
  await shot('ticking');
  return `${language} console: grid cells of ${expected.console.staticName} match; ${spec.name} ${spec.counter} went ${moved.from} -> ${moved.to}`;
}

const ROUTES = Object.assign(
  {
    panels: routePanels,
    iframe: routeIframe,
    jsapi: routeJsapi,
    console: routeConsole,
    rollup: (ctx) => routeRollup(ctx, { waitForIde, poll, Failure, locateGrid }),
    tree: (ctx) => routeTree(ctx, { waitForIde, poll, Failure, locateGrid }),
    format: (ctx) => require('./lib/format')(ctx, { waitForIde, poll, Failure }),
  },
  require('./lib/flows')({ Failure, poll, readGrid, locateGrid, expectStaticGrid, waitForIde, same }),
);

async function runRoute(browser, name, opts, expected) {
  const context = await browser.newContext({ viewport: { width: 1400, height: 900 }, ignoreHTTPSErrors: true });
  const answered = [];
  await faults.install(context, opts, (line) => answered.push(line));
  const page = await context.newPage();
  const trace = new Trace();
  await trace.attach(page);
  await page.addInitScript(gridHook);
  const shots = [];
  const shot = async (label) => {
    const file = path.join(opts.out, `${name}-${label}.png`);
    await page.screenshot({ path: file });
    shots.push(file);
  };
  const result = { route: name, ok: false };
  try {
    result.detail = await ROUTES[name]({ page, opts, expected, shot, trace });
    result.ok = true;
  } catch (e) {
    const unreachable = /net::ERR_[A-Z_]+/.exec(String(e.message));
    if (e instanceof Failure) result.detail = e.message;
    else if (unreachable) result.detail = `the server did not answer: ${unreachable[0]}`;
    else result.detail = `unexpected error: ${e.stack || e}`;
    await shot('FAILED').catch(() => {});
  }
  result.screenshots = shots;
  result.answeredHere = [...new Set(answered)];
  await trace.settle();
  const recorded = trace.toJSON();
  fs.writeFileSync(path.join(opts.out, `${name}.trace.json`), JSON.stringify(recorded));
  result.calls = recorded.calls.length;
  result.failedCalls = describeCalls(recorded.calls.filter((c) => c.status !== undefined && c.status !== 0));
  result.transports = [...new Set(recorded.calls.map((c) => c.transport + (c.httpProtocol ? `(${c.httpProtocol})` : '')))];
  result.failedRequests = recorded.static
    .filter((r) => r.failed || (r.status && r.status >= 400))
    .map((r) => `${r.method} ${r.url} -> ${r.failed || r.status}`);
  result.pageErrors = recorded.pageErrors.map((e) => e.text.split('\n')[0]);
  await context.close();
  return result;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  let expected;
  try {
    expected = JSON.parse(fs.readFileSync(opts.expected, 'utf8'));
  } catch (e) {
    usage(`cannot read ${opts.expected}: ${e.message}`);
  }
  fs.mkdirSync(opts.out, { recursive: true });
  let chromium;
  try {
    ({ chromium } = require('playwright'));
  } catch (e) {
    console.error(`error: cannot load playwright: ${e.message}`);
    process.exit(2);
  }
  const browser = await chromium.launch();
  const results = [];
  try {
    for (const name of opts.routes) {
      const r = await runRoute(browser, name, opts, expected);
      results.push(r);
      console.log(`${r.ok ? 'ok  ' : 'FAIL'} ${name}: ${r.detail}`);
      console.log(`     ${r.calls} gRPC calls over ${r.transports.join(', ') || 'nothing'}; screenshots: ${r.screenshots.join(', ') || 'none'}`);
      if (r.answeredHere.length) console.log(`     answered in the browser: ${r.answeredHere.join('; ')}`);
      if (!r.ok) {
        for (const line of r.failedRequests) console.log(`     request failed: ${line}`);
        for (const line of r.failedCalls) console.log(`     call failed: ${line}`);
        for (const line of r.pageErrors) console.log(`     page error: ${line}`);
      }
    }
  } finally {
    await browser.close();
  }
  fs.writeFileSync(path.join(opts.out, 'report.json'), JSON.stringify({ url: opts.url, results }, null, 2));
  const failed = results.filter((r) => !r.ok);
  console.log(failed.length ? `FAILED: ${failed.map((r) => r.route).join(', ')}` : `PASSED: ${results.map((r) => r.route).join(', ')}`);
  process.exit(failed.length ? 1 : 0);
}

if (require.main === module) {
  main().catch((e) => {
    console.error(`error: ${e.stack || e}`);
    process.exit(2);
  });
}

module.exports = { toRows, checkTickingState, parseArgs };
