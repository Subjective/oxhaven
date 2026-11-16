// The rollup route of the IDE acceptance test: make a table in the console, make a rollup
// of it in the Rollup Rows panel of its grid, see the grid paint the grand total and the
// first level, expand a node by a click on its row and collapse it again, sort a column
// from its header, and filter by the value of a cell.
//
// The grid is a canvas, so a row is found by the text the IDE painted (see gridHook in
// ide-test.js) and clicked at the position of that text.
//
// The route checks, besides what the grid shows, that the panel called
// HierarchicalTableService/Rollup and that the sort and the filter each called
// HierarchicalTableService/Apply, and that the calls ended with OK.
//
// The tree route makes a tree in the console, which the IDE opens as it opens a table,
// expands a row by a click on its marker and collapses it again, sorts a column from its
// header, expands the row again and filters by the value of a cell of one of its children.
// It checks that the sort and the filter each called HierarchicalTableService/Apply.
//
// The selectors are those of web-client-ui v1.33.1, as in the other routes.
'use strict';

const short = (c) => `${c.service.replace(/^.*\./, '')}/${c.method}`;
const holds = (c, text) => c.requests.some((r) => Buffer.from(r, 'base64').includes(Buffer.from(text, 'utf8')));

// Opens the IDE and returns the language of its console and the command of `spec` for it.
async function openConsole(ctx, { waitForIde, Failure }, spec, what) {
  const { page, opts } = ctx;
  await page.goto(`${opts.url}/ide/` + (opts.psk !== undefined ? `?psk=${encodeURIComponent(opts.psk)}` : ''));
  await waitForIde(page, opts.timeout);
  const editor = page.locator('.console-input .editor-container[data-mode-id]');
  try {
    await editor.waitFor({ timeout: opts.timeout });
  } catch (_) {
    throw new Failure('the IDE shows no console input (the server offers no console type)');
  }
  const language = await editor.getAttribute('data-mode-id');
  const command = spec.commands[language];
  if (!command) throw new Failure(`expected.json has no ${what} command for the console type ${language}`);
  return { language, command };
}

// Types `command` into the console and runs it.
async function runCommand(page, command) {
  await page.locator('.console-input .monaco-editor').first().click();
  await page.keyboard.insertText(command);
  await page.keyboard.press('Enter');
}

// What both routes do with a grid of a hierarchical table.
function gridOf(ctx, { poll, Failure, locateGrid }) {
  const { page, opts } = ctx;
  // Waits until the grid with the header `columns` paints `want` under it; returns where
  // it painted each text, the header first.
  const expectRows = (columns, label, want) =>
    poll(opts.timeout, `the grid did not show ${label}`, async () => {
      const painted = await locateGrid(page, columns);
      if (painted === undefined) return { why: `no grid with the header ${JSON.stringify(columns)} was painted` };
      const got = painted.slice(1).map((row) => row.map((cell) => cell.text));
      if (JSON.stringify(got) !== JSON.stringify(want)) {
        return { why: `painted ${JSON.stringify(got)}, expected ${JSON.stringify(want)}` };
      }
      return { done: true, value: painted };
    });
  const rowOf = (painted, text) => {
    const row = painted.slice(1).find((r) => r[0].text === text);
    if (!row) throw new Failure(`no painted row starts with ${text}`);
    return row;
  };
  // Clicks the tree marker of the row whose first cell is `text`: the marker is painted
  // left of the text of the first column.
  const toggle = async (painted, text) => {
    const cell = rowOf(painted, text)[0];
    await page.mouse.click(cell.x - 10, cell.y);
  };
  // Waits for a call `name` made after call number `from` whose request holds every text
  // of `texts`, and requires that it ended with OK; returns the number of calls seen.
  const expectCall = async (name, texts, from) => {
    let found;
    let count = 0;
    await poll(opts.timeout, `the IDE did not call ${name} with ${JSON.stringify(texts)}`, async () => {
      await ctx.trace.settle();
      const all = ctx.trace.calls();
      count = all.length;
      found = all.slice(from).find((c) => short(c) === name && texts.every((x) => holds(c, x)) && c.status !== undefined);
      return found ? { done: true } : { why: `calls since: ${all.slice(from).map(short).join(', ') || 'none'}` };
    });
    if (found.status !== 0) {
      throw new Failure(`${name} with ${JSON.stringify(texts)} ended with status ${found.status}: ${found.message || ''}`);
    }
    return count;
  };
  const menuEntry = (text) => page.locator('.context-menu-container button', { hasText: text });
  // Sorts `column` descending from the header painted in `painted`, whose columns are
  // `columns`.
  const sortDescending = async (painted, columns, column) => {
    const header = painted[0][columns.indexOf(column)];
    await page.mouse.click(header.x, header.y, { button: 'right' });
    await menuEntry('Sort by').hover();
    await menuEntry(new RegExp(`^${column} Descending`)).click();
  };
  // Filters by the value of the cell number `filter.cell` of the row of `filter.row`,
  // or of `filter.value` when the row starts with the value.
  const filterByValue = async (painted, filter) => {
    const cell = rowOf(painted, filter.row === undefined ? filter.value : filter.row)[filter.cell];
    if (cell.text !== filter.value) throw new Failure(`the cell to filter by shows ${cell.text}, expected ${filter.value}`);
    await page.mouse.click(cell.x, cell.y, { button: 'right' });
    await menuEntry('Filter by Value').hover();
    await menuEntry(new RegExp(`^${filter.entry}`)).click();
  };
  return { expectRows, rowOf, toggle, expectCall, sortDescending, filterByValue };
}

async function routeRollup(ctx, deps) {
  const { poll, Failure } = deps;
  const { page, opts, expected, shot } = ctx;
  const spec = expected.rollup;
  if (!spec) throw new Failure('expected.json has no rollup section');
  const { language, command } = await openConsole(ctx, deps, spec, 'rollup');
  const { expectRows, toggle, expectCall, sortDescending, filterByValue } = gridOf(ctx, deps);

  ctx.trace.mark('create');
  await runCommand(page, command);
  await expectRows(spec.source.columns, 'the table to roll up', spec.source.rows);
  await ctx.trace.settle();
  let seen = ctx.trace.calls().length;

  // The panel makes the rollup anew whenever its columns or its options change. Without
  // the constituents: the rollup then has the aggregated rows only.
  ctx.trace.mark('rollup');
  await page.locator('.iris-grid button[aria-label="Table Options"]').first().click();
  await page.locator('[data-testid="menu-item-Rollup Rows"]').click();
  const panel = page.locator('.rollup-rows');
  await panel.waitFor({ timeout: opts.timeout });
  for (const option of spec.without) {
    const box = panel.locator('.custom-checkbox', { hasText: option });
    await box.locator('label').click();
    if (await box.locator('input').isChecked()) throw new Failure(`the option ${option} of Rollup Rows is still checked`);
  }
  for (const column of spec.groupBy) {
    const item = panel.locator('.rollup-available-grouping-columns .draggable-item-list-item', {
      has: page.locator('.title', { hasText: new RegExp(`^${column}$`) }),
    });
    await item.dblclick();
    await poll(opts.timeout, `Rollup Rows does not group by ${column}`, async () => {
      const grouped = await panel.locator('.rollup-rows-group-by').innerText();
      return grouped.includes(column) ? { done: true } : { why: `Group By shows ${JSON.stringify(grouped)}` };
    });
  }
  seen = await expectCall('HierarchicalTableService/Rollup', spec.groupBy, seen);
  await page.locator('.table-sidebar [data-testid="btn-page-close"]').first().click();
  let painted = await expectRows(spec.columns, 'the grand total and the first level', spec.collapsed);
  await shot('collapsed');

  ctx.trace.mark('expand');
  await toggle(painted, spec.expand);
  painted = await expectRows(spec.columns, `the children of ${spec.expand}`, spec.expanded);
  await shot('expanded');
  ctx.trace.mark('collapse');
  await toggle(painted, spec.expand);
  painted = await expectRows(spec.columns, `${spec.expand} collapsed again`, spec.collapsed);
  await ctx.trace.settle();
  seen = ctx.trace.calls().length;

  ctx.trace.mark('sort');
  await sortDescending(painted, spec.columns, spec.sort.column);
  seen = await expectCall('HierarchicalTableService/Apply', [spec.sort.column], seen);
  painted = await expectRows(spec.columns, `the rollup sorted by ${spec.sort.column}, descending`, spec.sort.rows);
  await shot('sorted');

  ctx.trace.mark('filter');
  await filterByValue(painted, spec.filter);
  await expectCall('HierarchicalTableService/Apply', [spec.filter.column], seen);
  await expectRows(spec.columns, `the rollup where ${spec.filter.column} is ${spec.filter.value}, still sorted`, spec.filter.rows);
  await shot('filtered');
  return `${language} console: Rollup Rows by ${spec.groupBy.join(', ')} painted ${spec.collapsed.length} rows, ${spec.expanded.length} with ${spec.expand} expanded; sorted by ${spec.sort.column} descending; filtered to ${spec.filter.column} = ${spec.filter.value}`;
}

async function routeTree(ctx, deps) {
  const { Failure } = deps;
  const { page, expected, shot } = ctx;
  const spec = expected.tree;
  if (!spec) throw new Failure('expected.json has no tree section');
  const { language, command } = await openConsole(ctx, deps, spec, 'tree');
  const { expectRows, toggle, expectCall, sortDescending, filterByValue } = gridOf(ctx, deps);

  ctx.trace.mark('create');
  await runCommand(page, command);
  let painted = await expectRows(spec.columns, 'the rows without a parent', spec.collapsed);
  await shot('collapsed');

  ctx.trace.mark('expand');
  await toggle(painted, spec.expand);
  painted = await expectRows(spec.columns, `the children of ${spec.expand}`, spec.expanded);
  await shot('expanded');
  ctx.trace.mark('collapse');
  await toggle(painted, spec.expand);
  painted = await expectRows(spec.columns, `${spec.expand} collapsed again`, spec.collapsed);
  await ctx.trace.settle();
  let seen = ctx.trace.calls().length;

  ctx.trace.mark('sort');
  await sortDescending(painted, spec.columns, spec.sort.column);
  seen = await expectCall('HierarchicalTableService/Apply', [spec.sort.column], seen);
  painted = await expectRows(spec.columns, `the tree sorted by ${spec.sort.column}, descending`, spec.sort.rows);
  await toggle(painted, spec.expand);
  painted = await expectRows(spec.columns, `the children of ${spec.expand}, sorted`, spec.sort.expanded);
  await shot('sorted');

  ctx.trace.mark('filter');
  await filterByValue(painted, spec.filter);
  await expectCall('HierarchicalTableService/Apply', [spec.filter.column], seen);
  await expectRows(spec.columns, `the tree where ${spec.filter.column} is ${spec.filter.value}, still sorted`, spec.filter.rows);
  await shot('filtered');
  return `${language} console: the tree painted ${spec.collapsed.length} rows, ${spec.expanded.length} with ${spec.expand} expanded; sorted by ${spec.sort.column} descending; filtered to ${spec.filter.column} = ${spec.filter.value}`;
}

module.exports = { routeRollup, routeTree };
