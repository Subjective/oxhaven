// The rollup route of the IDE acceptance test: type a command that makes a rollup into the
// console, see the grid paint the grand total and the first level, expand a node by a
// click on its row, see its children, and collapse it again.
//
// The grid is a canvas, so a row is found by the text the IDE painted (see gridHook in
// ide-test.js) and clicked at the position of that text.
'use strict';

// The painted texts of the grid whose header row starts with `columns`, as rows of
// { y, cells: [{ text, x }] }; undefined if no canvas shows such a grid.
async function readPainted(page, columns) {
  const grids = await page.evaluate(() => window.__ideTestGrids());
  for (const g of grids) {
    const byY = new Map();
    for (const t of g.texts) {
      const y = Math.round(t.y);
      if (!byY.has(y)) byY.set(y, []);
      byY.get(y).push(t);
    }
    const rows = [...byY.keys()]
      .sort((a, b) => a - b)
      .map((y) => ({ y, cells: byY.get(y).sort((a, b) => a.x - b.x) }));
    if (rows.length && JSON.stringify(rows[0].cells.map((c) => c.text)) === JSON.stringify(columns)) {
      const box = await page.evaluate((frames) => {
        for (const canvas of document.querySelectorAll('canvas')) {
          const r = canvas.getBoundingClientRect();
          if (r.width > 0 && canvas.__ideTestFrames === frames) return { x: r.x, y: r.y, scale: canvas.width / r.width };
        }
        return undefined;
      }, g.id);
      return { rows: rows.slice(1), box };
    }
  }
  return undefined;
}

const texts = (painted) => painted.rows.map((r) => r.cells.map((c) => c.text));

async function routeRollup(ctx, { waitForIde, poll, Failure }) {
  const { page, opts, expected, shot } = ctx;
  const spec = expected.rollup;
  if (!spec) throw new Failure('expected.json has no rollup section');
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
  if (!command) throw new Failure(`expected.json has no rollup command for the console type ${language}`);

  const expectRows = (label, want) =>
    poll(opts.timeout, `the rollup grid did not show ${label}`, async () => {
      const painted = await readPainted(page, spec.columns);
      if (painted === undefined) return { why: `no grid with the header ${JSON.stringify(spec.columns)} was painted` };
      const got = texts(painted);
      if (JSON.stringify(got) !== JSON.stringify(want)) {
        return { why: `painted ${JSON.stringify(got)}, expected ${JSON.stringify(want)}` };
      }
      return { done: true, value: painted };
    });
  // Clicks the tree marker of the row whose first cell is `text`: the marker is painted
  // left of the text of the first column.
  const toggle = async (painted, text) => {
    const row = painted.rows.find((r) => r.cells[0].text === text);
    if (!row) throw new Failure(`no painted row starts with ${text}`);
    const canvas = page.locator('.iris-grid canvas.grid-canvas').first();
    const box = await canvas.boundingBox();
    const scale = await canvas.evaluate((c) => c.width / c.getBoundingClientRect().width);
    await page.mouse.click(box.x + row.cells[0].x / scale - 10, box.y + row.y / scale);
  };

  ctx.trace.mark('run rollup');
  await page.locator('.console-input .monaco-editor').first().click();
  await page.keyboard.insertText(command);
  await page.keyboard.press('Enter');
  let painted = await expectRows('the grand total and the first level', spec.collapsed);
  await shot('collapsed');
  ctx.trace.mark('expand');
  await toggle(painted, spec.expand);
  painted = await expectRows(`the children of ${spec.expand}`, spec.expanded);
  await shot('expanded');
  ctx.trace.mark('collapse');
  await toggle(painted, spec.expand);
  await expectRows(`${spec.expand} collapsed again`, spec.collapsed);
  return `${language} console: rollup painted ${spec.collapsed.length} rows, ${spec.expanded.length} with ${spec.expand} expanded, ${spec.collapsed.length} after collapsing it`;
}

module.exports = { routeRollup };
