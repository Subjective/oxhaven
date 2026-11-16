'use strict';

// Route format: a table with formats made in the console. The grid must paint each cell
// with the text and the colour of the text the formats give it: a colour of a column, of
// the rows where a condition holds, and a number format. What a cell is painted with is
// what `fillText` was called with, and the `fillStyle` it was called under.

module.exports = async function routeFormat(ctx, { waitForIde, poll, Failure }) {
  const { page, opts, expected, shot } = ctx;
  const spec = expected.format;
  await page.goto(`${opts.url}/ide/` + (opts.psk !== undefined ? `?psk=${encodeURIComponent(opts.psk)}` : ''));
  await waitForIde(page, opts.timeout);
  const editor = page.locator('.console-input .editor-container[data-mode-id]');
  try {
    await editor.waitFor({ timeout: opts.timeout });
  } catch (_) {
    throw new Failure('the IDE shows no console input (the server offers no console type)');
  }
  const language = await editor.getAttribute('data-mode-id');
  const command = (spec.commands || {})[language];
  if (!command) throw new Failure(`expected.json has no format command for the console type ${language}`);
  ctx.trace.mark('create');
  await page.locator('.console-input .monaco-editor').first().click();
  await page.keyboard.insertText(command);
  await page.keyboard.press('Enter');
  // Every painted text of the grid whose header is `columns`, under it, as [text, colour].
  const painted = async () => {
    const grids = await page.evaluate(() => window.__ideTestGrids());
    for (const g of grids) {
      const byY = new Map();
      for (const t of g.texts) {
        const y = Math.round(t.y);
        if (!byY.has(y)) byY.set(y, []);
        byY.get(y).push(t);
      }
      const rows = [...byY.keys()].sort((a, b) => a - b).map((y) => byY.get(y).sort((a, b) => a.x - b.x));
      if (rows.length && JSON.stringify(rows[0].map((t) => t.text)) === JSON.stringify(spec.columns)) {
        return rows.slice(1).map((row) => row.map((t) => [t.text, t.fill]));
      }
    }
    return undefined;
  };
  await poll(opts.timeout, `the grid of ${spec.name} did not paint the expected cells`, async () => {
    const cells = await painted();
    if (cells === undefined) return { why: `no grid with the header ${JSON.stringify(spec.columns)} was painted` };
    const top = cells.slice(0, spec.cells.length);
    if (JSON.stringify(top) !== JSON.stringify(spec.cells)) {
      return { why: `painted ${JSON.stringify(top)}, expected ${JSON.stringify(spec.cells)}` };
    }
    return { done: true };
  });
  await shot('formatted');
  return `${language} console: ${spec.name} painted with the texts and colours its formats give`;
};
