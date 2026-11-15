// The routes of ide-test.js that do what a user does around the console: notebooks in the
// File Explorer, the Log and Command History panels, sorting, filtering and scrolling a
// grid, commands that fail, a variable that goes away, and a server that restarts.
//
// Each route drives the IDE as a user would (clicks, keys, the context menus) and checks two
// things: what the IDE shows afterwards, and that the gRPC calls the step is there for were
// made and ended with OK. The calls each step makes on the Java reference are listed in
// REQUIREMENTS.md, "What the flows around the console add".
//
// The selectors are those of web-client-ui v1.33.1, as in the other routes.
'use strict';

const { execFile } = require('child_process');

module.exports = (t) => {
  const { Failure, poll, readGrid, locateGrid, expectStaticGrid, waitForIde } = t;

  // ---------------------------------------------------------------------------------------
  // Shared steps
  // ---------------------------------------------------------------------------------------

  // Opens the IDE and waits for the console input; returns the console's language.
  async function openConsole(ctx) {
    const { page, opts } = ctx;
    await page.goto(`${opts.url}/ide/` + (opts.psk !== undefined ? `?psk=${encodeURIComponent(opts.psk)}` : ''));
    await waitForIde(page, opts.timeout);
    const editor = page.locator('.console-input .editor-container[data-mode-id]');
    try {
      await editor.waitFor({ timeout: opts.timeout });
    } catch (_) {
      throw new Failure('the IDE shows no console input (the server offers no console type)');
    }
    return editor.getAttribute('data-mode-id');
  }

  function commandsFor(ctx, section, language) {
    const commands = (ctx.expected[section] || {})[language];
    if (!commands) throw new Failure(`expected.json has no ${section} commands for the console type ${language}`);
    return commands;
  }

  async function runCommand(page, command) {
    await page.locator('.console-input .monaco-editor').first().click();
    await page.keyboard.insertText(command);
    await page.keyboard.press('Enter');
  }

  // The gRPC calls of the page so far.
  async function calls(ctx) {
    await ctx.trace.settle();
    return ctx.trace.calls();
  }

  const short = (c) => `${c.service.replace(/^.*\./, '')}/${c.method}`;
  const holds = (c, text) => c.requests.some((r) => Buffer.from(r, 'base64').includes(Buffer.from(text, 'utf8')));

  // Waits for a call `Service/Method` made after call number `from` whose request holds
  // every text of `texts`, and requires that it ended with OK. Returns the number of calls
  // seen, to pass as `from` of the next step.
  async function expectCall(ctx, name, texts, from) {
    let found;
    let count = 0;
    await poll(ctx.opts.timeout, `the IDE did not call ${name} with ${JSON.stringify(texts)}`, async () => {
      const all = await calls(ctx);
      count = all.length;
      found = all.slice(from).find((c) => short(c) === name && texts.every((x) => holds(c, x)) && c.status !== undefined);
      return found ? { done: true } : { why: `calls since: ${all.slice(from).map(short).join(', ') || 'none'}` };
    });
    if (found.status !== 0) {
      throw new Failure(`${name} with ${JSON.stringify(texts)} ended with status ${found.status}: ${found.message || ''}`);
    }
    return count;
  }

  async function countCalls(ctx) {
    return (await calls(ctx)).length;
  }

  // What a command printed or returned, and its error, as the console shows them.
  const consoleText = (page) => page.locator('.console-history').innerText();

  async function expectText(ctx, what, read, text) {
    await poll(ctx.opts.timeout, `${what} does not show ${JSON.stringify(text)}`, async () => {
      const shown = await read();
      return shown.includes(text) ? { done: true } : { why: `it shows ${JSON.stringify(shown.replace(/\s+/g, ' ').slice(-300))}` };
    });
  }

  const tab = (page, title) => page.locator('.lm_tab', { hasText: title });

  // A run of the test must not trip over what an earlier run left behind.
  const unique = () => `${Date.now().toString(36)}${Math.floor(Math.random() * 1296).toString(36)}`;

  // ---------------------------------------------------------------------------------------
  // notebook: the File Explorer and the notebook editor
  // ---------------------------------------------------------------------------------------

  const fileItem = (page, name) => page.locator('.file-list-item', { hasText: name });

  async function contextMenu(page, name, entry) {
    await fileItem(page, name).first().click({ button: 'right' });
    await page.locator('.context-menu-container button', { hasText: entry }).click();
  }

  async function expectListed(ctx, name, listed) {
    await poll(ctx.opts.timeout, `the File Explorer ${listed ? 'does not list' : 'still lists'} ${name}`, async () => {
      const count = await fileItem(ctx.page, name).count();
      return (count > 0) === listed ? { done: true } : {};
    });
  }

  async function routeNotebook(ctx) {
    const { page, opts, expected, shot } = ctx;
    const language = await openConsole(ctx);
    const notebook = commandsFor(ctx, 'notebook', language);
    const id = unique();
    const dir = `ide_dir_${id}`;
    const file = `ide_nb_${id}.${notebook.extension}`;
    const renamed = `ide_renamed_${id}.${notebook.extension}`;
    const notebookEditor = () => page.locator('.notebook-container .monaco-editor, .notebook-editor .monaco-editor').last();

    await tab(page, 'File Explorer').click();
    let seen = await countCalls(ctx);

    ctx.trace.mark('new folder');
    await page.getByRole('button', { name: 'New folder' }).click();
    await page.locator('.modal input.form-control').fill(dir);
    await page.locator('.modal .btn-primary').click();
    seen = await expectCall(ctx, 'StorageService/CreateDirectory', [dir], seen);
    await expectListed(ctx, dir, true);

    ctx.trace.mark('new notebook');
    await page.getByRole('button', { name: 'New notebook' }).click();
    await page.getByRole('button', { name: 'Run Selected' }).waitFor({ timeout: opts.timeout });
    await notebookEditor().click();
    await page.keyboard.insertText(notebook.lines.join('\n'));

    // The cursor is at the end of the last line: select that line and run it alone.
    ctx.trace.mark('run selected');
    const last = notebook.lines[notebook.lines.length - 1];
    await page.keyboard.press('Shift+Home');
    await page.getByRole('button', { name: 'Run Selected' }).click();
    seen = await expectCall(ctx, 'ConsoleService/ExecuteCommand', [last], seen);
    const ran = (await calls(ctx)).filter((c) => short(c) === 'ConsoleService/ExecuteCommand').pop();
    if (holds(ran, notebook.lines[0])) throw new Failure('Run Selected sent the whole notebook, not the selected line');
    await expectStaticGrid(page, notebook.selected, opts.timeout);
    await shot('selected');

    ctx.trace.mark('run');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    seen = await expectCall(ctx, 'ConsoleService/ExecuteCommand', notebook.lines, seen);
    // the new table's panel opens behind the one that is showing
    await tab(page, notebook.staticName).click();
    await expectStaticGrid(page, Object.assign({}, expected.static, { name: notebook.staticName }), opts.timeout);
    await shot('static');

    ctx.trace.mark('save as');
    await notebookEditor().click();
    await page.keyboard.press('Control+s');
    await page.locator('.new-file-modal input.form-control').fill(file);
    await page.locator('.new-file-modal .btn-primary').click();
    seen = await expectCall(ctx, 'StorageService/SaveFile', [file, last], seen);
    await expectListed(ctx, file, true);

    ctx.trace.mark('close and open');
    await tab(page, file).locator('.lm_close_tab').click();
    await tab(page, file).waitFor({ state: 'detached', timeout: opts.timeout });
    await fileItem(page, file).dblclick();
    seen = await expectCall(ctx, 'StorageService/FetchFile', [file], seen);
    await tab(page, file).waitFor({ timeout: opts.timeout });
    await expectText(ctx, 'the notebook opened from the File Explorer', () => notebookEditor().innerText().then((s) => s.replace(/ /g, ' ')), last);

    ctx.trace.mark('save');
    await notebookEditor().click();
    await page.keyboard.press('Control+End');
    await page.keyboard.insertText(`\n${notebook.comment} ${id}`);
    await page.keyboard.press('Control+s');
    seen = await expectCall(ctx, 'StorageService/SaveFile', [file, `${notebook.comment} ${id}`], seen);

    ctx.trace.mark('rename');
    await contextMenu(page, file, 'Rename');
    const input = page.locator('input.file-list-item-editor-input');
    await input.fill(renamed);
    await input.press('Enter');
    seen = await expectCall(ctx, 'StorageService/MoveItem', [file, renamed], seen);
    await expectListed(ctx, renamed, true);
    await expectListed(ctx, file, false);

    ctx.trace.mark('move');
    await fileItem(page, renamed).dragTo(fileItem(page, dir));
    seen = await expectCall(ctx, 'StorageService/MoveItem', [renamed, `${dir}/${renamed}`], seen);
    await shot('moved');

    ctx.trace.mark('delete');
    // the moved file is listed again once its folder is open
    if (!(await fileItem(page, renamed).count())) await fileItem(page, dir).click();
    await expectListed(ctx, renamed, true);
    await contextMenu(page, renamed, 'Delete');
    await page.getByRole('button', { name: 'Delete', exact: true }).click();
    seen = await expectCall(ctx, 'StorageService/DeleteItem', [`${dir}/${renamed}`], seen);
    await expectListed(ctx, renamed, false);
    await contextMenu(page, dir, 'Delete');
    await page.getByRole('button', { name: 'Delete', exact: true }).click();
    await expectCall(ctx, 'StorageService/DeleteItem', [dir], seen);
    await expectListed(ctx, dir, false);
    return `${language} notebook: created, run (selection and whole), saved, opened, renamed, moved and deleted ${file}`;
  }

  // ---------------------------------------------------------------------------------------
  // log: the Log panel and the Command History panel
  // ---------------------------------------------------------------------------------------

  // The Log panel and the console input are Monaco editors: only the lines in view are in
  // the DOM, and a space is painted as a no-break space.
  const editorText = (locator) => locator.innerText().then((s) => s.replace(/ /g, ' '));

  async function routeLog(ctx) {
    const { page, opts, shot } = ctx;
    const language = await openConsole(ctx);
    const marker = `ide-log-${unique()}`;
    const command = commandsFor(ctx, 'log', language).print.replace('{marker}', marker);
    let seen = await countCalls(ctx);

    ctx.trace.mark('print');
    await runCommand(page, command);
    seen = await expectCall(ctx, 'ConsoleService/ExecuteCommand', [marker], seen);
    // the command itself and, under it, what it printed
    await poll(opts.timeout, 'the console does not show what the command printed', async () => {
      const shown = (await consoleText(page)).split(marker).length - 1;
      return shown >= 2 ? { done: true } : { why: `the marker is shown ${shown} times, expected the command and its output` };
    });

    // The Log panel shows the server's log stream, which carries what scripts print. It
    // follows the end of the log, and a server that logs a lot moves the line out of view,
    // so the line is looked for with the editor's own search.
    ctx.trace.mark('log panel');
    await tab(page, 'Log').click();
    const log = page.locator('.log-pane-editor');
    await log.waitFor({ timeout: opts.timeout });
    await poll(opts.timeout, `the Log panel does not show a line of level STDOUT with ${marker}`, async () => {
      await log.locator('.monaco-editor').first().click();
      await page.keyboard.press('Control+f');
      const find = log.locator('.find-widget textarea[aria-label="Find"]');
      await find.fill(marker);
      await find.press('Enter');
      const line = (await editorText(log.locator('.view-lines')))
        .split('\n')
        .find((l) => l.includes(marker) && l.includes('STDOUT'));
      await page.keyboard.press('Escape');
      return line ? { done: true } : {};
    });
    await shot('log');

    // Command History lists what was run; it is kept by the browser, not by the server.
    ctx.trace.mark('command history');
    await tab(page, 'Console').click();
    await tab(page, 'Command History').click();
    const history = page.locator('.command-history');
    await history.locator('input.form-control').fill(marker);
    const item = history.locator('.command-history-item');
    await poll(opts.timeout, `Command History does not list the one command with ${marker}`, async () => {
      const texts = await item.allInnerTexts();
      return texts.length === 1 && texts[0].includes(marker) ? { done: true } : { why: `it lists ${JSON.stringify(texts)}` };
    });
    await item.click();
    await history.getByRole('button', { name: 'Open selected commands in the console' }).click();
    await expectText(ctx, 'the console input', () => editorText(page.locator('.console-input .view-lines')), command);
    await shot('history');
    await page.locator('.console-input .monaco-editor').first().click();
    await page.keyboard.press('Enter');
    await expectCall(ctx, 'ConsoleService/ExecuteCommand', [marker], seen);
    return `${language} console: ${marker} is in the Log panel as STDOUT and in Command History, and ran again from there`;
  }

  // ---------------------------------------------------------------------------------------
  // grid: scroll, sort and filter a table from its grid
  // ---------------------------------------------------------------------------------------

  // Waits until the first rows the grid paints under the header `columns` are `rows`.
  async function expectTopRows(ctx, columns, rows, what) {
    await poll(ctx.opts.timeout, `the grid does not show ${what}`, async () => {
      const painted = await readGrid(ctx.page, columns);
      if (painted === undefined) return { why: `no grid with the header ${JSON.stringify(columns)} was painted` };
      const top = painted.slice(0, rows.length);
      return t.same(top, rows) ? { done: true } : { why: `its first rows are ${JSON.stringify(top)}, expected ${JSON.stringify(rows)}` };
    });
  }

  async function located(ctx, columns) {
    const cells = await locateGrid(ctx.page, columns);
    if (!cells) throw new Failure(`no grid with the header ${JSON.stringify(columns)} is painted`);
    return cells;
  }

  async function menuEntry(page, text) {
    return page.locator('.context-menu-container button', { hasText: text });
  }

  async function routeGrid(ctx) {
    const { page, opts, expected, shot } = ctx;
    const language = await openConsole(ctx);
    const grid = expected.grid;
    const create = commandsFor(ctx, 'grid', language).create;
    const { columns } = grid;
    let seen = await countCalls(ctx);

    ctx.trace.mark('create');
    await runCommand(page, create);
    await expectTopRows(ctx, columns, grid.top, `the first rows of ${grid.name}`);
    seen = await countCalls(ctx);

    // Scrolling moves the viewport of the subscription the grid has: a further request on
    // the same DoExchange, and rows the first viewport did not hold.
    ctx.trace.mark('scroll');
    const header = (await located(ctx, columns))[0];
    await page.mouse.move(header[0].x + 100, header[0].y + 150);
    await page.mouse.wheel(0, 1000000);
    await poll(opts.timeout, `the grid does not show the last row of ${grid.name} after scrolling to the end`, async () => {
      const painted = (await readGrid(page, columns)) || [];
      const last = painted[painted.length - 1];
      return t.same(last, grid.bottom) ? { done: true } : { why: `its last row is ${JSON.stringify(last)}, expected ${JSON.stringify(grid.bottom)}` };
    });
    await shot('scrolled');
    const exchange = (await calls(ctx)).filter((c) => short(c) === 'FlightService/DoExchange' || short(c) === 'BrowserFlightService/NextDoExchange');
    const moved = exchange.reduce((n, c) => n + c.requests.length, 0);
    if (moved < 2) throw new Failure(`scrolling sent no further subscription request (${moved} DoExchange messages in all)`);
    await page.mouse.wheel(0, -1000000);
    await expectTopRows(ctx, columns, grid.top, `the first rows of ${grid.name} after scrolling back`);

    ctx.trace.mark('sort');
    const sortColumn = header[columns.indexOf(grid.sort.column)];
    await page.mouse.click(sortColumn.x, sortColumn.y, { button: 'right' });
    await (await menuEntry(page, 'Sort by')).hover();
    await (await menuEntry(page, new RegExp(`^${grid.sort.column} Descending`))).click();
    seen = await expectCall(ctx, 'TableService/Batch', [grid.sort.column], seen);
    await expectTopRows(ctx, columns, grid.sort.top, `${grid.name} sorted by ${grid.sort.column}, descending`);
    await shot('sorted');

    ctx.trace.mark('filter');
    const first = (await located(ctx, columns))[1];
    const cell = first[columns.indexOf(grid.filter.column)];
    if (cell.text !== grid.filter.value) throw new Failure(`the cell to filter by shows ${cell.text}, expected ${grid.filter.value}`);
    await page.mouse.click(cell.x, cell.y, { button: 'right' });
    await (await menuEntry(page, 'Filter by Value')).hover();
    await (await menuEntry(page, /^is equal to/)).click();
    await expectCall(ctx, 'TableService/Batch', [grid.filter.column], seen);
    await expectTopRows(ctx, columns, grid.filter.top, `${grid.name} where ${grid.filter.column} is ${grid.filter.value}, still sorted`);
    await shot('filtered');
    return `${language} console: ${grid.name} scrolled to row ${grid.bottom[0]} and back, sorted by ${grid.sort.column} descending, filtered to ${grid.filter.column} = ${grid.filter.value}`;
  }

  return { notebook: routeNotebook, log: routeLog, grid: routeGrid };
};

void execFile;
