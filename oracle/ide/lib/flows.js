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
  const { Failure, poll, readGrid, expectStaticGrid, waitForIde } = t;

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

  return { notebook: routeNotebook };
};

void execFile;
