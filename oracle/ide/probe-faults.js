#!/usr/bin/env node
// Finds out which calls the IDE cannot do without: runs ide-test.js once per call with that
// call answered UNIMPLEMENTED in the browser (and once answered with an empty message), and
// prints which routes still pass. Run it against the Java reference; REQUIREMENTS.md quotes
// its output.
//
//   node oracle/ide/probe-faults.js --url dh+psk://oxhaven@127.0.0.1:10500 [--routes panels,iframe,jsapi]
//                                   [--timeout 12000] [--base-empty ConsoleService/GetConsoleTypes]
//
// --base-empty is applied to every run. The default takes the console away (the IDE then
// starts no console), so that the matrix describes a server without one.
'use strict';

const { spawnSync } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

const CALLS = [
  'ConfigService/GetAuthenticationConstants',
  'ConfigService/GetConfigurationConstants',
  'SessionService/TerminationNotification',
  'SessionService/Release',
  'TableService/ExportedTableUpdates',
  'StorageService/ListItems',
  'ApplicationService/ListFields',
  'ConsoleService/GetConsoleTypes',
  'TableService/FetchTable',
  'TableService/ApplyPreviewColumns',
  'TableService/Batch',
  'FlightService/DoExchange',
  'FlightService/DoGet',
  'ObjectService/FetchObject',
];

function arg(name, fallback) {
  const i = process.argv.indexOf(name);
  return i < 0 ? fallback : process.argv[i + 1];
}

const url = arg('--url');
if (!url) {
  console.error('usage: node oracle/ide/probe-faults.js --url <server> [--routes ...] [--timeout ms] [--base-empty rules]');
  process.exit(2);
}
const routes = arg('--routes', 'panels,iframe,jsapi').split(',');
const timeout = arg('--timeout', '12000');
const baseEmpty = arg('--base-empty', 'ConsoleService/GetConsoleTypes');
const out = fs.mkdtempSync(path.join(os.tmpdir(), 'ide-probe-'));

function run(extra) {
  const args = [path.join(__dirname, 'ide-test.js'), '--url', url, '--routes', routes.join(','), '--timeout', timeout, '--out', out];
  const result = spawnSync(process.execPath, args.concat(extra), { encoding: 'utf8' });
  if (result.status !== 0 && result.status !== 1) {
    console.error(result.stdout + result.stderr);
    process.exit(2);
  }
  const verdict = {};
  for (const route of routes) {
    const line = result.stdout.split('\n').find((l) => new RegExp(`^(ok  |FAIL) ${route}:`).test(l)) || '';
    verdict[route] = line.startsWith('ok') ? 'ok' : `FAIL (${line.replace(/^FAIL \w+: /, '').slice(0, 90)})`;
  }
  return verdict;
}

function show(label, verdict) {
  console.log(label);
  for (const route of routes) console.log(`    ${route.padEnd(8)} ${verdict[route]}`);
}

const without = (rule) => baseEmpty.split(',').filter((r) => r && r !== rule).join(',');
show(`baseline (--empty ${baseEmpty})`, run(baseEmpty ? ['--empty', baseEmpty] : []));
for (const call of CALLS) {
  const base = without(call);
  show(`--deny ${call}`, run(['--deny', call].concat(base ? ['--empty', base] : [])));
}
for (const call of CALLS) {
  if (baseEmpty.split(',').includes(call)) continue;
  show(`--empty ${call}`, run(['--empty', [call, baseEmpty].filter(Boolean).join(',')]));
}
fs.rmSync(out, { recursive: true, force: true });
