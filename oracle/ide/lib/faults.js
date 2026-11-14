// Answers chosen gRPC calls in the browser instead of the server, to see what the IDE does
// when a server lacks a method:
//   deny   the call ends with UNIMPLEMENTED and no message;
//   empty  the call ends with OK after one message with no fields set.
// A rule is `Service/Method` or `Service/*`, with the service's short name
// (`ConsoleService/GetConsoleTypes`).
//
// Both transports are covered: streams of the multiplexed websocket are answered in the
// framing of io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream, fetch calls as
// a gRPC-web response. Everything else passes through untouched.
'use strict';

function parseRules(text) {
  return (text || '')
    .split(',')
    .filter(Boolean)
    .map((rule) => {
      const [service, method] = rule.split('/');
      if (!service || !method) throw new Error(`a fault rule is Service/Method or Service/*, not ${rule}`);
      return { service, method };
    });
}

function matches(rules, path) {
  const m = /([A-Za-z0-9_]+)\/([A-Za-z0-9_]+)$/.exec(path);
  if (!m) return false;
  return rules.some((r) => r.service === m[1] && (r.method === '*' || r.method === m[2]));
}

function frame(flags, payload) {
  const head = Buffer.alloc(5);
  head[0] = flags;
  head.writeUInt32BE(payload.length, 1);
  return Buffer.concat([head, payload]);
}

function streamMessage(id, last, body) {
  const head = Buffer.alloc(4);
  head.writeUInt32BE(last ? (id | 0x80000000) >>> 0 : id, 0);
  return Buffer.concat([head, body]);
}

const UNIMPLEMENTED = Buffer.from('grpc-status: 12\r\ngrpc-message: answered by ide-test --deny\r\n', 'latin1');
const HEADERS = Buffer.from('content-type: application/grpc-web+proto\r\n', 'latin1');
const OK = Buffer.from('grpc-status: 0\r\n', 'latin1');

// Installs the rules on a Playwright browser context. `log` receives one line per answered call.
async function install(context, { deny, empty }, log) {
  if (!deny.length && !empty.length) return;

  await context.routeWebSocket(/.*/, (ws) => {
    const server = ws.connectToServer();
    const answered = new Set();
    server.onMessage((message) => ws.send(message));
    ws.onMessage((message) => {
      if (typeof message === 'string' || message.length < 4) {
        server.send(message);
        return;
      }
      const id = message.readUInt32BE(0) & 0x7fffffff;
      if (answered.has(id)) return; // the rest of a call answered here
      const text = message.subarray(4).toString('latin1');
      const path = /grpc-websockets-path: *([^\r\n]+)/.exec(text);
      if (path && matches(deny, path[1])) {
        answered.add(id);
        log(`denied ${path[1]}`);
        ws.send(streamMessage(id, true, frame(0x80, UNIMPLEMENTED)));
        return;
      }
      if (path && matches(empty, path[1])) {
        answered.add(id);
        log(`answered empty ${path[1]}`);
        ws.send(streamMessage(id, false, frame(0x80, HEADERS)));
        ws.send(streamMessage(id, false, frame(0, Buffer.alloc(0))));
        ws.send(streamMessage(id, true, frame(0x80, OK)));
        return;
      }
      server.send(message);
    });
  });

  await context.route(
    (url) => matches(deny, url.pathname) || matches(empty, url.pathname),
    async (route) => {
      const request = route.request();
      const type = (await request.headerValue('content-type')) || '';
      if (request.method() !== 'POST' || !type.startsWith('application/grpc')) return route.fallback();
      const pathname = new URL(request.url()).pathname;
      const cors = { 'access-control-allow-origin': '*', 'access-control-expose-headers': 'grpc-status,grpc-message' };
      if (matches(deny, pathname)) {
        log(`denied ${pathname.slice(1)}`);
        return route.fulfill({
          status: 200,
          headers: Object.assign({ 'content-type': 'application/grpc-web+proto', 'grpc-status': '12', 'grpc-message': 'answered by ide-test --deny' }, cors),
          body: Buffer.alloc(0),
        });
      }
      log(`answered empty ${pathname.slice(1)}`);
      return route.fulfill({
        status: 200,
        headers: Object.assign({ 'content-type': 'application/grpc-web+proto' }, cors),
        body: Buffer.concat([frame(0, Buffer.alloc(0)), frame(0x80, OK)]),
      });
    },
  );
}

module.exports = { parseRules, install };
