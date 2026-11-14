// Records everything a page sends and receives, through the Chrome DevTools Protocol, and
// reassembles the gRPC calls out of it. Passive: the page is not instrumented.
//
// Two transports carry gRPC from the Deephaven JS API (web/client-api, ConnectOptions):
//   - one websocket of subprotocol `grpc-websockets-multiplex` shared by all calls, when the
//     page is http:// (MultiplexedWebsocketTransport.java / MultiplexedWebSocketServerStream.java);
//   - gRPC-web over fetch, one POST per call, when the page is https:// (FetchTransport.java).
// `calls()` returns both in one shape.
'use strict';

const GRPC_MIME = /^application\/grpc/;

class Trace {
  constructor() {
    this.t0 = Date.now();
    this.http = new Map(); // requestId -> entry
    this.sockets = new Map(); // requestId -> entry
    this.console = [];
    this.pageErrors = [];
    this.marks = [];
    this.pending = [];
  }

  // Attaches to a Playwright page. Call before the first navigation.
  async attach(page) {
    const cdp = await page.context().newCDPSession(page);
    this.cdp = cdp;
    await cdp.send('Network.enable', { maxPostDataSize: 64 * 1024 * 1024 });
    const now = () => Date.now() - this.t0;

    cdp.on('Network.requestWillBeSent', (e) => {
      if (e.request.url.startsWith('data:')) return;
      const entry = {
        id: e.requestId,
        t: now(),
        method: e.request.method,
        url: e.request.url,
        requestHeaders: e.request.headers,
        // Binary bodies arrive as base64 entries; `postData` alone would mangle them.
        requestBody: (e.request.postDataEntries || []).map((p) => p.bytes || ''),
        chunks: [],
      };
      if (e.redirectResponse) {
        // The same requestId continues after a redirect: keep the redirect as its own entry.
        const prev = this.http.get(e.requestId);
        if (prev) {
          prev.status = e.redirectResponse.status;
          prev.protocol = e.redirectResponse.protocol;
          prev.responseHeaders = e.redirectResponse.headers;
          this.http.set(e.requestId + ':redirect' + prev.t, prev);
        }
      }
      this.http.set(e.requestId, entry);
      if (e.request.hasPostData && !entry.requestBody.length && isGrpcRequest(entry)) {
        // fetch() bodies are not part of the event; ask for them.
        this.pending.push(
          cdp.send('Network.getRequestPostData', { requestId: e.requestId }).then(
            (r) => {
              entry.requestBody = [r.base64Encoded ? r.postData : Buffer.from(r.postData, 'latin1').toString('base64')];
            },
            () => {
              entry.noRequestBody = true;
            },
          ),
        );
      }
    });
    cdp.on('Network.requestWillBeSentExtraInfo', (e) => {
      const entry = this.http.get(e.requestId);
      if (entry) entry.requestHeadersWire = e.headers;
    });
    cdp.on('Network.responseReceived', (e) => {
      const entry = this.http.get(e.requestId);
      if (!entry) return;
      entry.status = e.response.status;
      entry.protocol = e.response.protocol;
      entry.mime = e.response.mimeType;
      entry.responseHeaders = e.response.headers;
      entry.tResponse = now();
      entry.fromCache = e.response.fromDiskCache || e.response.fromServiceWorker || false;
      if (GRPC_MIME.test(e.response.mimeType || '')) {
        // Makes Network.dataReceived carry the bytes, so that a response stream that never
        // ends (a subscription) can still be read.
        this.pending.push(
          cdp.send('Network.streamResourceContent', { requestId: e.requestId }).then(
            (r) => {
              if (r && r.bufferedData) entry.chunks.unshift(r.bufferedData);
            },
            () => {
              entry.noStream = true;
            },
          ),
        );
      }
    });
    cdp.on('Network.dataReceived', (e) => {
      const entry = this.http.get(e.requestId);
      if (entry && e.data) entry.chunks.push(e.data);
    });
    cdp.on('Network.loadingFinished', (e) => {
      const entry = this.http.get(e.requestId);
      if (entry) {
        entry.finished = true;
        entry.tEnd = now();
        entry.encodedLength = e.encodedDataLength;
        if (isGrpcRequest(entry)) {
          // A response that ended before streaming was switched on is read as a whole.
          this.pending.push(
            cdp.send('Network.getResponseBody', { requestId: e.requestId }).then(
              (r) => {
                entry.wholeBody = r.base64Encoded ? r.body : Buffer.from(r.body, 'latin1').toString('base64');
              },
              () => {},
            ),
          );
        }
      }
    });
    cdp.on('Network.loadingFailed', (e) => {
      const entry = this.http.get(e.requestId);
      if (entry) entry.failed = e.errorText || 'failed';
    });

    cdp.on('Network.webSocketCreated', (e) => {
      this.sockets.set(e.requestId, { id: e.requestId, t: now(), url: e.url, frames: [] });
    });
    cdp.on('Network.webSocketWillSendHandshakeRequest', (e) => {
      const s = this.sockets.get(e.requestId);
      if (s) s.requestHeaders = e.request.headers;
    });
    cdp.on('Network.webSocketHandshakeResponseReceived', (e) => {
      const s = this.sockets.get(e.requestId);
      if (s) {
        s.status = e.response.status;
        s.responseHeaders = e.response.headers;
      }
    });
    const frame = (dir) => (e) => {
      const s = this.sockets.get(e.requestId);
      if (s) s.frames.push({ dir, t: now(), opcode: e.response.opcode, data: e.response.payloadData });
    };
    cdp.on('Network.webSocketFrameSent', frame('>'));
    cdp.on('Network.webSocketFrameReceived', frame('<'));
    cdp.on('Network.webSocketFrameError', (e) => {
      const s = this.sockets.get(e.requestId);
      if (s) s.error = e.errorMessage;
    });
    cdp.on('Network.webSocketClosed', (e) => {
      const s = this.sockets.get(e.requestId);
      if (s) s.closed = now();
    });

    page.on('console', (m) => this.console.push({ t: now(), type: m.type(), text: m.text() }));
    page.on('pageerror', (err) => this.pageErrors.push({ t: now(), text: String(err) }));
  }

  // Waits for the bodies that were asked for. Call before calls() or toJSON().
  async settle() {
    while (this.pending.length) await Promise.all(this.pending.splice(0));
  }

  // A named point in time, so that the calls of one step can be told from the next.
  mark(name) {
    this.marks.push({ t: Date.now() - this.t0, name });
  }

  // Requests that are not gRPC: the static files.
  staticRequests() {
    return [...this.http.values()]
      .filter((r) => !isGrpcRequest(r))
      .map((r) => ({
        t: r.t,
        method: r.method,
        url: r.url,
        status: r.status,
        protocol: r.protocol,
        mime: r.mime,
        failed: r.failed,
        requestHeaders: r.requestHeadersWire || r.requestHeaders,
        responseHeaders: r.responseHeaders,
      }));
  }

  // Every gRPC call, in the order the client started them.
  calls() {
    const out = [];
    for (const s of this.sockets.values()) out.push(...multiplexedCalls(s));
    for (const r of this.http.values()) if (isGrpcRequest(r)) out.push(fetchCall(r));
    out.sort((a, b) => a.t - b.t);
    return out;
  }

  toJSON() {
    return {
      marks: this.marks,
      sockets: [...this.sockets.values()].map((s) => ({
        url: s.url,
        t: s.t,
        status: s.status,
        requestHeaders: s.requestHeaders,
        responseHeaders: s.responseHeaders,
        frameCount: s.frames.length,
        closed: s.closed,
        error: s.error,
      })),
      static: this.staticRequests(),
      calls: this.calls(),
      console: this.console,
      pageErrors: this.pageErrors,
    };
  }
}

function isGrpcRequest(r) {
  const h = r.requestHeaders || {};
  const ct = h['content-type'] || h['Content-Type'] || '';
  return r.method === 'POST' && GRPC_MIME.test(ct);
}

function parseHeaderText(text) {
  const headers = {};
  for (const line of text.split('\r\n')) {
    const i = line.indexOf(':');
    if (i > 0) headers[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim();
  }
  return headers;
}

// Splits a byte stream of gRPC(-web) frames: 1 byte flags, 4 bytes length, payload.
// Flag 0x80 marks a frame of headers or trailers as text. Returns what is left over too.
function splitFrames(buf) {
  const frames = [];
  let off = 0;
  while (buf.length - off >= 5) {
    const flags = buf[off];
    const len = buf.readUInt32BE(off + 1);
    if (buf.length - off - 5 < len) break;
    frames.push({ flags, data: buf.subarray(off + 5, off + 5 + len) });
    off += 5 + len;
  }
  return { frames, rest: buf.length - off };
}

function grpcPath(urlOrPath) {
  const m = /([A-Za-z0-9_.]+)\/([A-Za-z0-9_]+)$/.exec(urlOrPath);
  return m ? { service: m[1], method: m[2] } : { service: '?', method: urlOrPath };
}

// Behavior: io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream#onMessage and
// io.deephaven.web.client.api.grpc.MultiplexedWebsocketTransport.
// Client to server, each websocket message: int32 stream id (sign bit set: half close), then
//   first message of a stream: the request headers as `name: value\r\n` text, including
//   `grpc-websockets-path: <service>/<method>`;
//   later messages: one byte 0 followed by a piece of the gRPC frame stream, or one byte 1
//   alone with the sign bit set, to half close.
// Server to client, each websocket message: int32 stream id (sign bit set on the last one),
//   then gRPC-web frames; a frame of flag 0x80 is the response headers (first) or the
//   trailers with grpc-status (last).
function multiplexedCalls(socket) {
  const streams = new Map();
  const order = [];
  for (const f of socket.frames) {
    if (f.opcode !== 2) continue;
    const buf = Buffer.from(f.data, 'base64');
    if (buf.length < 4) continue;
    const raw = buf.readInt32BE(0);
    const last = raw < 0;
    const id = raw & 0x7fffffff;
    if (id === 0x7fffffff) continue; // the transport's GO_AWAY and its acknowledgement
    const body = buf.subarray(4);
    let s = streams.get(id);
    if (f.dir === '>') {
      if (!s) {
        const headers = parseHeaderText(body.toString('latin1'));
        const p = grpcPath(headers['grpc-websockets-path'] || '');
        s = {
          transport: 'ws-multiplex',
          socketUrl: socket.url,
          streamId: id,
          t: f.t,
          service: p.service,
          method: p.method,
          requestHeaders: headers,
          up: [],
          down: [],
          requestMessageTimes: [],
          responseMessageTimes: [],
          halfClosed: false,
          ended: false,
        };
        streams.set(id, s);
        order.push(s);
        continue;
      }
      if (body.length >= 1 && body[0] === 1 && last) {
        s.halfClosed = true;
      } else {
        s.up.push(body.subarray(1));
        s.requestMessageTimes.push(f.t);
      }
    } else if (s) {
      s.down.push(body);
      s.responseMessageTimes.push(f.t);
      if (last) {
        s.ended = true;
        s.tEnd = f.t;
      }
    }
  }
  return order.map((s) => {
    const up = splitFrames(Buffer.concat(s.up));
    const down = splitFrames(Buffer.concat(s.down));
    const call = finishCall(s, up.frames, down.frames);
    call.requestHeaders = s.requestHeaders;
    call.halfClosed = s.halfClosed;
    call.ended = s.ended;
    return call;
  });
}

function fetchCall(r) {
  const p = grpcPath(new URL(r.url).pathname);
  const up = splitFrames(Buffer.concat(r.requestBody.map((b) => Buffer.from(b, 'base64'))));
  const streamed = Buffer.concat(r.chunks.map((b) => Buffer.from(b, 'base64')));
  const whole = r.wholeBody ? Buffer.from(r.wholeBody, 'base64') : Buffer.alloc(0);
  const down = splitFrames(whole.length >= streamed.length ? whole : streamed);
  const call = finishCall(
    { transport: 'grpc-web', t: r.t, tEnd: r.tEnd, service: p.service, method: p.method },
    up.frames,
    down.frames,
  );
  call.httpProtocol = r.protocol;
  call.httpStatus = r.status;
  call.requestHeaders = lower(r.requestHeadersWire || r.requestHeaders);
  call.ended = Boolean(r.finished || r.failed);
  call.failed = r.failed;
  const rh = lower(r.responseHeaders);
  call.responseHeaders = Object.assign({}, rh, call.responseHeaders);
  // A trailers-only response carries the status in the HTTP headers.
  if (call.status === undefined && rh['grpc-status'] !== undefined) {
    call.status = Number(rh['grpc-status']);
    call.message = decodeMessage(rh['grpc-message']);
  }
  return call;
}

function lower(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers || {})) out[k.toLowerCase()] = v;
  return out;
}

function decodeMessage(m) {
  if (m === undefined) return undefined;
  try {
    return decodeURIComponent(m);
  } catch (_) {
    return m;
  }
}

function finishCall(base, upFrames, downFrames) {
  const call = {
    transport: base.transport,
    t: base.t,
    tEnd: base.tEnd,
    streamId: base.streamId,
    service: base.service,
    method: base.method,
    requests: upFrames.filter((f) => !(f.flags & 0x80)).map((f) => f.data.toString('base64')),
    responses: [],
    responseHeaders: {},
    status: undefined,
    message: undefined,
  };
  const meta = downFrames.filter((f) => f.flags & 0x80).map((f) => parseHeaderText(f.data.toString('latin1')));
  for (const f of downFrames) if (!(f.flags & 0x80)) call.responses.push(f.data.toString('base64'));
  for (const m of meta) {
    if (m['grpc-status'] !== undefined) {
      call.status = Number(m['grpc-status']);
      call.message = decodeMessage(m['grpc-message']);
      call.trailers = m;
    } else {
      Object.assign(call.responseHeaders, m);
    }
  }
  return call;
}

const STATUS_NAMES = [
  'OK', 'CANCELLED', 'UNKNOWN', 'INVALID_ARGUMENT', 'DEADLINE_EXCEEDED', 'NOT_FOUND',
  'ALREADY_EXISTS', 'PERMISSION_DENIED', 'RESOURCE_EXHAUSTED', 'FAILED_PRECONDITION', 'ABORTED',
  'OUT_OF_RANGE', 'UNIMPLEMENTED', 'INTERNAL', 'UNAVAILABLE', 'DATA_LOSS', 'UNAUTHENTICATED',
];

function statusName(code) {
  return code === undefined ? 'open' : STATUS_NAMES[code] || String(code);
}

// One line per call, for a failure report.
function describeCalls(calls) {
  return calls.map(
    (c) =>
      `${String(c.t).padStart(6)}ms ${c.transport} ${c.service.replace(/^.*\./, '')}/${c.method}` +
      ` req=${c.requests.length} resp=${c.responses.length} ${statusName(c.status)}` +
      (c.message ? ` "${c.message}"` : ''),
  );
}

module.exports = { Trace, describeCalls, statusName, splitFrames };
