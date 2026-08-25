# express-grpc

Express' developer experience — a callable app, `use()`, declarative routing,
the `next()` chain, rich `req`/`res`-style abstractions — brought to native
gRPC over `node:http2`. No `@grpc/grpc-js`, no C++ binding.

```js
const grpc = require('express-grpc')

const app = grpc()
const root = grpc.loadSync('helloworld.proto')

app.service(root.lookupService('helloworld.Greeter'), {
  SayHello (call) {
    call.send({ message: `Hello ${call.request.name}` })
  }
})

app.listen(50051)
```

That's a complete, working gRPC server. `call.request` is already a decoded
plain object and `call.send()` encodes a plain object back — the codec, the
path, and the call type all come from the `.proto`.

## Why

Three ideas make Express what it is, and none of them are actually coupled to
HTTP/1 or JSON:

1. **The app is a function.** `express()` returns `function (req, res, next)`,
   so `http.createServer(app)` just works. Here, `grpc()` returns
   `function (stream, headers)`, so `http2Server.on('stream', app)` just
   works the same way.
2. **A flat stack of layers**, walked with a `next()` continuation. No magic.
3. **A rich object injected via prototype**, not copied. `req`/`res` become a
   single unified `call` — gRPC doesn't have Express' status-line/body split,
   so read and write sides share one object.

Swap the transport (HTTP/2 stream), the match predicate (a gRPC path instead
of a URL), and the codec (Protobuf instead of JSON) — keep everything else.

## Install

```sh
npm install express-grpc
```

Requires Node.js ≥ 20 (for `node:http2` server-side streams and top-level
`AbortController`).

## The four call types

gRPC has no HTTP verb — every call is a `POST`. The axis Express uses for
`app.get`/`app.post` is replaced with **call type**, derived from the
`.proto`:

```js
app.service(root.lookupService('helloworld.Greeter'), {
  // unary — one request, one response
  SayHello (call) {
    call.send({ message: `Hello ${call.request.name}` })
  },

  // server streaming — one request, N responses
  async LotsOfReplies (call) {
    for (const name of ['a', 'b', 'c']) {
      await call.writeAsync({ message: `Hello ${name}` })
    }
    call.end()
  },

  // client streaming — N requests, one response
  async LotsOfGreetings (call) {
    const names = []
    for await (const msg of call) names.push(msg.name)
    call.send({ message: `Hello ${names.join(', ')}` })
  },

  // bidirectional streaming — N requests, N responses
  async BidiHello (call) {
    for await (const msg of call) call.write({ message: msg.name.toUpperCase() })
    call.end()
  }
})
```

`call.request` and reading `call` as an `AsyncIterable` are mutually
exclusive per type — a unary/server-streaming call gets the single buffered
request as `call.request` (the `express.json()`-before-the-handler
equivalent); a client-streaming/bidi call reads `for await (const msg of
call)` instead, with backpressure handled by Node's own `Readable`
implementation.

## Routing without `.proto`, if you want it

`app.service()` is the fast path once you have a `.proto`, but the router
underneath doesn't require one — you can register paths by hand with raw
`Buffer` payloads:

```js
app.use((call, next) => {
  console.log(call.service, call.methodName)
  next()
})

app.use('/helloworld.Greeter', (call, next) => {
  // runs for every method of this service, not others
  next()
})

app.unary('/helloworld.Greeter/SayHello', (call) => {
  call.send(Buffer.from('...'))
})
```

`app.route(path)` returns a `Route` you can chain `.unary()`/`.serverStream()`/
`.clientStream()`/`.bidi()`/`.any()` on. A path is exact — there is no
`path-to-regexp`, no params, and (deliberately, unlike Express)
`use('/pkg.Service')` matches by prefix but never rewrites `call.path`: a
gRPC path is identity, not a directory.

## Error handling

```js
const { GrpcError, status, errorHandler } = require('express-grpc')

app.unary('/helloworld.Greeter/SayHello', (call) => {
  if (!call.request.name) {
    throw new GrpcError(status.INVALID_ARGUMENT, 'name is required')
  }
  call.send({ message: `Hello ${call.request.name}` })
})

app.use(errorHandler((err, call, next) => {
  console.error(err)
  next(err)
}))
```

A thrown error (or `next(err)`) closes the call with that status. Error
interceptors are detected by arity — `(err, call, next)` has arity 3 — with
`errorHandler()` as the explicit, transpilation-proof way to mark one when
arity alone would be fragile. Anything left uncaught closes with
`13 INTERNAL` (message suppressed outside `NODE_ENV=development`); no match
at all closes with `12 UNIMPLEMENTED`, gRPC's "404".

`GrpcError` has a factory per status: `GrpcError.notFound(msg)`,
`GrpcError.invalidArgument(msg)`, `GrpcError.failedPrecondition(msg)`,
`GrpcError.unauthenticated(msg)`, and so on for every code in `status`.

## Metadata, deadlines, cancellation

```js
app.unary('/helloworld.Greeter/SayHello', (call) => {
  call.metadata.get('authorization')   // request metadata (multimap, case-insensitive)
  call.set('x-served-by', 'node-1')    // initial metadata — before the first write
  call.trailer('x-request-cost', '3')  // trailing metadata — any time before close

  call.signal.addEventListener('abort', () => {
    // grpc-timeout exceeded, or the client cancelled
  })

  call.send({ message: `Hello ${call.request.name}` })
})
```

`call.signal` is a standard `AbortSignal` — pass it straight to `fetch` or a
database driver. `grpc-timeout` becomes `call.deadline` and aborts the signal
automatically; a client `RST_STREAM` does the same.

## API

### `grpc()`

Creates an application — a callable `function (stream, headers)`.

- `app.listen(...args)` — brings up an `Http2Server` (h2c by default) wired to
  this app's `'stream'` event, and calls `.listen(...args)` on it.
  `app.set('tls', { cert, key })` switches to `http2.createSecureServer()`.
- `app.use([path], fn)` — register an interceptor, or mount a sub-app at
  `path` (anything with `.handle`/`.set` — another `grpc()` app).
- `app.route(path)` — get or create the `Route` at an exact path.
- `app.unary(path, ...fns)` / `.serverStream(...)` / `.clientStream(...)` /
  `.bidi(...)` / `.any(...)` — register handlers for one call type.
- `app.service(serviceOrPath, impl)` — register every method of a resolved
  `protobuf.Service` (or a `.proto` file path containing exactly one
  service), deriving path/type/codec from the `.proto`. A method missing
  from `impl` logs a warning and gets a stub that closes `UNIMPLEMENTED`; an
  `impl` key that isn't a method on the service throws immediately.
- `app.set(name, val)` / `.get(name)` / `.enable(name)` / `.disable(name)` /
  `.enabled(name)` / `.disabled(name)` — settings, Express-style.

### `grpc.load(path)` / `grpc.loadSync(path)`

Load and fully resolve a `.proto` file (`root.resolveAll()` runs once, here —
never per request). Returns a `protobufjs` `Root`; call `root.lookupService(
'pkg.Service')` to get what `app.service()` expects.

### `call`

The unified read/write object passed to every handler.

| Read | | Write | |
|---|---|---|---|
| `call.request` | decoded request (unary/serverStream) | `call.send(msg)` | encode + close, terminal |
| `call[Symbol.asyncIterator]` | decoded requests (clientStream/bidi) | `call.write(msg)` | encode + write, non-terminal |
| `call.metadata` | request `Metadata` | `call.writeAsync(msg)` | `write()`, awaiting `'drain'` |
| `call.path` / `.service` / `.methodName` | the gRPC path, parsed | `call.writer()` | a real `Writable` for `pipeline()` |
| `call.type` | `'unary'\|'serverStream'\|'clientStream'\|'bidi'` | `call.end([msg])` | flush trailers, close |
| `call.deadline` / `.signal` | timeout as `Date.now()+ms` / `AbortSignal` | `call.status(code, [msg])` | set the closing status |
| `call.app` | the owning `grpc()` app | `call.set(k, v)` | initial metadata (before 1st write) |
| | | `call.trailer(k, v)` | trailing metadata (until close) |
| | | `call.fail(err\|code, [msg])` | close with an error/status |

### `Metadata`

A case-insensitive multimap. `get(key)` returns every value (an array,
possibly empty); `getFirst(key)` returns the first or `undefined`. Keys
ending in `-bin` carry `Buffer`s, base64-encoded on the wire.

### `status`, `GrpcError`, `errorHandler`

`status` is the canonical 0–16 code enum. `new GrpcError(code, message,
trailers?)` is a normal `Error` with a `.code`; thrown from anywhere in the
stack, it closes the call with that status. `errorHandler(fn)` marks a
3-arity function as an error interceptor explicitly.

## Settings

| Setting | Default | |
|---|---|---|
| `env` | `NODE_ENV` or `'development'` | suppresses internal error messages outside `'development'` |
| `max receive message size` | `4 MiB` | oversized request message → `8 RESOURCE_EXHAUSTED` |
| `strict encode` | enabled | `Type.verify()` before encoding a response; `app.disable(...)` to skip |
| `protobuf toObject` | `{ defaults: true, longs: String, enums: String, bytes: Buffer }` | shape of decoded messages — `longs: String` avoids `int64` precision loss |
| `tls` | — | `{ cert, key, ... }` switches `app.listen()` to `createSecureServer` |
| `compression` | — | preferred response encoding (`'gzip'`), negotiated against the client's `grpc-accept-encoding` |

## Generating types

`call.request`/handler arguments can get real editor autocomplete via
protobufjs's own `pbjs`/`pbts` (the `protobufjs-cli` package):

```sh
npx pbjs -t static-module -w commonjs -o service.pb.js service.proto
npx pbts -o service.pb.d.ts service.pb.js
```

## Testing

```sh
npm test
```

The suite runs against real clients — `@grpc/grpc-js` and a raw `node:http2`
client — not mocks, on the theory that framing/trailers/status edge cases
are exactly where a hand-rolled gRPC implementation goes wrong.

## What this deliberately doesn't do

- **No path rewriting.** `use('/pkg.Service')` matches a prefix but
  `call.path` never changes — a gRPC path is an identity, not a directory
  (unlike Express' `trim_prefix`).
- **No `path-to-regexp`.** A gRPC path is static and known at boot from the
  `.proto`; matching is a string comparison, not a compiled regex.
- **No views, cookies, `etag`/`fresh`.** Browser-era HTTP concepts with no
  gRPC analogue.
- **No gRPC-Web, no Server Reflection.** Both are real, both need different
  framing/negotiation than this v1 implements; the codec boundary is built
  to leave room for them later.

## License

MIT
