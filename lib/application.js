/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var http2 = require('node:http2')
var Router = require('./router')
var Route = require('./router/route')
var finalHandler = require('./final-handler')
var utils = require('./utils')
var statuses = require('./status')

var status = statuses.status
var CALL_TYPES = Route.TYPES

/**
 * Application prototype, mixed onto the callable `app` function created by
 * `createApplication()` (`lib/grpc.js`) — the same shape as
 * `lib/application.js` in Express, just retargeted at http2 streams.
 *
 * @private
 */

var app = exports = module.exports = {}

/**
 * Lazily build the router and set up default configuration. Mirrors
 * `app.init()` (`express/lib/application.js:59-79`).
 *
 * @private
 */

app.init = function init () {
  var router = null

  this.cache = Object.create(null)
  this.settings = Object.create(null)

  this.defaultConfiguration()

  Object.defineProperty(this, 'router', {
    configurable: true,
    enumerable: true,
    get: function getrouter () {
      if (router === null) router = new Router()
      return router
    }
  })
}

/**
 * @private
 */

app.defaultConfiguration = function defaultConfiguration () {
  var env = process.env.NODE_ENV || 'development'

  this.set('env', env)
  this.set('max receive message size', 4 * 1024 * 1024)

  this.locals = Object.create(null)
  this.mountpath = '/'

  // a mounted sub-app falls back to its parent's settings via the
  // prototype chain, mirroring express/lib/application.js:104-109
  this.on('mount', function onmount (parent) {
    Object.setPrototypeOf(this.settings, parent.settings)
  })
}

/**
 * Get/set an application setting. `app.set('foo')` reads, `app.set('foo', v)`
 * writes. Mirrors `express/lib/application.js:346-370` without the HTTP-only
 * special cases (`etag`, `query parser`, `trust proxy`) that have no gRPC
 * analogue.
 *
 * @param {string} setting
 * @param {*} [val]
 * @return {*|app}
 * @public
 */

app.set = function set (setting, val) {
  if (arguments.length === 1) return this.settings[setting]

  this.settings[setting] = val

  return this
}

/**
 * @param {string} setting
 * @return {*}
 * @public
 */

app.get = function get (setting) {
  return this.set(setting)
}

/**
 * @param {string} setting
 * @return {app}
 * @public
 */

app.enable = function enable (setting) {
  return this.set(setting, true)
}

/**
 * @param {string} setting
 * @return {app}
 * @public
 */

app.disable = function disable (setting) {
  return this.set(setting, false)
}

/**
 * @param {string} setting
 * @return {boolean}
 * @public
 */

app.enabled = function enabled (setting) {
  return Boolean(this.set(setting))
}

/**
 * @param {string} setting
 * @return {boolean}
 * @public
 */

app.disabled = function disabled (setting) {
  return !this.set(setting)
}

/**
 * Register an interceptor, or mount a sub-app at a prefix. Mirrors
 * `express/lib/application.js:181-241`.
 *
 * When `fn` is another express-grpc app (has `.handle`/`.set`), it is
 * mounted: a layer is installed that hands the *same* `call` object to the
 * sub-app's `dispatch()` after swapping its prototype to the sub-app's
 * `call` prototype (so `call.app` resolves correctly inside it), then
 * restores the original prototype in the continuation before calling
 * `next()`. This is the same trick as `Object.setPrototypeOf(req, orig.request)`
 * (`application.js:225-231`) — except our `call` already carries live state
 * (metadata, deadline, response machine), so unlike Express we must reuse
 * the single `call` instance rather than reconstructing one; that's exactly
 * why `handle()` (below) delegates to a separate `dispatch()` instead of
 * doing the prototype swap itself.
 *
 * @param {string|Function} fn path prefix, or the middleware/app itself
 * @param {...Function} [fns]
 * @return {app} for chaining
 * @public
 */

app.use = function use (fn) {
  var offset = 0
  var path = '/'

  if (typeof fn !== 'function') {
    var arg = fn

    while (Array.isArray(arg) && arg.length !== 0) arg = arg[0]

    if (typeof arg !== 'function') {
      offset = 1
      path = fn
    }
  }

  var fns = utils.flatten(Array.prototype.slice.call(arguments, offset))

  if (fns.length === 0) {
    throw new TypeError('app.use() requires a middleware function')
  }

  var router = this.router
  var self = this

  fns.forEach(function (fn) {
    if (!fn || typeof fn.handle !== 'function' || typeof fn.set !== 'function') {
      return router.use(path, fn)
    }

    fn.mountpath = path
    fn.parent = self

    router.use(path, function mountedApp (call, next) {
      var originalProto = Object.getPrototypeOf(call)

      fn.dispatch(call, function (err) {
        Object.setPrototypeOf(call, originalProto)
        next(err)
      })
    })

    fn.emit('mount', self)
  })

  return this
}

/**
 * Fetch-or-create the `Route` at `path`. Mirrors `application.js`'s
 * delegation to `router.route()`.
 *
 * @param {string} path exact `/{package}.{Service}/{Method}`
 * @return {Route}
 * @public
 */

app.route = function route (path) {
  return this.router.route(path)
}

// app.unary/serverStream/clientStream/bidi/any(path, ...handlers), the
// call-type analogue of methods.forEach generating app.get/post/...
// (application.js:466-481).
CALL_TYPES.concat('any').forEach(function (type) {
  app[type] = function (path) {
    var route = this.route(path)
    route[type].apply(route, Array.prototype.slice.call(arguments, 1))
    return this
  }
})

/**
 * Handle one http2 'stream' event: validate the protocol preconditions,
 * construct the `call` (exactly once), then dispatch it. Mirrors
 * `express/lib/application.js:146-177`.
 *
 * Two failures happen before a `call` exists at all and get a direct wire
 * response instead of going through `call.fail()`:
 *   - wrong `:method` -> Trailers-Only `12 UNIMPLEMENTED` (still a gRPC
 *     response, since the peer used HTTP/2 framing correctly)
 *   - non-gRPC `content-type` -> a plain HTTP `415`, since the peer isn't
 *     speaking gRPC and a grpc-status trailer would be meaningless to it
 *
 * @param {ServerHttp2Stream} stream
 * @param {object} headers
 * @param {Function} [callback]
 * @public
 */

app.handle = function handle (stream, headers, callback) {
  if (headers[':method'] !== 'POST') {
    return respondTrailersOnly(stream, status.UNIMPLEMENTED, 'gRPC requires POST, got ' + headers[':method'])
  }

  if (!utils.isGrpcContentType(headers['content-type'])) {
    return respondHttpStatus(stream, 415)
  }

  var call = Object.create(this.call)

  call.init(stream, headers)

  if (call.service === undefined) {
    return call.fail(status.UNIMPLEMENTED, 'Malformed gRPC path: ' + call.path)
  }

  this.dispatch(call, callback)
}

/**
 * The reusable half of request handling: assign `call`'s prototype to this
 * app's `call` prototype (so `call.app` is `this`) and walk this app's
 * router. Split out from `handle()` so mounting a sub-app can re-enter here
 * — with the *same* `call` object — without re-parsing headers or
 * re-binding the deadline/cancellation listeners a second time.
 *
 * @param {call} call
 * @param {Function} [callback]
 * @public
 */

app.dispatch = function dispatch (call, callback) {
  Object.setPrototypeOf(call, this.call)

  var done = callback || finalHandler(call, {
    env: this.get('env'),
    onerror: this.get('onerror')
  })

  this.router.handle(call, done)
}

/**
 * Bring up an `Http2Server` (or `Http2SecureServer` when `app.set('tls', ...)`
 * was configured) with this app wired to its `'stream'` event. Mirrors
 * `express/lib/application.js:590-598`.
 *
 * `allowHTTP1` is intentionally left off: an HTTP/1 client cannot speak
 * gRPC, and gRPC without TLS requires h2c with prior knowledge, which
 * `http2.createServer()` provides with no upgrade dance.
 *
 * @return {Http2Server}
 * @public
 */

app.listen = function listen () {
  var options = this.get('http2 options') || {}
  var tls = this.get('tls')

  var server = tls
    ? http2.createSecureServer(Object.assign({}, tls, options))
    : http2.createServer(options)

  server.on('stream', this)

  return server.listen.apply(server, arguments)
}

/**
 * @param {ServerHttp2Stream} stream
 * @param {number} code
 * @param {string} message
 * @private
 */

function respondTrailersOnly (stream, code, message) {
  if (stream.destroyed || stream.headersSent) return

  stream.respond({
    ':status': 200,
    'content-type': 'application/grpc+proto',
    'grpc-status': String(code),
    'grpc-message': utils.encodeMessage(message)
  }, { endStream: true })
}

/**
 * @param {ServerHttp2Stream} stream
 * @param {number} code
 * @private
 */

function respondHttpStatus (stream, code) {
  if (stream.destroyed || stream.headersSent) return

  stream.respond({ ':status': code }, { endStream: true })
}
