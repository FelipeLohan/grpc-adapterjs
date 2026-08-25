/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var http2 = require('node:http2')
var Router = require('./router')
var finalHandler = require('./final-handler')
var utils = require('./utils')
var statuses = require('./status')

var status = statuses.status

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
 * Register an interceptor. Only a bare function is accepted in Phase 1 —
 * path-scoped `use('/pkg.Svc', fn)` and sub-app mounting
 * (`express/lib/application.js:181-241`) are Phase 2, once `Layer` exists.
 *
 * @param {Function} fn `(call)` or `(call, next)`
 * @return {app} for chaining
 * @public
 */

app.use = function use (fn) {
  this.router.use(fn)

  return this
}

/**
 * Handle one http2 'stream' event: validate the protocol preconditions,
 * construct the `call`, then walk the router. Mirrors
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

  var done = callback || finalHandler(call, {
    env: this.get('env'),
    onerror: this.get('onerror')
  })

  if (call.service === undefined) {
    return call.fail(status.UNIMPLEMENTED, 'Malformed gRPC path: ' + call.path)
  }

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
