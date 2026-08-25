/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var EventEmitter = require('node:events').EventEmitter
var mixin = require('./utils').mixin
var proto = require('./application')
var callProto = require('./call')

/**
 * Create a gRPC application.
 *
 * Structural copy of `express/lib/express.js:35-56`, with the callable
 * signature swapped from `(req, res, next)` to `(stream, headers)` — the
 * shape of node:http2's `'stream'` event. That single change is what makes
 * `http2Server.on('stream', app)` work with no wrapper, and what makes
 * mounting one app inside another (`app.use(subApp)`) trivial: an app *is*
 * a handler.
 *
 * @return {Function}
 * @public
 */

function createApplication () {
  var app = function (stream, headers) {
    app.handle(stream, headers)
  }

  mixin(app, EventEmitter.prototype, false)
  mixin(app, proto, false)

  // the prototype installed on every call, mirroring how express.js:45-50
  // installs app.request/app.response on req/res
  app.call = Object.create(callProto, {
    app: { configurable: true, enumerable: true, writable: true, value: app }
  })

  app.init()

  return app
}

exports = module.exports = createApplication
exports.application = proto
exports.call = callProto
exports.status = require('./status').status
exports.GrpcError = require('./status').GrpcError
