/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var statuses = require('./status')
var status = statuses.status
var statusFromError = statuses.statusFromError

/**
 * The tail of the middleware chain, mirroring `finalhandler`: what runs when
 * nobody else claimed the call.
 *
 * Two situations reach here:
 *   - `next()` was called past the end of the stack with no error -> the
 *     request matched nothing, which in gRPC is `12 UNIMPLEMENTED` (the
 *     "404" of this protocol, per plan §2.1).
 *   - `next(err)` reached the end unhandled -> `13 INTERNAL`, with the
 *     message suppressed outside of `development` the same way `finalhandler`
 *     hides stack traces in production.
 *
 * Operates on the already-constructed `call` rather than the raw stream —
 * `call.fail()` already knows how to fall back to a Trailers-Only response
 * when nothing has been written yet (plan §4.4), so this only needs to pick
 * the status and message.
 *
 * @param {call} call
 * @param {object} [options]
 * @param {Function} [options.onerror]
 * @param {string} [options.env]
 * @return {Function} done(err)
 * @public
 */

function finalHandler (call, options) {
  var opts = options || {}
  var env = opts.env || process.env.NODE_ENV || 'development'

  return function done (err) {
    if (call.closed) return

    if (err) {
      if (typeof opts.onerror === 'function') opts.onerror(err)

      var resolved = statusFromError(err)
      var message = env === 'production' && resolved.code === status.INTERNAL
        ? 'Internal server error'
        : resolved.message

      if (resolved.trailers) call.trailer(resolved.trailers)

      return call.fail(resolved.code, message)
    }

    return call.fail(status.UNIMPLEMENTED, 'Not implemented: ' + call.path)
  }
}

module.exports = finalHandler
