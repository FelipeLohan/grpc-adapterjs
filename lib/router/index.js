/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var GrpcError = require('../status').GrpcError
var status = require('../status').status

/**
 * Flat stack of handlers, walked with a `next()` continuation exactly like
 * Express's router did before extraction (`router/index.js` at `cec5780d^`).
 *
 * Path-based matching — exact routes and `use()` prefixes, the eventual
 * `Layer`/`Route` split of plan §5 — is Phase 2. For now every layer matches
 * unconditionally: enough to dispatch a single handler end-to-end for
 * milestone M1, without inventing behavior the plan hasn't specified yet.
 *
 * @public
 */

function Router () {
  if (!(this instanceof Router)) return new Router()

  this.stack = []
}

/**
 * Register a handler. Only a bare function is accepted for now — path
 * prefixes, sub-app mounting and error-handler detection by arity all
 * arrive with the real `Layer` in Phase 2.
 *
 * @param {Function} fn `(call)` or `(call, next)`
 * @return {Router} for chaining
 * @public
 */

Router.prototype.use = function use (fn) {
  if (typeof fn !== 'function') {
    throw new TypeError('router.use() requires a function')
  }

  this.stack.push({ handle: fn, arity: fn.length })

  return this
}

/**
 * Walk the stack for one call.
 *
 * @param {call} call
 * @param {Function} done `(err) => void`, called once nothing else will run
 * @public
 */

Router.prototype.handle = function handle (call, done) {
  var stack = this.stack
  var idx = 0

  next()

  function next (err) {
    if (err) return done(err)

    if (call.signal.aborted) {
      return done(new GrpcError(status.DEADLINE_EXCEEDED, 'Deadline exceeded'))
    }

    var layer = stack[idx++]

    if (!layer) return done()

    try {
      if (layer.arity <= 1) layer.handle(call)
      else layer.handle(call, next)
    } catch (thrown) {
      next(thrown)
    }
  }
}

module.exports = Router
