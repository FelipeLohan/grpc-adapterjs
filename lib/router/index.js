/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var Layer = require('./layer')
var Route = require('./route')
var isErrorHandler = require('./error-handler').isErrorHandler
var GrpcError = require('../status').GrpcError
var status = require('../status').status
var utils = require('../utils')

var slice = Array.prototype.slice

/**
 * Espelha `router/index.js` (`cec5780d^`): a stack of `Layer`s walked with a
 * `next()` continuation. Two structural differences from Express, both
 * consequences of a gRPC path being static and non-hierarchical (plan §5.1,
 * §5.4):
 *
 *   - No `trim_prefix`/`baseUrl`/`fqdn` handling (`router/index.js:263-291`):
 *     `use('/pkg.Svc')` matches by prefix but never rewrites `call.path`
 *     (plan A5).
 *   - Route layers are still ordinary members of the one ordered `stack` —
 *     registration order still decides who runs before/after whom, exactly
 *     like Express — but `routes` is a `Map<path, Route>` kept alongside it
 *     so `router.route(path)` is an O(1) idempotent lookup instead of a
 *     linear scan, and each route layer's `match()` is a plain `===`
 *     (`Layer#compileMatcher`) instead of a compiled `path-to-regexp` regex.
 *     That is where the "O(1) dispatch" of plan §5.1 actually pays off:
 *     matching one candidate layer is now a string comparison, not a regex
 *     `.exec()`.
 *
 * @public
 */

function Router () {
  if (!(this instanceof Router)) return new Router()

  this.stack = []
  this.routes = new Map()
}

/**
 * Register an interceptor, exactly like Express's `router.use()` minus path
 * rewriting. `path` defaults to `'/'` (match everything).
 *
 * @param {string} [path]
 * @param {...Function} fn
 * @return {Router} for chaining
 * @public
 */

Router.prototype.use = function use (fn) {
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

  var fns = utils.flatten(slice.call(arguments, offset))

  if (fns.length === 0) {
    throw new TypeError('router.use() requires a middleware function')
  }

  var self = this

  fns.forEach(function (handle) {
    if (typeof handle !== 'function') {
      throw new TypeError('router.use() requires a middleware function')
    }

    self.stack.push(new Layer(path, { exact: false }, handle))
  })

  return this
}

/**
 * Fetch-or-create the `Route` for an exact path. Idempotent: calling this
 * twice for the same path returns the same `Route` (and does not push a
 * second layer), which is what makes `app.unary(p, a); app.any(p, b)` land
 * both handlers on one route instead of two competing layers.
 *
 * @param {string} path
 * @return {Route}
 * @public
 */

Router.prototype.route = function route (path) {
  var existing = this.routes.get(path)

  if (existing) return existing

  var newRoute = new Route(path)
  var layer = new Layer(path, { exact: true }, function dispatchRoute (call, next) {
    newRoute.dispatch(call, next)
  })

  layer.route = newRoute

  this.routes.set(path, newRoute)
  this.stack.push(layer)

  return newRoute
}

/**
 * Walk the stack for one call. Mirrors `router/index.js:113-291`: `next(err)`
 * skips non-matching layers, an error only reaches error-handler layers
 * (arity 3 or flagged via `grpc.errorHandler()`), and reaching the end calls
 * `done` via `setImmediate` so a fully synchronous chain can't blow the call
 * stack and so `done` (finalHandler) never runs inside this function's own
 * try/catch.
 *
 * Added over the Phase 1 stub: a deadline short-circuit. If `call.signal` is
 * already aborted before a layer runs, the chain stops with
 * `4 DEADLINE_EXCEEDED` (or `1 CANCELLED` if the client hung up) instead of
 * spending work on a caller that is already gone.
 *
 * @param {call} call
 * @param {Function} done
 * @public
 */

Router.prototype.handle = function handle (call, done) {
  var stack = this.stack
  var idx = 0

  next()

  function next (err) {
    if (call.signal.aborted && !err) {
      return finish(new GrpcError(
        call.cancelled ? status.CANCELLED : status.DEADLINE_EXCEEDED,
        call.cancelled ? 'Call cancelled by client' : 'Deadline exceeded'
      ))
    }

    var layer = null

    while (idx < stack.length) {
      var candidate = stack[idx++]

      if (candidate.match(call.path)) {
        layer = candidate
        break
      }
    }

    if (!layer) return finish(err)

    var errorLayer = isErrorHandler(layer.handle)

    if (err) {
      if (!errorLayer) return next(err)

      try {
        layer.handle(err, call, next)
      } catch (thrown) {
        next(thrown)
      }

      return
    }

    if (errorLayer) return next(err)

    try {
      if (layer.handle.length <= 1) layer.handle(call)
      else layer.handle(call, next)
    } catch (thrown) {
      next(thrown)
    }
  }

  function finish (err) {
    setImmediate(done, err)
  }
}

module.exports = Router
