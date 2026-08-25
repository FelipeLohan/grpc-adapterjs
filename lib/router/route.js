/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var utils = require('../utils')
var isErrorHandler = require('./error-handler').isErrorHandler

var slice = Array.prototype.slice

/**
 * `Route.methods` (`router/route.js:47`, `cec5780d^`) indexed the wire verb.
 * gRPC has no verb — every call is a POST — so the axis becomes *call type*,
 * which is static per path (a `.proto` rpc is exactly one of these). See
 * plan §5.3.
 */

var TYPES = ['unary', 'serverStream', 'clientStream', 'bidi']
var ANY = '*'

/**
 * The terminal handler stack for one exact gRPC path.
 *
 * Unlike Express, where a single path can legitimately serve GET, POST, etc.
 * with independent handler stacks, a gRPC path maps to exactly one rpc
 * method — so a Route has exactly one `type`, fixed by whichever
 * type-registering call (`.unary`, `.serverStream`, ...) touches it first.
 * Registering a second, different type on the same Route is a programming
 * error caught here; `.any()` handlers run regardless of type, the
 * `route.all()` analogue.
 *
 * @param {string} path
 * @public
 */

function Route (path) {
  this.path = path
  this.type = undefined
  this.stack = []
}

TYPES.forEach(function (type) {
  Route.prototype[type] = registerFor(type)
})

Route.prototype.any = registerFor(ANY)

/**
 * @param {string} type one of TYPES, or ANY
 * @return {Function}
 * @private
 */

function registerFor (type) {
  var label = type === ANY ? 'any' : type

  return function register () {
    var handlers = utils.flatten(slice.call(arguments))

    if (handlers.length === 0) {
      throw new TypeError('route.' + label + '() requires at least one handler function')
    }

    if (type !== ANY) {
      if (this.type !== undefined && this.type !== type) {
        throw new Error(
          'Route "' + this.path + '" was already registered as "' + this.type + '"; ' +
          'cannot also register "' + type + '" -- a gRPC path serves exactly one call type'
        )
      }

      this.type = type
    }

    var self = this

    handlers.forEach(function (fn) {
      if (typeof fn !== 'function') {
        throw new TypeError('route.' + label + '() requires a function')
      }

      self.stack.push({ type: type, handle: fn })
    })

    return this
  }
}

/**
 * Walk this route's stack for one call. Nearly a literal copy of
 * `router/route.js:92-125` with the verb filter removed (every layer here
 * already belongs to this route's single type) and the `'route'` sentinel
 * (`router/route.js:107`) preserved: a handler calling `next('route')` bails
 * out of the route early without treating it as an error.
 *
 * @param {call} call
 * @param {Function} done
 * @public
 */

Route.prototype.dispatch = function dispatch (call, done) {
  call.type = this.type

  var stack = this.stack
  var idx = 0

  // unary/serverStream have a single, buffered `call.request` -- the
  // handler stack only starts once it's fully read (plan §6.3, the
  // express.json()-before-the-handler analogue). clientStream/bidi read
  // `call` as an AsyncIterable instead, so the stack starts immediately and
  // the handler pulls messages itself.
  if (this.type === 'unary' || this.type === 'serverStream') {
    call.bufferRequest().then(next, done)
  } else {
    next()
  }

  function next (err) {
    if (err === 'route') return done()

    var layer = stack[idx++]

    if (!layer) return done(err)

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
}

module.exports = Route
module.exports.TYPES = TYPES
module.exports.ANY = ANY
