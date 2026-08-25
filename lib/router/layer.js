/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var SLASH = 0x2f // '/'

/**
 * One entry in the router's stack. Espelha `router/layer.js`, drastically
 * simplified: since a gRPC path is fully static (§5.1), there is no
 * `path-to-regexp` compilation, no captured params, no regex `.exec()` per
 * request — `match()` is a plain string comparison.
 *
 * @param {string} path exact method path or a service/wildcard prefix
 * @param {object} options
 * @param {boolean} [options.exact=false] true for a route layer (exact path
 *   only); false for a `use()` layer (prefix/wildcard)
 * @param {Function} fn `(call)`, `(call, next)`, or `(err, call, next)`
 * @public
 */

function Layer (path, options, fn) {
  var opts = options || {}

  this.handle = fn
  this.name = fn.name || '<anonymous>'
  this.path = path
  this.route = undefined // set by Router#route() for route-wrapping layers
  this.match = compileMatcher(path, opts)
}

/**
 * @param {string} path
 * @param {object} opts
 * @return {function(string): boolean}
 * @private
 */

function compileMatcher (path, opts) {
  if (opts.exact) {
    return function match (candidate) {
      return candidate === path
    }
  }

  if (path === '/' || path === '*') {
    return function match () {
      return true
    }
  }

  var prefix = path.charCodeAt(path.length - 1) === SLASH ? path.slice(0, -1) : path

  return function match (candidate) {
    if (candidate === prefix) return true

    return candidate.length > prefix.length &&
      candidate.charCodeAt(prefix.length) === SLASH &&
      candidate.startsWith(prefix)
  }
}

module.exports = Layer
