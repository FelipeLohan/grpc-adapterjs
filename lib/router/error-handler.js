/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

/**
 * Error-handler detection is arity-based by default (`(err, call, next)` has
 * arity 3), the direct analogue of Express's `fn.length !== 4` check
 * (`router/layer.js:62`, `cec5780d^`). The 2-vs-3 split is more fragile than
 * Express's 3-vs-4 one — arrow functions with default params, transpilation
 * and wrappers all mangle `Function#length` — so an explicit flag is the
 * canonical, documented path; arity is only the fallback (plan §5.2).
 */

var kIsErrorHandler = Symbol('express-grpc.isErrorHandler')

/**
 * Mark `fn` as an error interceptor regardless of what `Function#length`
 * reports.
 *
 *   app.use(grpc.errorHandler((err, call, next) => { ... }))
 *
 * @param {Function} fn `(err, call, next)`
 * @return {Function} fn, marked
 * @public
 */

function errorHandler (fn) {
  if (typeof fn !== 'function') {
    throw new TypeError('grpc.errorHandler() requires a function')
  }

  if (fn.length !== 3) {
    throw new TypeError(
      'grpc.errorHandler() expects a function of arity 3: (err, call, next), got arity ' + fn.length
    )
  }

  fn[kIsErrorHandler] = true

  return fn
}

/**
 * @param {Function} fn
 * @return {boolean}
 * @public
 */

function isErrorHandler (fn) {
  return fn.length === 3 || fn[kIsErrorHandler] === true
}

exports.kIsErrorHandler = kIsErrorHandler
exports.errorHandler = errorHandler
exports.isErrorHandler = isErrorHandler
