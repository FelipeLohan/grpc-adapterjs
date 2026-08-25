/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

/**
 * `merge-descriptors` in miniature — copy property *descriptors* (not values)
 * from `src` onto `dest`, so getters stay getters.
 *
 * Same role as `mixin()` in `lib/express.js:40-42`.
 *
 * @param {object} dest
 * @param {object} src
 * @param {boolean} [redefine=true]
 * @return {object} dest
 * @private
 */

function mixin (dest, src, redefine) {
  var shouldRedefine = redefine !== false

  for (var name of Object.getOwnPropertyNames(src)) {
    if (!shouldRedefine && Object.prototype.hasOwnProperty.call(dest, name)) continue
    Object.defineProperty(dest, name, Object.getOwnPropertyDescriptor(src, name))
  }

  return dest
}

/**
 * Flatten nested handler arrays, mirroring the `array-flatten` call in
 * `lib/application.js:210`.
 *
 * @param {Array} arr
 * @return {Array}
 * @private
 */

function flatten (arr) {
  return arr.flat(Infinity)
}

/**
 * A gRPC path is rigidly `/{package}.{Service}/{Method}`; the package may be
 * empty and the method preserves the casing from the `.proto`.
 *
 * There are no path parameters, so `path-to-regexp` has nothing to do here
 * (see plan §5.1) — this single regexp replaces the whole matcher pipeline of
 * `router/layer.js:110-146`.
 */

var GRPC_PATH = /^\/((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*)\/([A-Za-z_]\w*)$/

/**
 * @param {string} path
 * @return {{service: string, method: string}|null}
 * @private
 */

function parsePath (path) {
  if (typeof path !== 'string') return null

  var match = GRPC_PATH.exec(path)

  return match === null ? null : { service: match[1], method: match[2] }
}

/**
 * `grpc-timeout` is a positive integer plus a unit suffix.
 *
 * @param {string} value raw header
 * @return {number|null} milliseconds, or null when absent/invalid
 * @private
 */

var TIMEOUT = /^(\d{1,8})([HMSmun])$/

var TIMEOUT_UNITS = {
  H: 3600000,
  M: 60000,
  S: 1000,
  m: 1,
  u: 1 / 1000,
  n: 1 / 1000000
}

function parseTimeout (value) {
  if (typeof value !== 'string') return null

  var match = TIMEOUT.exec(value)

  if (match === null) return null

  return Number(match[1]) * TIMEOUT_UNITS[match[2]]
}

/**
 * `grpc-message` must be percent-encoded — only printable ASCII travels on
 * the wire.
 *
 * @param {string} message
 * @return {string}
 * @private
 */

function encodeMessage (message) {
  return encodeURIComponent(String(message == null ? '' : message))
}

/**
 * Content types that mean "this is gRPC". `application/grpc`, plus any
 * `+proto` / `+json` style suffix.
 *
 * @param {string} value
 * @return {boolean}
 * @private
 */

function isGrpcContentType (value) {
  if (typeof value !== 'string') return false

  var type = value.split(';')[0].trim().toLowerCase()

  return type === 'application/grpc' || type.startsWith('application/grpc+')
}

exports.mixin = mixin
exports.flatten = flatten
exports.GRPC_PATH = GRPC_PATH
exports.parsePath = parsePath
exports.parseTimeout = parseTimeout
exports.encodeMessage = encodeMessage
exports.isGrpcContentType = isGrpcContentType
