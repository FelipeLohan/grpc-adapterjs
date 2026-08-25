/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

/**
 * Canonical gRPC status codes.
 * https://grpc.github.io/grpc/core/md_doc_statuscodes.html
 */

var status = {
  OK: 0,
  CANCELLED: 1,
  UNKNOWN: 2,
  INVALID_ARGUMENT: 3,
  DEADLINE_EXCEEDED: 4,
  NOT_FOUND: 5,
  ALREADY_EXISTS: 6,
  PERMISSION_DENIED: 7,
  RESOURCE_EXHAUSTED: 8,
  FAILED_PRECONDITION: 9,
  ABORTED: 10,
  OUT_OF_RANGE: 11,
  UNIMPLEMENTED: 12,
  INTERNAL: 13,
  UNAVAILABLE: 14,
  DATA_LOSS: 15,
  UNAUTHENTICATED: 16
}

var names = Object.create(null)

for (var name of Object.keys(status)) {
  names[status[name]] = name
}

/**
 * Name of a status code, e.g. `12` -> `'UNIMPLEMENTED'`.
 *
 * @param {number} code
 * @return {string}
 * @public
 */

function statusName (code) {
  return names[code] || 'UNKNOWN'
}

/**
 * True when `code` is a status code we are allowed to put on the wire.
 *
 * @param {*} code
 * @return {boolean}
 * @public
 */

function isValidStatus (code) {
  return Number.isInteger(code) && code >= 0 && code <= 16
}

/**
 * An error carrying an explicit gRPC status.
 *
 * The `trailers` argument accepts anything `Metadata` accepts and is merged
 * into the trailing metadata block when the call is closed.
 *
 * @param {number} code
 * @param {string} [message]
 * @param {object|Metadata} [trailers]
 * @public
 */

class GrpcError extends Error {
  constructor (code, message, trailers) {
    super(message === undefined ? statusName(code) : String(message))
    this.name = 'GrpcError'
    this.code = isValidStatus(code) ? code : status.UNKNOWN
    this.trailers = trailers
    Error.captureStackTrace?.(this, GrpcError)
  }
}

// one constructor per status name: GrpcError.notFound('nope')
for (var key of Object.keys(status)) {
  if (key === 'OK') continue
  var factory = key.toLowerCase().replace(/_(.)/g, function (_, c) { return c.toUpperCase() })
  GrpcError[factory] = createFactory(status[key])
}

function createFactory (code) {
  return function (message, trailers) {
    return new GrpcError(code, message, trailers)
  }
}

/**
 * Derive `{ code, message, trailers }` from an arbitrary thrown value.
 *
 * Mirrors what `finalhandler` does for HTTP status inference, but on the
 * gRPC axis: an explicit `code` wins, then a few well-known DOM/Node error
 * names, then `2 UNKNOWN`.
 *
 * @param {*} err
 * @return {{code: number, message: string, trailers: *}}
 * @public
 */

function statusFromError (err) {
  if (err == null) {
    return { code: status.UNKNOWN, message: 'unknown error', trailers: undefined }
  }

  if (typeof err !== 'object') {
    return { code: status.UNKNOWN, message: String(err), trailers: undefined }
  }

  var code = isValidStatus(err.code)
    ? err.code
    : isValidStatus(err.status)
      ? err.status
      : inferCode(err)

  return {
    code: code,
    message: typeof err.message === 'string' && err.message !== '' ? err.message : statusName(code),
    trailers: err.trailers || err.metadata
  }
}

function inferCode (err) {
  switch (err.name) {
    case 'AbortError':
      return status.CANCELLED
    case 'TimeoutError':
      return status.DEADLINE_EXCEEDED
    case 'RangeError':
      return status.OUT_OF_RANGE
    case 'TypeError':
      return status.INTERNAL
    default:
      return status.UNKNOWN
  }
}

exports.status = status
exports.statusName = statusName
exports.isValidStatus = isValidStatus
exports.GrpcError = GrpcError
exports.statusFromError = statusFromError
