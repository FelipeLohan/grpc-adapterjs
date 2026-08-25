/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var protobuf = require('protobufjs')
var GrpcError = require('../status').GrpcError
var status = require('../status').status

/**
 * Bridge to `protobufjs` (plan §7). Resolves `.proto` files into a `Root`
 * once at boot and wraps each `Type` in a small codec object exposing the
 * `encodeMessage`/`decodeMessage` shape `call.js` already expects
 * (`call.encode`/`call.decode`, written in Phase 1/3 before this codec
 * existed).
 */

var DEFAULT_TO_OBJECT = { defaults: true, longs: String, enums: String, bytes: Buffer }
var MAX_DECODE_ERROR_LENGTH = 200

/**
 * Load and fully resolve a `.proto` file (or set of files), synchronously.
 *
 * `resolveAll()` runs once here, at boot — never per request — so
 * `method.resolvedRequestType`/`resolvedResponseType` and every field type
 * are already Type instances, not unresolved name strings.
 *
 * @param {string|string[]} filename
 * @return {protobuf.Root}
 * @public
 */

function loadSync (filename) {
  var root = protobuf.loadSync(filename)
  root.resolveAll()
  return root
}

/**
 * Async counterpart of `loadSync`.
 *
 * @param {string|string[]} filename
 * @return {Promise<protobuf.Root>}
 * @public
 */

function load (filename) {
  return protobuf.load(filename).then(function (root) {
    root.resolveAll()
    return root
  })
}

/**
 * Wrap a resolved `protobuf.Type` with the encode/decode shape `call.js`
 * calls into. Never mutates the `Type` itself — a `Type` from a shared
 * `Root` may back more than one route.
 *
 * @param {protobuf.Type} type
 * @return {{type: protobuf.Type, encodeMessage: Function, decodeMessage: Function}}
 * @public
 */

function createCodec (type) {
  return {
    type: type,

    /**
     * `verify()` (opt-out via `app.disable('strict encode')`) -> `create()` ->
     * `encode()` -> `Buffer`. A failed verify is a server bug, not a client
     * error in the usual sense, but it's still surfaced as INVALID_ARGUMENT
     * since there's no better-fitting status and the plan calls this out as
     * a dev-facing check (§7.2).
     *
     * @param {object} message
     * @param {app} [app]
     * @return {Buffer}
     */

    encodeMessage: function encodeMessage (message, app) {
      var strict = !app || typeof app.disabled !== 'function' || !app.disabled('strict encode')

      if (strict) {
        var verifyError = type.verify(message)

        if (verifyError) {
          throw new GrpcError(status.INVALID_ARGUMENT, 'Invalid ' + type.fullName + ': ' + verifyError)
        }
      }

      return Buffer.from(type.encode(type.create(message)).finish())
    },

    /**
     * `decode()` -> `toObject()`. Any decode failure (malformed bytes,
     * truncated frame) is a client error: 3 INVALID_ARGUMENT, with the
     * underlying message truncated so a huge/binary error string never
     * reaches a trailer (plan §7.2).
     *
     * @param {Buffer} payload
     * @param {app} [app]
     * @return {object}
     */

    decodeMessage: function decodeMessage (payload, app) {
      var decoded

      try {
        decoded = type.decode(payload)
      } catch (err) {
        throw new GrpcError(status.INVALID_ARGUMENT, 'Failed to decode ' + type.fullName + ': ' + truncate(err.message))
      }

      var toObjectOptions = (app && app.get('protobuf toObject')) || DEFAULT_TO_OBJECT

      return type.toObject(decoded, toObjectOptions)
    }
  }
}

/**
 * @param {string} message
 * @return {string}
 * @private
 */

function truncate (message) {
  var str = String(message == null ? '' : message)

  return str.length > MAX_DECODE_ERROR_LENGTH
    ? str.slice(0, MAX_DECODE_ERROR_LENGTH) + '…'
    : str
}

exports.protobuf = protobuf
exports.load = load
exports.loadSync = loadSync
exports.createCodec = createCodec
exports.DEFAULT_TO_OBJECT = DEFAULT_TO_OBJECT
