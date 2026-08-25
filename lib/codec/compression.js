/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var zlib = require('node:zlib')
var GrpcError = require('../status').GrpcError
var status = require('../status').status

/**
 * Per-message compression, negotiated with `grpc-encoding` (request) and
 * `grpc-accept-encoding` (response). The 1-byte flag on every frame means a
 * single stream may mix compressed and identity messages.
 */

var codecs = {
  identity: null,
  gzip: { compress: zlib.gzipSync, decompress: zlib.gunzipSync },
  deflate: { compress: zlib.deflateSync, decompress: zlib.inflateSync }
}

var SUPPORTED = Object.keys(codecs)

/**
 * @param {string} name
 * @return {boolean}
 * @public
 */

function supports (name) {
  return Object.prototype.hasOwnProperty.call(codecs, name)
}

/**
 * Pick the response encoding: the first value of `grpc-accept-encoding` we
 * understand, falling back to `identity`.
 *
 * @param {string} [acceptEncoding] raw header value
 * @param {string} [preferred] encoding the app would like to use
 * @return {string}
 * @public
 */

function negotiate (acceptEncoding, preferred) {
  if (!preferred || preferred === 'identity') return 'identity'
  if (!acceptEncoding) return 'identity'

  var accepted = String(acceptEncoding).split(',').map(function (v) { return v.trim() })

  return accepted.includes(preferred) && supports(preferred) ? preferred : 'identity'
}

/**
 * @param {Buffer} payload
 * @param {string} encoding
 * @return {Buffer}
 * @public
 */

function compress (payload, encoding) {
  var codec = codecs[encoding]
  if (!codec) return payload
  return codec.compress(payload)
}

/**
 * @param {Buffer} payload
 * @param {string} encoding value of `grpc-encoding` on the request
 * @return {Buffer}
 * @public
 */

function decompress (payload, encoding) {
  if (!supports(encoding)) {
    throw new GrpcError(
      status.UNIMPLEMENTED,
      'Unsupported grpc-encoding: ' + encoding,
      { 'grpc-accept-encoding': SUPPORTED.join(',') }
    )
  }

  var codec = codecs[encoding]
  if (!codec) return payload

  try {
    return codec.decompress(payload)
  } catch (err) {
    throw new GrpcError(status.INTERNAL, 'Failed to decompress message: ' + err.message)
  }
}

exports.SUPPORTED = SUPPORTED
exports.supports = supports
exports.negotiate = negotiate
exports.compress = compress
exports.decompress = decompress
