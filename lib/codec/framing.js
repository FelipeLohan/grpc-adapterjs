/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var Transform = require('node:stream').Transform
var GrpcError = require('../status').GrpcError
var status = require('../status').status

var HEADER_SIZE = 5
var DEFAULT_MAX_MESSAGE_SIZE = 4 * 1024 * 1024

/**
 * Wrap a payload in the gRPC length-prefixed frame:
 * `[1 byte compressed-flag][4 bytes uint32 BE length][payload]`.
 *
 * This is the whole of the gRPC "body format" — the delimiter that
 * `Content-Length` provides in Express (`lib/response.js:213-232`) is
 * carried per-message instead of per-response.
 *
 * @param {Buffer} payload
 * @param {boolean} [compressed]
 * @return {Buffer}
 * @public
 */

function frame (payload, compressed) {
  var header = Buffer.allocUnsafe(HEADER_SIZE)

  header[0] = compressed ? 1 : 0
  header.writeUInt32BE(payload.length, 1)

  return Buffer.concat([header, payload], HEADER_SIZE + payload.length)
}

/**
 * Transform of raw DATA bytes into `{ compressed, payload }` records.
 *
 * Messages do not align with HTTP/2 DATA frames, so this keeps a partial
 * buffer and reassembles across chunk boundaries. Oversized messages fail the
 * call with `8 RESOURCE_EXHAUSTED` rather than allocating.
 *
 * @param {object} [options]
 * @param {number} [options.maxReceiveMessageSize=4194304]
 * @public
 */

class LengthPrefixedDecoder extends Transform {
  constructor (options) {
    super({ readableObjectMode: true, writableObjectMode: false })

    var opts = options || {}

    this.maxMessageSize = opts.maxReceiveMessageSize === undefined
      ? DEFAULT_MAX_MESSAGE_SIZE
      : opts.maxReceiveMessageSize

    this.buffer = null
    this.messageCount = 0
  }

  _transform (chunk, _encoding, callback) {
    this.buffer = this.buffer === null || this.buffer.length === 0
      ? chunk
      : Buffer.concat([this.buffer, chunk])

    for (;;) {
      if (this.buffer.length < HEADER_SIZE) break

      var length = this.buffer.readUInt32BE(1)

      if (this.maxMessageSize >= 0 && length > this.maxMessageSize) {
        return callback(new GrpcError(
          status.RESOURCE_EXHAUSTED,
          'Received message larger than max (' + length + ' vs. ' + this.maxMessageSize + ')'
        ))
      }

      var total = HEADER_SIZE + length

      if (this.buffer.length < total) break

      var compressed = this.buffer[0] !== 0
      var payload = this.buffer.subarray(HEADER_SIZE, total)

      this.buffer = this.buffer.subarray(total)
      this.messageCount++
      this.push({ compressed: compressed, payload: payload })
    }

    callback()
  }

  _flush (callback) {
    if (this.buffer !== null && this.buffer.length !== 0) {
      return callback(new GrpcError(
        status.INTERNAL,
        'Incomplete gRPC frame: ' + this.buffer.length + ' trailing byte(s)'
      ))
    }

    callback()
  }
}

/**
 * Convenience for tests and for `grpc.decodeFrames(buffer)`: split a complete
 * buffer into its frames synchronously.
 *
 * @param {Buffer} buffer
 * @return {Array<{compressed: boolean, payload: Buffer}>}
 * @public
 */

function decodeFrames (buffer) {
  var out = []
  var offset = 0

  while (offset + HEADER_SIZE <= buffer.length) {
    var length = buffer.readUInt32BE(offset + 1)
    var end = offset + HEADER_SIZE + length

    if (end > buffer.length) break

    out.push({
      compressed: buffer[offset] !== 0,
      payload: buffer.subarray(offset + HEADER_SIZE, end)
    })

    offset = end
  }

  return out
}

exports.HEADER_SIZE = HEADER_SIZE
exports.DEFAULT_MAX_MESSAGE_SIZE = DEFAULT_MAX_MESSAGE_SIZE
exports.frame = frame
exports.decodeFrames = decodeFrames
exports.LengthPrefixedDecoder = LengthPrefixedDecoder
