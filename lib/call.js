/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var EventEmitter = require('node:events').EventEmitter
var Writable = require('node:stream').Writable
var Metadata = require('./metadata')
var framing = require('./codec/framing')
var compression = require('./codec/compression')
var utils = require('./utils')
var statuses = require('./status')

var status = statuses.status
var GrpcError = statuses.GrpcError
var statusFromError = statuses.statusFromError

/**
 * Lifecycle of the response side of a call.
 *
 * IDLE          nothing on the wire yet — initial metadata is still mutable,
 *               and a failure here becomes a Trailers-Only response.
 * HEADERS_SENT  HEADERS went out with `waitForTrailers`; messages may flow.
 * CLOSED        trailers flushed; every further write is a programming error.
 */

var IDLE = 'idle'
var HEADERS_SENT = 'headers_sent'
var CLOSED = 'closed'

/**
 * Prototype installed on every call, the way `lib/request.js` and
 * `lib/response.js` are installed on `req`/`res` by `app.handle()`
 * (`lib/application.js:171-172`).
 *
 * A single object carries both directions of the RPC: the read side mirrors
 * `req` (`metadata`, `request`, `deadline`), the write side mirrors `res`
 * (`set`, `write`, `send`, `end`) with status living in the trailers instead
 * of the head (plan §1.2 A2).
 */

var call = Object.create(EventEmitter.prototype)

/**
 * Wire a freshly created call object to its http2 stream.
 *
 * @param {Http2ServerStream} stream
 * @param {object} headers raw request headers
 * @private
 */

call.init = function init (stream, headers) {
  EventEmitter.call(this)

  this.stream = stream
  this.headers = headers
  this.path = headers[':path']
  this.metadata = new Metadata(headers)

  var parsed = utils.parsePath(this.path)

  this.service = parsed === null ? undefined : parsed.service
  this.methodName = parsed === null ? undefined : parsed.method

  this.state = IDLE
  this.initialMetadata = new Metadata()
  this.trailingMetadata = new Metadata()
  this.statusCode = status.OK
  this.statusMessage = undefined
  this.messagesSent = 0
  this.cancelled = false

  this.requestEncoding = headers['grpc-encoding'] || 'identity'
  this.responseEncoding = compression.negotiate(
    headers['grpc-accept-encoding'],
    this.app && this.app.get('compression')
  )

  this.type = undefined
  this.request = undefined

  this.bindDecoder()
  this.bindDeadline(headers['grpc-timeout'])
  this.bindCancellation()

  return this
}

/**
 * Wire the request-reading side: raw DATA bytes -> reassembled frames
 * (`LengthPrefixedDecoder`, plan §6.1). Piping starts immediately — before
 * routing even knows this call's type — so a message-size violation or a
 * malformed frame surfaces the moment it arrives rather than being silently
 * buffered.
 *
 * A permanent listener records decode errors on `this._decodeError` instead
 * of letting them go unhandled: `bufferRequest()`/the async iterator may not
 * start consuming until after routing decides this call's type, and an
 * `'error'` event with no listener crashes the process.
 *
 * @private
 */

call.bindDecoder = function bindDecoder () {
  this._decoder = new framing.LengthPrefixedDecoder({
    maxReceiveMessageSize: this.app ? this.app.get('max receive message size') : undefined
  })

  this._decodeError = null

  var self = this

  this._decoder.on('error', function onDecodeError (err) {
    self._decodeError = err
  })

  this.stream.pipe(this._decoder)
}

/**
 * Decompress (if needed) and decode one frame into the message shape a
 * handler sees. Until the protobuf codec lands (plan §7.2), a route without
 * a resolved request type gets the raw decompressed `Buffer`.
 *
 * @param {{compressed: boolean, payload: Buffer}} frame
 * @return {*}
 * @private
 */

call.decodeFrame = function decodeFrame (frame) {
  var payload = frame.compressed
    ? compression.decompress(frame.payload, this.requestEncoding)
    : frame.payload

  return this.decode(payload)
}

/**
 * @param {Buffer} payload
 * @return {*}
 * @private
 */

call.decode = function decode (payload) {
  if (this.requestType === undefined) return payload

  return this.requestType.decodeMessage(payload, this.app)
}

/**
 * Buffer the single request message for a unary/serverStream call and
 * populate `call.request`, resolving only once `END_STREAM` has been seen —
 * the direct analogue of `express.json()` running before the handler
 * (plan §6.3). A second message before the stream ends is a client error:
 * `3 INVALID_ARGUMENT`, not a framework bug.
 *
 * @return {Promise<void>}
 * @private
 */

call.bufferRequest = function bufferRequest () {
  if (this._decodeError) return Promise.reject(this._decodeError)

  var self = this
  var received = false

  return new Promise(function (resolve, reject) {
    function cleanup () {
      self._decoder.removeListener('data', onData)
      self._decoder.removeListener('end', onEnd)
      self._decoder.removeListener('error', onError)
    }

    function onData (frame) {
      if (received) {
        cleanup()
        self._decoder.destroy()
        return reject(new GrpcError(
          status.INVALID_ARGUMENT,
          self.type + ' call received more than one request message'
        ))
      }

      received = true

      try {
        self.request = self.decodeFrame(frame)
      } catch (err) {
        cleanup()
        self._decoder.destroy()
        reject(err)
      }
    }

    function onEnd () {
      cleanup()
      resolve()
    }

    function onError (err) {
      cleanup()
      reject(err)
    }

    self._decoder.on('data', onData)
    self._decoder.on('end', onEnd)
    self._decoder.on('error', onError)
  })
}

/**
 * `call` as an `AsyncIterable` of decoded request messages, for
 * clientStream/bidi handlers (`for await (const msg of call)`, plan §6.2).
 * Delegates to the decoder's own async iterator, so pull-based backpressure
 * (the consumer only reads when it asks) comes for free from Node's
 * `Readable` implementation rather than anything we wrote.
 *
 * @return {AsyncIterator}
 * @public
 */

call[Symbol.asyncIterator] = function asyncIterator () {
  var self = this
  var source = this._decoder[Symbol.asyncIterator]()

  return {
    next: function next () {
      return source.next().then(function (result) {
        if (result.done) return result

        return { done: false, value: self.decodeFrame(result.value) }
      })
    },
    return: function (value) {
      return typeof source.return === 'function'
        ? source.return(value)
        : Promise.resolve({ done: true, value: value })
    },
    throw: function (err) {
      return typeof source.throw === 'function'
        ? source.throw(err)
        : Promise.reject(err)
    }
  }
}

/**
 * `grpc-timeout: 5S` becomes a `deadline` and an `AbortSignal`, so handlers
 * can hand `call.signal` straight to `fetch` or a database driver.
 *
 * @param {string} [timeout] raw `grpc-timeout` header
 * @private
 */

call.bindDeadline = function bindDeadline (timeout) {
  this.controller = new AbortController()
  this.signal = this.controller.signal

  var ms = utils.parseTimeout(timeout)

  if (ms === null) {
    this.deadline = Infinity
    this.deadlineTimer = null
    return
  }

  this.deadline = Date.now() + ms

  var self = this

  this.deadlineTimer = setTimeout(function onDeadline () {
    self.deadlineTimer = null
    self.abort(new GrpcError(status.DEADLINE_EXCEEDED, 'Deadline exceeded'))
  }, Math.max(0, Math.ceil(ms)))
}

/**
 * A client RST_STREAM (or a dead socket) aborts `call.signal` so in-flight
 * work can unwind instead of writing to a stream nobody is reading.
 *
 * @private
 */

call.bindCancellation = function bindCancellation () {
  var self = this

  this.stream.once('close', function onClose () {
    self.clearDeadline()

    if (self.state === CLOSED) return

    self.state = CLOSED
    self.cancelled = true

    var err = new GrpcError(status.CANCELLED, 'Call cancelled by client')

    if (!self.signal.aborted) self.controller.abort(err)
    if (self._decoder && !self._decoder.destroyed) self._decoder.destroy(err)

    self.emit('cancelled')
  })

  this.stream.on('error', function onError (err) {
    self.emit('streamError', err)
  })

  this.stream.on('drain', function onDrain () {
    self.emit('drain')
  })
}

/**
 * Abort the call from the server side: trip the signal, then close with the
 * matching status if the stream is still open.
 *
 * @param {Error} err
 * @private
 */

call.abort = function abort (err) {
  if (!this.signal.aborted) this.controller.abort(err)
  if (this._decoder && !this._decoder.destroyed) this._decoder.destroy(err)

  this.emit('cancelled')

  if (this.state !== CLOSED) this.fail(err)
}

call.clearDeadline = function clearDeadline () {
  if (this.deadlineTimer !== null) {
    clearTimeout(this.deadlineTimer)
    this.deadlineTimer = null
  }
}

/**
 * Set initial metadata — the analogue of `res.set()` (`lib/response.js:558`).
 *
 * Only legal before the first message: initial metadata rides in the HEADERS
 * block, which is already on the wire once anything has been written.
 *
 * @param {string|object} key
 * @param {string|Buffer} [value]
 * @return {call} for chaining
 * @public
 */

call.set = function set (key, value) {
  if (this.state !== IDLE) {
    throw new Error('Cannot set initial metadata after headers were sent; use call.trailer() instead')
  }

  if (typeof key === 'object' && key !== null) this.initialMetadata.merge(key)
  else this.initialMetadata.set(key, value)

  return this
}

/**
 * Set trailing metadata. Legal at any point until the call closes — this is
 * the block that also carries `grpc-status`.
 *
 * @param {string|object} key
 * @param {string|Buffer} [value]
 * @return {call} for chaining
 * @public
 */

call.trailer = function trailer (key, value) {
  if (this.state === CLOSED) {
    throw new Error('Cannot set trailing metadata after the call was closed')
  }

  if (typeof key === 'object' && key !== null) this.trailingMetadata.merge(key)
  else this.trailingMetadata.set(key, value)

  return this
}

/**
 * Record the status this call will close with. Unlike `res.status()`
 * (`lib/response.js:59`), nothing is written here — the code only reaches the
 * wire in the trailers (plan §1.2 A2).
 *
 * @param {number} code
 * @param {string} [message]
 * @return {call} for chaining
 * @public
 */

call.status = function setStatus (code, message) {
  if (!statuses.isValidStatus(code)) {
    throw new RangeError('Invalid gRPC status code: ' + code)
  }

  this.statusCode = code
  if (message !== undefined) this.statusMessage = message

  return this
}

/**
 * Flush the HEADERS block. Idempotent, and called implicitly by the first
 * `write()`/`send()`.
 *
 * @return {call} for chaining
 * @public
 */

call.respond = function respond () {
  if (this.state !== IDLE) return this

  this.state = HEADERS_SENT

  if (this.stream.destroyed || this.stream.headersSent) return this

  var self = this

  this.stream.on('wantTrailers', function onWantTrailers () {
    if (self.stream.destroyed) return
    self.stream.sendTrailers(self.buildTrailers())
  })

  this.stream.respond(this.buildHeaders(), { waitForTrailers: true })

  return this
}

/**
 * @return {object} the initial HEADERS block
 * @private
 */

call.buildHeaders = function buildHeaders () {
  var headers = this.initialMetadata.toHeaders()

  headers[':status'] = 200
  headers['content-type'] = this.contentType()
  headers['grpc-accept-encoding'] = compression.SUPPORTED.join(',')

  if (this.responseEncoding !== 'identity') headers['grpc-encoding'] = this.responseEncoding

  return headers
}

/**
 * @return {object} the trailing HEADERS block
 * @private
 */

call.buildTrailers = function buildTrailers () {
  var trailers = this.trailingMetadata.toHeaders()

  trailers['grpc-status'] = String(this.statusCode)

  if (this.statusCode !== status.OK) {
    trailers['grpc-message'] = utils.encodeMessage(
      this.statusMessage === undefined ? statuses.statusName(this.statusCode) : this.statusMessage
    )
  } else if (this.statusMessage !== undefined) {
    trailers['grpc-message'] = utils.encodeMessage(this.statusMessage)
  }

  return trailers
}

/**
 * Echo back the request's gRPC content-type sub-type when there is one, so a
 * `application/grpc+json` client is answered in kind.
 *
 * @return {string}
 * @private
 */

call.contentType = function contentType () {
  var requested = this.headers['content-type']

  return utils.isGrpcContentType(requested)
    ? requested.split(';')[0].trim().toLowerCase()
    : 'application/grpc+proto'
}

/**
 * Serialize a message to its wire payload.
 *
 * Until the protobuf codec lands (plan §7.2) a route without a resolved
 * response type accepts `Buffer`s only — the "raw buffers" mode of milestone
 * M2.
 *
 * @param {*} message
 * @return {Buffer}
 * @private
 */

call.encode = function encode (message) {
  if (Buffer.isBuffer(message)) return message
  if (message instanceof Uint8Array) return Buffer.from(message)

  if (this.responseType === undefined) {
    throw new TypeError(
      'call.write()/send() needs a Buffer when the route has no response type; ' +
      'register the route through app.service() to encode plain objects'
    )
  }

  return this.responseType.encodeMessage(message, this.app)
}

/**
 * Frame, (optionally) compress and write one message. The shared mechanics
 * behind both `write()` and `send()` — factored out so `send()`'s internal
 * write isn't subject to `write()`'s own "not valid for clientStream" guard
 * below (`send()` is precisely how a clientStream call is allowed to reply).
 *
 * @param {*} message
 * @return {boolean} false when the buffer is full (await `'drain'`)
 * @private
 */

call._writeFrame = function _writeFrame (message) {
  this.respond()

  var payload = this.encode(message)
  var compressed = false

  if (this.responseEncoding !== 'identity') {
    payload = compression.compress(payload, this.responseEncoding)
    compressed = true
  }

  this.messagesSent++

  return this.stream.write(framing.frame(payload, compressed))
}

/**
 * Write one message. Non-terminal — the streaming counterpart of
 * `res.write()`. Not valid for a clientStream call, which replies exactly
 * once via `send()` after reading every request message (plan §6.3).
 *
 * @param {*} message
 * @return {boolean} false when the buffer is full (await `'drain'`)
 * @public
 */

call.write = function write (message) {
  this.assertWritable('write')

  if (this.type === 'clientStream') {
    throw new Error('call.write() is not valid for a clientStream call; use call.send() ' +
      'once every request message has been read')
  }

  return this._writeFrame(message)
}

/**
 * `write()` that resolves once the message is actually accepted, so handlers
 * can `await` instead of listening for `'drain'` by hand.
 *
 * @param {*} message
 * @return {Promise<void>}
 * @public
 */

call.writeAsync = function writeAsync (message) {
  if (this.write(message)) return Promise.resolve()

  var self = this

  return new Promise(function (resolve, reject) {
    function cleanup () {
      self.stream.removeListener('drain', onDrain)
      self.stream.removeListener('close', onClose)
      self.stream.removeListener('error', onClose)
    }

    function onDrain () {
      cleanup()
      resolve()
    }

    function onClose (err) {
      cleanup()
      reject(err || new GrpcError(status.CANCELLED, 'Call cancelled by client'))
    }

    self.stream.once('drain', onDrain)
    self.stream.once('close', onClose)
    self.stream.once('error', onClose)
  })
}

/**
 * A real `stream.Writable` (object mode) wrapping `write()`/`end()`, for
 * handlers that would rather `pipeline(source, call.writer())` than call
 * `write()`/`writeAsync()` by hand. `call` itself isn't a stream instance —
 * it carries too much other state for that — so this is the adapter the
 * plan's "pipeline(source, call)" becomes in practice.
 *
 * @return {Writable}
 * @public
 */

call.writer = function writer () {
  var self = this

  return new Writable({
    objectMode: true,
    write: function (chunk, encoding, callback) {
      if (self.write(chunk)) callback()
      else self.stream.once('drain', function () { callback() })
    },
    final: function (callback) {
      self.end()
      callback()
    }
  })
}

/**
 * Write one message and close — the terminal shape, mirroring `res.send()`
 * (`lib/response.js:126`). Used by unary and client-streaming handlers.
 *
 * @param {*} message
 * @return {call}
 * @public
 */

call.send = function send (message) {
  this.assertWritable('send')

  if (this.messagesSent !== 0) {
    throw new Error('call.send() is terminal and was already called; use call.write() to stream')
  }

  this._writeFrame(message)

  return this.end()
}

/**
 * Close the call: flush trailers (with `grpc-status`) and end the stream.
 *
 * @param {*} [message] optional final message, as a convenience
 * @return {call}
 * @public
 */

call.end = function end (message) {
  if (this.state === CLOSED) return this

  if (message !== undefined) this.write(message)

  this.respond()
  this.clearDeadline()
  this.state = CLOSED

  if (!this.stream.destroyed && this.stream.writable) this.stream.end()

  return this
}

/**
 * Close the call with an error. Before any HEADERS this produces a
 * Trailers-Only response — a single HEADERS frame with END_STREAM, which is
 * how gRPC reports "this never started" (plan §4.4).
 *
 * @param {Error|number} err
 * @param {string} [message]
 * @return {call}
 * @public
 */

call.fail = function fail (err, message) {
  if (this.state === CLOSED) return this

  var resolved = typeof err === 'number'
    ? { code: err, message: message, trailers: undefined }
    : statusFromError(err)

  if (resolved.trailers) this.trailingMetadata.merge(resolved.trailers)

  this.statusCode = resolved.code
  this.statusMessage = resolved.message

  if (this.state === IDLE) return this.trailersOnly()

  return this.end()
}

/**
 * Single HEADERS frame carrying the status, with END_STREAM set.
 *
 * @return {call}
 * @private
 */

call.trailersOnly = function trailersOnly () {
  this.clearDeadline()
  this.state = CLOSED

  if (this.stream.destroyed || this.stream.headersSent) return this

  var headers = this.buildTrailers()

  headers[':status'] = 200
  headers['content-type'] = this.contentType()

  this.stream.respond(headers, { endStream: true })

  return this
}

/**
 * @param {string} method name used in the error message
 * @private
 */

call.assertWritable = function assertWritable (method) {
  if (this.state === CLOSED) {
    throw new Error('call.' + method + '() after the call was closed' +
      (this.cancelled ? ' (client cancelled)' : ''))
  }
}

/**
 * True once the response has been closed out.
 *
 * @return {boolean}
 * @public
 */

Object.defineProperty(call, 'closed', {
  configurable: true,
  enumerable: false,
  get: function () { return this.state === CLOSED }
})

/**
 * True while messages may still be written.
 *
 * @return {boolean}
 * @public
 */

Object.defineProperty(call, 'writable', {
  configurable: true,
  enumerable: false,
  get: function () { return this.state !== CLOSED && !this.stream.destroyed }
})

module.exports = call
module.exports.IDLE = IDLE
module.exports.HEADERS_SENT = HEADERS_SENT
module.exports.CLOSED = CLOSED
