/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var EventEmitter = require('node:events').EventEmitter

var callProto = require('../lib/call')
var status = require('../lib/status').status

/**
 * A minimal double for a ServerHttp2Stream: just enough surface for
 * call.js to drive (respond/write/end/sendTrailers + the events it
 * listens on), without paying for a real http2 connection.
 */

function fakeStream () {
  var stream = new EventEmitter()

  stream.destroyed = false
  stream.headersSent = false
  stream.writable = true
  stream.written = []
  stream.headers = null
  stream.trailers = null
  stream.ended = false

  stream.respond = function respond (headers, opts) {
    stream.headersSent = true
    stream.headers = headers
    stream.waitingForTrailers = Boolean(opts && opts.waitForTrailers)

    if (opts && opts.endStream) {
      stream.ended = true
      stream.destroyed = true
    }
  }

  stream.write = function write (chunk) {
    stream.written.push(chunk)
    return true
  }

  stream.end = function end () {
    stream.ended = true

    if (stream.waitingForTrailers) stream.emit('wantTrailers')
    else stream.destroyed = true
  }

  stream.pipe = function pipe (destination) {
    return destination
  }

  stream.unpipe = function unpipe () {
    return stream
  }

  stream.resume = function resume () {
    return stream
  }

  stream.sendTrailers = function sendTrailers (trailers) {
    stream.trailers = trailers
    stream.destroyed = true
  }

  return stream
}

function makeCall (headers) {
  var call = Object.create(callProto)
  call.init(fakeStream(), Object.assign({
    ':method': 'POST',
    ':path': '/helloworld.Greeter/SayHello',
    'content-type': 'application/grpc+proto'
  }, headers))
  return call
}

test('set() is legal in IDLE and rejected once headers are sent', function () {
  var call = makeCall()

  call.set('x-a', '1')
  call.write(Buffer.from(''))

  assert.throws(function () { call.set('x-b', '2') }, /after headers were sent/)
})

test('trailer() stays legal after headers are sent, rejected once closed', function () {
  var call = makeCall()

  call.write(Buffer.from(''))
  call.trailer('x-cost', '1')

  call.end()

  assert.throws(function () { call.trailer('x-late', '1') }, /after the call was closed/)
})

test('send() is terminal: a second call throws (call already closed)', function () {
  var call = makeCall()

  call.send(Buffer.from('one'))

  assert.throws(function () { call.send(Buffer.from('two')) }, /after the call was closed/)
})

test('send() rejects mixing with a prior write() (streaming already started)', function () {
  var call = makeCall()

  call.write(Buffer.from('one'))

  assert.throws(function () { call.send(Buffer.from('two')) }, /terminal/)
})

test('write()/end() after close throw a clear error', function () {
  var call = makeCall()

  call.end()

  assert.throws(function () { call.write(Buffer.from('x')) }, /after the call was closed/)
})

test('end() is idempotent', function () {
  var call = makeCall()

  call.end()
  assert.doesNotThrow(function () { call.end() })
})

test('fail() before any write produces a Trailers-Only response', function () {
  var call = makeCall()

  call.fail(status.NOT_FOUND, 'nope')

  assert.equal(call.stream.headers['grpc-status'], String(status.NOT_FOUND))
  assert.equal(call.stream.headers['grpc-message'], 'nope')
  assert.equal(call.stream.written.length, 0)
  assert.equal(call.closed, true)
})

test('fail() after a write flushes trailers instead of Trailers-Only', function () {
  var call = makeCall()

  call.write(Buffer.from('partial'))
  call.fail(status.INTERNAL, 'boom')

  assert.equal(call.stream.written.length, 1)
  assert.equal(call.stream.trailers['grpc-status'], String(status.INTERNAL))
  assert.equal(call.stream.trailers['grpc-message'], 'boom')
})

test('invalid status code is rejected', function () {
  var call = makeCall()

  assert.throws(function () { call.status(999) }, RangeError)
})

test('closed and writable reflect the state machine', function () {
  var call = makeCall()

  assert.equal(call.closed, false)
  assert.equal(call.writable, true)

  call.end()

  assert.equal(call.closed, true)
  assert.equal(call.writable, false)
})
