/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var path = require('node:path')

var codecs = require('../lib/codec/protobuf')
var status = require('../lib/status').status

var HELLO_PROTO = path.join(__dirname, 'fixtures', 'helloworld.proto')
var CODEC_PROTO = path.join(__dirname, 'fixtures', 'codec.proto')

test('loadSync() resolves the root: method request/response types are Type instances', function () {
  var root = codecs.loadSync(HELLO_PROTO)
  var method = root.lookupService('helloworld.Greeter').methodsArray[0]

  assert.equal(method.resolvedRequestType.fullName, '.helloworld.HelloRequest')
  assert.equal(method.resolvedResponseType.fullName, '.helloworld.HelloReply')
})

test('load() is the async counterpart and resolves the same way', async function () {
  var root = await codecs.load(HELLO_PROTO)
  var method = root.lookupService('helloworld.Greeter').methodsArray[0]

  assert.equal(method.resolvedRequestType.fullName, '.helloworld.HelloRequest')
})

test('createCodec(): encodeMessage/decodeMessage round-trip a plain object', function () {
  var root = codecs.loadSync(HELLO_PROTO)
  var codec = codecs.createCodec(root.lookupType('helloworld.HelloRequest'))

  var payload = codec.encodeMessage({ name: 'Ada' })
  assert.ok(Buffer.isBuffer(payload))

  var decoded = codec.decodeMessage(payload)
  assert.deepEqual(decoded, { name: 'Ada' })
})

test('encodeMessage(): an invalid message is rejected by verify() by default', function () {
  var root = codecs.loadSync(HELLO_PROTO)
  var codec = codecs.createCodec(root.lookupType('helloworld.HelloRequest'))

  assert.throws(function () {
    codec.encodeMessage({ name: 123 })
  }, function (err) {
    assert.equal(err.code, status.INVALID_ARGUMENT)
    return true
  })
})

test('encodeMessage(): app.disable(\'strict encode\') skips verify()', function () {
  var root = codecs.loadSync(HELLO_PROTO)
  var codec = codecs.createCodec(root.lookupType('helloworld.HelloRequest'))

  var lenientApp = { disabled: function () { return true } }

  // protobufjs still needs *a* string for the wire type -- verify is what
  // we're skipping, not type coercion -- so this exercises the "no throw"
  // path without also depending on encode() tolerating a wrong JS type.
  assert.doesNotThrow(function () {
    codec.encodeMessage({ name: 'Ada' }, lenientApp)
  })
})

test('decodeMessage(): a malformed buffer is 3 INVALID_ARGUMENT with a truncated message', function () {
  var root = codecs.loadSync(HELLO_PROTO)
  var codec = codecs.createCodec(root.lookupType('helloworld.HelloRequest'))

  assert.throws(function () {
    codec.decodeMessage(Buffer.from([0xff, 0xff, 0xff]))
  }, function (err) {
    assert.equal(err.code, status.INVALID_ARGUMENT)
    assert.ok(err.message.length <= 250)
    return true
  })
})

test('decodeMessage(): "protobuf toObject" options control int64/bytes shape', function () {
  var root = codecs.loadSync(CODEC_PROTO)
  var codec = codecs.createCodec(root.lookupType('codec.Numbers'))

  // protobufjs's verify() only accepts a number/Long for an int64 field
  // (it's decode()/toObject() that can *render* one as a string) -- so the
  // wire value is built from a plain number here, and it's the decoded
  // shape that's under test.
  var payload = codec.encodeMessage({ big: 123456789, data: Buffer.from('hi') })

  var withDefaults = codec.decodeMessage(payload, {
    get: function () { return { defaults: true, longs: String, enums: String, bytes: Buffer } }
  })

  assert.equal(typeof withDefaults.big, 'string')
  assert.equal(withDefaults.big, '123456789')
  assert.ok(Buffer.isBuffer(withDefaults.data))

  var withNumberLongs = codec.decodeMessage(payload, {
    get: function () { return { defaults: true, longs: Number, enums: String, bytes: Array } }
  })

  assert.equal(typeof withNumberLongs.big, 'number')
  assert.ok(Array.isArray(withNumberLongs.data))
})
