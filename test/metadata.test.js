/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')

var Metadata = require('../lib/metadata')

test('merge() skips pseudo-headers and protocol-reserved keys', function () {
  var md = new Metadata({
    ':path': '/helloworld.Greeter/SayHello',
    'content-type': 'application/grpc+proto',
    'grpc-timeout': '5S',
    'x-trace': 'abc'
  })

  assert.deepEqual(md.keys(), ['x-trace'])
})

test('keys are matched case-insensitively', function () {
  var md = new Metadata({ 'X-Trace-Id': 'abc' })

  assert.equal(md.getFirst('x-trace-id'), 'abc')
  assert.equal(md.getFirst('X-TRACE-ID'), 'abc')
})

test('-bin keys round-trip through base64 as Buffers', function () {
  var md = new Metadata()

  md.set('token-bin', Buffer.from('hello'))

  assert.deepEqual(md.get('token-bin'), [Buffer.from('hello')])
  assert.equal(md.toHeaders()['token-bin'], Buffer.from('hello').toString('base64'))
})

test('non-bin keys reject Buffer values and non-printable-ASCII strings', function () {
  var md = new Metadata()

  assert.throws(function () { md.set('x-trace', Buffer.from('a')) }, TypeError)
  assert.throws(function () { md.set('x-trace', 'line1\nline2') })
})

test('add() appends, set() replaces', function () {
  var md = new Metadata()

  md.add('x-trace', 'a')
  md.add('x-trace', 'b')
  assert.deepEqual(md.get('x-trace'), ['a', 'b'])

  md.set('x-trace', 'c')
  assert.deepEqual(md.get('x-trace'), ['c'])
})

test('merge(Metadata) copies every value without losing multimap entries', function () {
  var a = new Metadata()
  a.add('x-trace', '1')
  a.add('x-trace', '2')

  var b = new Metadata()
  b.merge(a)

  assert.deepEqual(b.get('x-trace'), ['1', '2'])
})
