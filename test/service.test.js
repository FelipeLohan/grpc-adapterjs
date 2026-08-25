/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var path = require('node:path')

var grpc = require('../lib/grpc')

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')

// app.service() warns (by design) about every .proto method left
// unimplemented; most of these tests don't care about that noise.
var originalWarn = console.warn

test.beforeEach(function () { console.warn = function () {} })
test.afterEach(function () { console.warn = originalWarn })

test('app.service(Service, impl): registers every method with its derived path and type', function () {
  var app = grpc()
  var root = grpc.loadSync(PROTO_PATH)
  var svc = root.lookupService('helloworld.Greeter')

  app.service(svc, {
    SayHello: function () {},
    LotsOfReplies: function () {},
    LotsOfGreetings: function () {},
    BidiHello: function () {}
  })

  var routes = app.router.routes

  assert.equal(routes.get('/helloworld.Greeter/SayHello').type, 'unary')
  assert.equal(routes.get('/helloworld.Greeter/LotsOfReplies').type, 'serverStream')
  assert.equal(routes.get('/helloworld.Greeter/LotsOfGreetings').type, 'clientStream')
  assert.equal(routes.get('/helloworld.Greeter/BidiHello').type, 'bidi')
})

test('app.service(): a method left out of impl gets a warning + UNIMPLEMENTED stub, not silence', function () {
  var app = grpc()
  var root = grpc.loadSync(PROTO_PATH)
  var svc = root.lookupService('helloworld.Greeter')

  var originalWarn = console.warn
  var warnings = []
  console.warn = function (msg) { warnings.push(msg) }

  try {
    app.service(svc, { SayHello: function () {} })
  } finally {
    console.warn = originalWarn
  }

  assert.equal(warnings.length, 3)
  assert.match(warnings[0], /LotsOfReplies/)

  var route = app.router.routes.get('/helloworld.Greeter/LotsOfReplies')
  assert.equal(route.type, 'serverStream')
  assert.equal(route.stack.length, 1)
})

test('app.service(): a typo\'d impl key throws immediately', function () {
  var app = grpc()
  var root = grpc.loadSync(PROTO_PATH)
  var svc = root.lookupService('helloworld.Greeter')

  assert.throws(function () {
    app.service(svc, { SayHelloo: function () {} })
  }, /"SayHelloo" is not a method of helloworld\.Greeter/)
})

test('app.service(): a call-type mismatch against a pre-registered route throws', function () {
  var app = grpc()
  var root = grpc.loadSync(PROTO_PATH)
  var svc = root.lookupService('helloworld.Greeter')

  app.serverStream('/helloworld.Greeter/SayHello', function () {})

  assert.throws(function () {
    app.service(svc, { SayHello: function () {} })
  }, /already registered as "serverStream"/)
})

test('app.service(path): a .proto path with exactly one service is loaded and used directly', function () {
  var app = grpc()

  app.service(PROTO_PATH, { SayHello: function () {} })

  assert.equal(app.router.routes.get('/helloworld.Greeter/SayHello').type, 'unary')
})

test('app.service(): rejects anything that is neither a Service nor a string', function () {
  var app = grpc()

  assert.throws(function () {
    app.service({ not: 'a service' }, {})
  }, TypeError)
})

test('app.service(): attaches request/response codecs to the route', function () {
  var app = grpc()
  var root = grpc.loadSync(PROTO_PATH)
  var svc = root.lookupService('helloworld.Greeter')

  app.service(svc, { SayHello: function () {} })

  var route = app.router.routes.get('/helloworld.Greeter/SayHello')

  assert.equal(typeof route.requestType.decodeMessage, 'function')
  assert.equal(typeof route.responseType.encodeMessage, 'function')
  assert.equal(route.requestType.type.fullName, '.helloworld.HelloRequest')
  assert.equal(route.responseType.type.fullName, '.helloworld.HelloReply')
})
