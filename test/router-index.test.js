/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')

var Router = require('../lib/router')
var errorHandler = require('../lib/router/error-handler').errorHandler
var status = require('../lib/status').status

function fakeCall (path) {
  return {
    path: path,
    cancelled: false,
    signal: { aborted: false },
    bufferRequest: function () { return Promise.resolve() }
  }
}

test('interceptors run in registration order: global -> service -> route', function (t, done) {
  var order = []
  var router = new Router()

  router.use(function (call, next) { order.push('global'); next() })
  router.use('/helloworld.Greeter', function (call, next) { order.push('service'); next() })
  router.route('/helloworld.Greeter/SayHello').unary(function (call, next) { order.push('route'); next() })

  router.handle(fakeCall('/helloworld.Greeter/SayHello'), function () {
    assert.deepEqual(order, ['global', 'service', 'route'])
    done()
  })
})

test('a service-scoped interceptor does not run for a different service', function (t, done) {
  var order = []
  var router = new Router()

  router.use('/helloworld.Greeter', function (call, next) { order.push('greeter-only'); next() })
  router.route('/other.Service/M').unary(function (call, next) { order.push('route'); next() })

  router.handle(fakeCall('/other.Service/M'), function () {
    assert.deepEqual(order, ['route'])
    done()
  })
})

test('no matching route -> done() with no error (finalHandler turns this into UNIMPLEMENTED)', function (t, done) {
  var router = new Router()

  router.handle(fakeCall('/helloworld.Greeter/Nope'), function (err) {
    assert.equal(err, undefined)
    done()
  })
})

test('an error from a use() layer skips later normal layers and reaches a later error handler', function (t, done) {
  var order = []
  var router = new Router()

  router.use(function (call, next) { order.push('a'); next(new Error('boom')) })
  router.use(function (call, next) { order.push('b-skipped'); next() })
  router.use(errorHandler(function (err, call, next) {
    order.push('handled:' + err.message)
    next()
  }))
  router.route('/helloworld.Greeter/SayHello').unary(function (call, next) { order.push('route'); next() })

  router.handle(fakeCall('/helloworld.Greeter/SayHello'), function (err) {
    order.push('done:' + err)
    assert.deepEqual(order, ['a', 'handled:boom', 'route', 'done:undefined'])
    done()
  })
})

test('an error from inside a route can be caught by an error handler registered after it', function (t, done) {
  var order = []
  var router = new Router()

  router.route('/helloworld.Greeter/SayHello').unary(function () {
    order.push('route-throws')
    throw new Error('route boom')
  })

  router.use(errorHandler(function (err, call, next) {
    order.push('caught:' + err.message)
    next()
  }))

  router.handle(fakeCall('/helloworld.Greeter/SayHello'), function (err) {
    assert.deepEqual(order, ['route-throws', 'caught:route boom'])
    assert.equal(err, undefined)
    done()
  })
})

test('an already-aborted signal short-circuits with DEADLINE_EXCEEDED before any layer runs', function (t, done) {
  var ran = false
  var router = new Router()

  router.use(function (call, next) { ran = true; next() })

  var call = fakeCall('/helloworld.Greeter/SayHello')
  call.signal.aborted = true

  router.handle(call, function (err) {
    assert.equal(ran, false)
    assert.equal(err.code, status.DEADLINE_EXCEEDED)
    done()
  })
})

test('an already-aborted + cancelled signal reports CANCELLED instead', function (t, done) {
  var router = new Router()

  var call = fakeCall('/helloworld.Greeter/SayHello')
  call.signal.aborted = true
  call.cancelled = true

  router.handle(call, function (err) {
    assert.equal(err.code, status.CANCELLED)
    done()
  })
})
