/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')

var Route = require('../lib/router/route')
var errorHandler = require('../lib/router/error-handler').errorHandler

/**
 * A minimal call double: just enough for Route.dispatch to run. Unary/
 * serverStream routes await `bufferRequest()` before starting the stack
 * (plan §6.3), so every fake call here resolves it immediately -- the
 * buffering behavior itself is covered by the call.js tests instead.
 */

function fakeCall () {
  return { bufferRequest: function () { return Promise.resolve() } }
}

test('a route serves exactly one call type; a second, different type throws', function () {
  var route = new Route('/helloworld.Greeter/SayHello')

  route.unary(function () {})

  assert.throws(function () {
    route.serverStream(function () {})
  }, /already registered as "unary"/)
})

test('.any() runs regardless of the route\'s registered type', function (t, done) {
  var order = []

  var route = new Route('/helloworld.Greeter/SayHello')

  route.any(function (call, next) { order.push('any'); next() })
  route.unary(function (call, next) { order.push('unary'); next() })

  route.dispatch(fakeCall(), function () {
    assert.deepEqual(order, ['any', 'unary'])
    done()
  })
})

test('dispatch() sets call.type before running any handler', function (t, done) {
  var seen

  var route = new Route('/helloworld.Greeter/LotsOfReplies')
  route.serverStream(function (call, next) { seen = call.type; next() })

  route.dispatch(fakeCall(), function () {
    assert.equal(seen, 'serverStream')
    done()
  })
})

test('a thrown error skips remaining normal handlers and reaches an error handler', function (t, done) {
  var order = []

  var route = new Route('/helloworld.Greeter/SayHello')

  route.unary(
    function (call, next) { order.push('h1'); next() },
    function () { order.push('h2-throws'); throw new Error('boom') },
    function (call, next) { order.push('h3-skipped'); next() }
  )

  route.any(errorHandler(function (err, call, next) {
    order.push('error:' + err.message)
    next(err)
  }))

  route.dispatch(fakeCall(), function (err) {
    order.push('done:' + err.message)
    assert.deepEqual(order, ['h1', 'h2-throws', 'error:boom', 'done:boom'])
    done()
  })
})

test('next("route") bails out of the route without treating it as an error', function (t, done) {
  var order = []

  var route = new Route('/helloworld.Greeter/SayHello')

  route.unary(
    function (call, next) { order.push('h1'); next('route') },
    function () { order.push('h2-never-runs') }
  )

  route.dispatch(fakeCall(), function (err) {
    order.push('done:' + err)
    assert.deepEqual(order, ['h1', 'done:undefined'])
    done()
  })
})

test('router.route(path) called twice returns the same Route', function () {
  var Router = require('../lib/router')
  var router = new Router()

  var a = router.route('/helloworld.Greeter/SayHello')
  var b = router.route('/helloworld.Greeter/SayHello')

  assert.equal(a, b)
  assert.equal(router.stack.length, 1)
})

test('a bare arity-3 function is detected as an error handler without grpc.errorHandler()', function (t, done) {
  var order = []

  var route = new Route('/helloworld.Greeter/SayHello')

  route.unary(function () { throw new Error('boom') })
  route.any(function (err, call, next) { order.push('bare-arity-3:' + err.message); next(err) })

  route.dispatch(fakeCall(), function (err) {
    order.push('done:' + err.message)
    assert.deepEqual(order, ['bare-arity-3:boom', 'done:boom'])
    done()
  })
})
