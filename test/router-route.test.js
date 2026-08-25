/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')

var Route = require('../lib/router/route')
var errorHandler = require('../lib/router/error-handler').errorHandler

test('a route serves exactly one call type; a second, different type throws', function () {
  var route = new Route('/helloworld.Greeter/SayHello')

  route.unary(function () {})

  assert.throws(function () {
    route.serverStream(function () {})
  }, /already registered as "unary"/)
})

test('.any() runs regardless of the route\'s registered type', function () {
  var order = []

  var route = new Route('/helloworld.Greeter/SayHello')

  route.any(function (call, next) { order.push('any'); next() })
  route.unary(function (call) { order.push('unary'); call.type = route.type })

  route.dispatch({}, function () {})

  assert.deepEqual(order, ['any', 'unary'])
})

test('dispatch() sets call.type before running any handler', function () {
  var seen

  var route = new Route('/helloworld.Greeter/LotsOfReplies')
  route.serverStream(function (call) { seen = call.type })

  route.dispatch({}, function () {})

  assert.equal(seen, 'serverStream')
})

test('a thrown error skips remaining normal handlers and reaches an error handler', function () {
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

  route.dispatch({}, function (err) {
    order.push('done:' + err.message)
  })

  assert.deepEqual(order, ['h1', 'h2-throws', 'error:boom', 'done:boom'])
})

test('next("route") bails out of the route without treating it as an error', function () {
  var order = []

  var route = new Route('/helloworld.Greeter/SayHello')

  route.unary(
    function (call, next) { order.push('h1'); next('route') },
    function () { order.push('h2-never-runs') }
  )

  route.dispatch({}, function (err) {
    order.push('done:' + err)
  })

  assert.deepEqual(order, ['h1', 'done:undefined'])
})

test('router.route(path) called twice returns the same Route', function () {
  var Router = require('../lib/router')
  var router = new Router()

  var a = router.route('/helloworld.Greeter/SayHello')
  var b = router.route('/helloworld.Greeter/SayHello')

  assert.equal(a, b)
  assert.equal(router.stack.length, 1)
})
