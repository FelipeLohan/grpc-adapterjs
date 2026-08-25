/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')

var Layer = require('../lib/router/layer')

test('exact layer matches only the literal path', function () {
  var layer = new Layer('/helloworld.Greeter/SayHello', { exact: true }, function () {})

  assert.equal(layer.match('/helloworld.Greeter/SayHello'), true)
  assert.equal(layer.match('/helloworld.Greeter/SayHelloAgain'), false)
  assert.equal(layer.match('/helloworld.Greeter'), false)
})

test('"/" and "*" use-layers match every path', function () {
  var root = new Layer('/', { exact: false }, function () {})
  var star = new Layer('*', { exact: false }, function () {})

  assert.equal(root.match('/anything.At.All/Method'), true)
  assert.equal(star.match('/anything.At.All/Method'), true)
})

test('service-prefix use-layer matches at a slash boundary only', function () {
  var layer = new Layer('/helloworld.Greeter', { exact: false }, function () {})

  assert.equal(layer.match('/helloworld.Greeter/SayHello'), true)
  assert.equal(layer.match('/helloworld.Greeter'), true)
  assert.equal(layer.match('/helloworld.GreeterExtra/M'), false)
  assert.equal(layer.match('/other.Service/M'), false)
})

test('a trailing slash on the use-layer path is normalized away', function () {
  var layer = new Layer('/helloworld.Greeter/', { exact: false }, function () {})

  assert.equal(layer.match('/helloworld.Greeter/SayHello'), true)
  assert.equal(layer.match('/helloworld.Greeter'), true)
})
