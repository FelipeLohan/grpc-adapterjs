/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var path = require('node:path')
var http2 = require('node:http2')

var grpcJs = require('@grpc/grpc-js')
var protoLoader = require('@grpc/proto-loader')

var grpc = require('../lib/grpc')
var status = require('../lib/status').status
var GrpcError = require('../lib/status').GrpcError

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')

/**
 * Same harness as the Phase 1 suite: boot a real app.listen() server and
 * hand back a @grpc/grpc-js client bound to Greeter, so routing is verified
 * against an actual gRPC stack rather than by calling app.handle() by hand.
 */

function startGreeter (registerApp) {
  var app = grpc()

  registerApp(app)

  var server = app.listen(0, '127.0.0.1')

  return new Promise(function (resolve) {
    server.on('listening', function () {
      var port = server.address().port

      var packageDef = protoLoader.loadSync(PROTO_PATH, { keepCase: false })
      var descriptor = grpcJs.loadPackageDefinition(packageDef)
      var client = new descriptor.helloworld.Greeter(
        '127.0.0.1:' + port,
        grpcJs.credentials.createInsecure()
      )

      resolve({
        app: app,
        server: server,
        client: client,
        close: function close () {
          client.close()
          return new Promise(function (r) { server.close(r) })
        }
      })
    })
  })
}

function unaryCall (client, req) {
  return new Promise(function (resolve, reject) {
    client.SayHello(req || { name: 'world' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })
  })
}

test('ordering: global interceptor -> service interceptor -> route', async function (t) {
  var order = []

  var ctx = await startGreeter(function (app) {
    app.use(function (call, next) { order.push('global'); next() })
    app.use('/helloworld.Greeter', function (call, next) { order.push('service'); next() })
    app.unary('/helloworld.Greeter/SayHello', function (call) {
      order.push('route')
      call.send(Buffer.from(''))
    })
  })

  t.after(ctx.close)

  await unaryCall(ctx.client)

  assert.deepEqual(order, ['global', 'service', 'route'])
})

test('a service-scoped interceptor does not leak to a different service path', async function (t) {
  var order = []

  var ctx = await startGreeter(function (app) {
    app.use('/some.OtherService', function (call, next) { order.push('other-service'); next() })
    app.unary('/helloworld.Greeter/SayHello', function (call) {
      order.push('route')
      call.send(Buffer.from(''))
    })
  })

  t.after(ctx.close)

  await unaryCall(ctx.client)

  assert.deepEqual(order, ['route'])
})

test('grpc.errorHandler() catches an error thrown by a route handler', async function (t) {
  var caught

  var ctx = await startGreeter(function (app) {
    app.unary('/helloworld.Greeter/SayHello', function () {
      throw new GrpcError(status.FAILED_PRECONDITION, 'nope')
    })

    app.use(grpc.errorHandler(function (err, call, next) {
      caught = err.message
      next(err)
    }))
  })

  t.after(ctx.close)

  await assert.rejects(unaryCall(ctx.client), function (err) {
    assert.equal(err.code, grpcJs.status.FAILED_PRECONDITION)
    return true
  })

  assert.equal(caught, 'nope')
})

test('sub-app mounting: app.use(prefix, subApp) delegates and restores call.app', async function (t) {
  var sub = grpc()
  sub.set('name', 'sub-app')

  sub.unary('/helloworld.Greeter/SayHello', function (call) {
    assert.equal(call.app.get('name'), 'sub-app')
    call.send(Buffer.from(''))
  })

  var ctx = await startGreeter(function (app) {
    app.set('name', 'parent-app')
    app.use('/helloworld.Greeter', sub)
  })

  t.after(ctx.close)

  var reply = await unaryCall(ctx.client)
  assert.deepEqual(reply, {})
})

test('sub-app mounting emits "mount" with the parent app', async function (t) {
  var mountedWith

  var sub = grpc()
  sub.unary('/helloworld.Greeter/SayHello', function (call) { call.send(Buffer.from('')) })
  sub.on('mount', function (parent) { mountedWith = parent })

  var ctx = await startGreeter(function (app) {
    app.use('/helloworld.Greeter', sub)
  })

  t.after(ctx.close)

  await unaryCall(ctx.client)

  assert.equal(mountedWith, ctx.app)
  assert.equal(sub.mountpath, '/helloworld.Greeter')
})

test('malformed gRPC path -> 12 UNIMPLEMENTED via Trailers-Only', async function (t) {
  var app = grpc()
  app.use(function (call) { call.send(Buffer.from('')) })

  var server = app.listen(0, '127.0.0.1')
  await new Promise(function (resolve) { server.on('listening', resolve) })

  var port = server.address().port
  var session = http2.connect('http://127.0.0.1:' + port)

  var result = await new Promise(function (resolve) {
    var req = session.request({
      ':method': 'POST',
      ':path': '/not-a-grpc-path',
      'content-type': 'application/grpc+proto'
    })

    req.on('response', function (headers) { resolve(headers) })
    req.end()
  })

  assert.equal(result['grpc-status'], String(status.UNIMPLEMENTED))
  assert.equal(result[':status'], 200)

  session.close()
  await new Promise(function (resolve) { server.close(resolve) })
})

test('app.route(path).unary(fn) is equivalent to app.unary(path, fn)', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.route('/helloworld.Greeter/SayHello').unary(function (call) {
      call.send(Buffer.from(''))
    })
  })

  t.after(ctx.close)

  var reply = await unaryCall(ctx.client)
  assert.deepEqual(reply, {})
})

test('registering two different call types on the same path throws at registration time', function () {
  var app = grpc()

  app.unary('/helloworld.Greeter/SayHello', function () {})

  assert.throws(function () {
    app.serverStream('/helloworld.Greeter/SayHello', function () {})
  }, /already registered as "unary"/)
})
