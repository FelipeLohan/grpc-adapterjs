/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var path = require('node:path')

var grpcJs = require('@grpc/grpc-js')
var protoLoader = require('@grpc/proto-loader')
var protobuf = require('protobufjs')

var grpc = require('../lib/grpc')
var status = require('../lib/status').status
var GrpcError = require('../lib/status').GrpcError

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')

/**
 * Boot an app, listen on an ephemeral port, and hand back a @grpc/grpc-js
 * client bound to the Greeter service plus a teardown function.
 *
 * Using the official client here (rather than a hand-rolled http2 client)
 * is the interop check called out by the plan: if a real gRPC stack can't
 * talk to this server, framing/trailers/status are wrong.
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
        client: client,
        close: function close () {
          client.close()
          return new Promise(function (r) { server.close(r) })
        }
      })
    })
  })
}

async function loadReplyType () {
  var root = await protobuf.load(PROTO_PATH)
  return root.lookupType('helloworld.HelloReply')
}

test('unary call: hardcoded response with grpc-status OK', async function (t) {
  var HelloReply = await loadReplyType()
  var payload = HelloReply.encode(HelloReply.create({ message: 'Hello world' })).finish()

  var ctx = await startGreeter(function (app) {
    app.use(function (call) {
      call.send(Buffer.from(payload))
    })
  })

  t.after(ctx.close)

  var reply = await new Promise(function (resolve, reject) {
    ctx.client.SayHello({ name: 'world' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })
  })

  assert.equal(reply.message, 'Hello world')
})

test('call.status()/call.trailer() surface on the client as metadata + code', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.use(function (call) {
      call.set('x-served-by', 'phase1-test')
      call.trailer('x-request-cost', '3')
      call.status(status.OK)
      call.send(Buffer.from(''))
    })
  })

  t.after(ctx.close)

  var trailingMetadata
  var reply = await new Promise(function (resolve, reject) {
    var call = ctx.client.SayHello({ name: 'world' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })

    call.on('status', function (s) { trailingMetadata = s.metadata })
  })

  assert.deepEqual(reply, {})
  assert.equal(trailingMetadata.get('x-request-cost')[0], '3')
})

test('no handler matches -> 12 UNIMPLEMENTED', async function (t) {
  var ctx = await startGreeter(function () {})

  t.after(ctx.close)

  await assert.rejects(
    new Promise(function (resolve, reject) {
      ctx.client.SayHello({ name: 'world' }, function (err, response) {
        if (err) reject(err)
        else resolve(response)
      })
    }),
    function (err) {
      assert.equal(err.code, grpcJs.status.UNIMPLEMENTED)
      return true
    }
  )
})

test('thrown GrpcError inside a handler closes with that status', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.use(function () {
      throw new GrpcError(status.INVALID_ARGUMENT, 'name is required')
    })
  })

  t.after(ctx.close)

  await assert.rejects(
    new Promise(function (resolve, reject) {
      ctx.client.SayHello({ name: '' }, function (err, response) {
        if (err) reject(err)
        else resolve(response)
      })
    }),
    function (err) {
      assert.equal(err.code, grpcJs.status.INVALID_ARGUMENT)
      assert.equal(err.details, 'name is required')
      return true
    }
  )
})

test('Trailers-Only: non-POST method never reaches a call', async function (t) {
  var http2 = require('node:http2')

  var app = grpc()
  app.use(function (call) { call.send(Buffer.from('')) })

  var server = app.listen(0, '127.0.0.1')

  await new Promise(function (resolve) { server.on('listening', resolve) })

  var port = server.address().port
  var session = http2.connect('http://127.0.0.1:' + port)

  var result = await new Promise(function (resolve) {
    var req = session.request({ ':method': 'GET', ':path': '/helloworld.Greeter/SayHello' })

    req.on('response', function (headers) {
      resolve(headers)
    })

    req.end()
  })

  assert.equal(result['grpc-status'], String(status.UNIMPLEMENTED))
  assert.equal(result[':status'], 200)

  session.close()
  await new Promise(function (resolve) { server.close(resolve) })
})

test('deadline: grpc-timeout aborts the call signal', async function (t) {
  var deadlineExceeded = false

  var ctx = await startGreeter(function (app) {
    app.use(function (call) {
      call.signal.addEventListener('abort', function () {
        deadlineExceeded = true
      })
      // never respond — force the deadline to fire
    })
  })

  t.after(ctx.close)

  await assert.rejects(
    new Promise(function (resolve, reject) {
      var deadline = new Date(Date.now() + 50)

      ctx.client.SayHello({ name: 'world' }, { deadline: deadline }, function (err, response) {
        if (err) reject(err)
        else resolve(response)
      })
    }),
    function (err) {
      assert.equal(err.code, grpcJs.status.DEADLINE_EXCEEDED)
      return true
    }
  )

  await new Promise(function (resolve) { setTimeout(resolve, 100) })
  assert.equal(deadlineExceeded, true)
})

test('unhandled non-GrpcError closes with 13 INTERNAL, message suppressed in production', async function (t) {
  var originalEnv = process.env.NODE_ENV
  process.env.NODE_ENV = 'production'

  var ctx = await startGreeter(function (app) {
    app.set('env', 'production')
    app.use(function () {
      throw new Error('a secret stack trace detail')
    })
  })

  t.after(function () {
    process.env.NODE_ENV = originalEnv
    return ctx.close()
  })

  await assert.rejects(
    new Promise(function (resolve, reject) {
      ctx.client.SayHello({ name: 'world' }, function (err, response) {
        if (err) reject(err)
        else resolve(response)
      })
    }),
    function (err) {
      assert.equal(err.code, grpcJs.status.INTERNAL)
      assert.equal(err.details, 'Internal server error')
      return true
    }
  )
})

test('client cancellation aborts call.signal on the server', async function (t) {
  var aborted = false

  var ctx = await startGreeter(function (app) {
    app.use(function (call) {
      call.signal.addEventListener('abort', function () {
        aborted = true
      })
      // never respond -- the client will cancel before we do
    })
  })

  t.after(ctx.close)

  var pending = new Promise(function (resolve) {
    var call = ctx.client.SayHello({ name: 'world' }, function (err) {
      resolve(err)
    })

    setTimeout(function () { call.cancel() }, 20)
  })

  var err = await pending

  assert.ok(err)
  assert.equal(err.code, grpcJs.status.CANCELLED)

  await new Promise(function (resolve) { setTimeout(resolve, 20) })
  assert.equal(aborted, true)
})
