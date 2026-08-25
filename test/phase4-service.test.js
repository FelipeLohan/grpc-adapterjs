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

var grpc = require('../lib/grpc')
var status = require('../lib/status').status

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')

/**
 * Milestone M4: app.service() with a .proto, exercised against a real
 * @grpc/grpc-js client. Unlike the Phase 1-3 suites, handlers here never
 * touch protobufjs directly -- call.request arrives already decoded, and
 * plain objects passed to call.send()/call.write() are encoded
 * automatically. That's the payoff of the whole plan.
 */

function startGreeter (impl, registerApp) {
  var app = grpc()
  var root = grpc.loadSync(PROTO_PATH)
  var svc = root.lookupService('helloworld.Greeter')

  if (registerApp) registerApp(app)

  // most of these tests only implement the one method they exercise --
  // app.service() warns (by design) about the rest, which is noise here.
  var originalWarn = console.warn
  console.warn = function () {}
  try {
    app.service(svc, impl)
  } finally {
    console.warn = originalWarn
  }

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
        client: client,
        close: function close () {
          client.close()
          return new Promise(function (r) { server.close(r) })
        }
      })
    })
  })
}

test('unary: call.request is already a decoded plain object', async function (t) {
  var ctx = await startGreeter({
    SayHello: function (call) {
      assert.equal(typeof call.request, 'object')
      assert.equal(call.request.name, 'Ada')
      call.send({ message: 'Hello ' + call.request.name })
    }
  })

  t.after(ctx.close)

  var reply = await new Promise(function (resolve, reject) {
    ctx.client.SayHello({ name: 'Ada' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })
  })

  assert.equal(reply.message, 'Hello Ada')
})

test('serverStream: writes take plain objects and the client decodes them normally', async function (t) {
  var ctx = await startGreeter({
    LotsOfReplies: async function (call) {
      for (var i = 0; i < 3; i++) {
        await call.writeAsync({ message: call.request.name + '#' + i })
      }

      call.end()
    }
  })

  t.after(ctx.close)

  var messages = await new Promise(function (resolve, reject) {
    var seen = []
    var stream = ctx.client.LotsOfReplies({ name: 'Ada' })

    stream.on('data', function (chunk) { seen.push(chunk.message) })
    stream.on('end', function () { resolve(seen) })
    stream.on('error', reject)
  })

  assert.deepEqual(messages, ['Ada#0', 'Ada#1', 'Ada#2'])
})

test('clientStream: call is an AsyncIterable of decoded objects', async function (t) {
  var ctx = await startGreeter({
    LotsOfGreetings: async function (call) {
      var names = []

      for await (var msg of call) {
        names.push(msg.name)
      }

      call.send({ message: names.join(',') })
    }
  })

  t.after(ctx.close)

  var reply = await new Promise(function (resolve, reject) {
    var stream = ctx.client.LotsOfGreetings(function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })

    stream.write({ name: 'a' })
    stream.write({ name: 'b' })
    stream.end()
  })

  assert.equal(reply.message, 'a,b')
})

test('bidi: request/response objects flow through the codec both ways', async function (t) {
  var ctx = await startGreeter({
    BidiHello: async function (call) {
      for await (var msg of call) {
        call.write({ message: msg.name.toUpperCase() })
      }

      call.end()
    }
  })

  t.after(ctx.close)

  var received = await new Promise(function (resolve, reject) {
    var seen = []
    var stream = ctx.client.BidiHello()

    stream.on('data', function (chunk) { seen.push(chunk.message) })
    stream.on('end', function () { resolve(seen) })
    stream.on('error', reject)

    stream.write({ name: 'ada' })
    stream.write({ name: 'grace' })
    stream.end()
  })

  assert.deepEqual(received, ['ADA', 'GRACE'])
})

test('a method left unimplemented in impl closes with UNIMPLEMENTED', async function (t) {
  var ctx = await startGreeter({ SayHello: function (call) { call.send({ message: 'hi' }) } })

  t.after(ctx.close)

  await assert.rejects(
    new Promise(function (resolve, reject) {
      ctx.client.LotsOfGreetings(function (err, response) {
        if (err) reject(err)
        else resolve(response)
      }).end()
    }),
    function (err) {
      assert.equal(err.code, grpcJs.status.UNIMPLEMENTED)
      return true
    }
  )
})

test('an invalid response object is rejected by verify() as INVALID_ARGUMENT', async function (t) {
  var ctx = await startGreeter({
    SayHello: function (call) {
      call.send({ message: 12345 })
    }
  })

  t.after(ctx.close)

  await assert.rejects(
    new Promise(function (resolve, reject) {
      ctx.client.SayHello({ name: 'Ada' }, function (err, response) {
        if (err) reject(err)
        else resolve(response)
      })
    }),
    function (err) {
      assert.equal(err.code, grpcJs.status.INVALID_ARGUMENT)
      return true
    }
  )
})

test('app.disable(\'strict encode\') lets an app-level setting skip verify()', async function (t) {
  var ctx = await startGreeter(
    { SayHello: function (call) { call.send({ message: 'ok', extra: 'ignored by verify but harmless' }) } },
    function (app) { app.disable('strict encode') }
  )

  t.after(ctx.close)

  var reply = await new Promise(function (resolve, reject) {
    ctx.client.SayHello({ name: 'Ada' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })
  })

  assert.equal(reply.message, 'ok')
})
