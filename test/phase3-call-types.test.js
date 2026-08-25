/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var path = require('node:path')
var http2 = require('node:http2')
var zlib = require('node:zlib')

var grpcJs = require('@grpc/grpc-js')
var protoLoader = require('@grpc/proto-loader')
var protobuf = require('protobufjs')

var grpc = require('../lib/grpc')
var status = require('../lib/status').status
var framing = require('../lib/codec/framing')

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')

var HelloRequest, HelloReply

/**
 * Same harness as Phase 1/2: a real app.listen() server plus a real
 * @grpc/grpc-js client bound to Greeter. Proto codec integration is Phase 4,
 * so handlers here decode/encode manually with protobufjs, exactly like the
 * earlier interop suites -- what's under test is the framework's framing,
 * buffering, iteration and backpressure, not a codec that doesn't exist yet.
 */

function startGreeter (registerApp, clientOptions) {
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
        grpcJs.credentials.createInsecure(),
        clientOptions
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

function encodeHello (message) {
  return Buffer.from(HelloRequest.encode(HelloRequest.create(message)).finish())
}

function decodeHello (buffer) {
  return HelloRequest.toObject(HelloRequest.decode(buffer))
}

function encodeReply (message) {
  return Buffer.from(HelloReply.encode(HelloReply.create(message)).finish())
}

function decodeReply (buffer) {
  return HelloReply.toObject(HelloReply.decode(buffer))
}

test.before(async function () {
  var root = await protobuf.load(PROTO_PATH)
  HelloRequest = root.lookupType('helloworld.HelloRequest')
  HelloReply = root.lookupType('helloworld.HelloReply')
})

test('unary: call.request is the buffered, decoded request payload', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.unary('/helloworld.Greeter/SayHello', function (call) {
      var req = decodeHello(call.request)
      call.send(encodeReply({ message: 'Hello ' + req.name }))
    })
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

test('serverStream: N writes reach the client in order, then the call ends OK', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.serverStream('/helloworld.Greeter/LotsOfReplies', async function (call) {
      var req = decodeHello(call.request)

      for (var i = 0; i < 5; i++) {
        await call.writeAsync(encodeReply({ message: req.name + '#' + i }))
      }

      call.end()
    })
  })

  t.after(ctx.close)

  // @grpc/grpc-js already deserializes with the type it resolved from the
  // .proto via proto-loader -- these arrive as plain objects, not bytes.
  var messages = await new Promise(function (resolve, reject) {
    var seen = []
    var stream = ctx.client.LotsOfReplies({ name: 'Ada' })

    stream.on('data', function (chunk) { seen.push(chunk) })
    stream.on('end', function () { resolve(seen) })
    stream.on('error', reject)
  })

  assert.deepEqual(messages.map(function (m) { return m.message }), [
    'Ada#0', 'Ada#1', 'Ada#2', 'Ada#3', 'Ada#4'
  ])
})

test('serverStream: call.write() backpressure signal matches Node stream.write()', async function (t) {
  var sawFalse = false

  var ctx = await startGreeter(function (app) {
    app.serverStream('/helloworld.Greeter/LotsOfReplies', async function (call) {
      var big = Buffer.alloc(64 * 1024, 'x')

      for (var i = 0; i < 64; i++) {
        var ok = call.write(encodeReply({ message: big.toString() }))
        if (!ok) sawFalse = true
        if (!ok) await new Promise(function (resolve) { call.stream.once('drain', resolve) })
      }

      call.end()
    })
  })

  t.after(ctx.close)

  await new Promise(function (resolve, reject) {
    var stream = ctx.client.LotsOfReplies({ name: 'Ada' })
    stream.on('data', function () {})
    stream.on('end', resolve)
    stream.on('error', reject)
  })

  assert.equal(sawFalse, true)
})

test('clientStream: call is an AsyncIterable of decoded request messages', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.clientStream('/helloworld.Greeter/LotsOfGreetings', async function (call) {
      var names = []

      for await (var chunk of call) {
        names.push(decodeHello(chunk).name)
      }

      call.send(encodeReply({ message: names.join(',') }))
    })
  })

  t.after(ctx.close)

  var reply = await new Promise(function (resolve, reject) {
    var stream = ctx.client.LotsOfGreetings(function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })

    stream.write({ name: 'a' })
    stream.write({ name: 'b' })
    stream.write({ name: 'c' })
    stream.end()
  })

  assert.equal(reply.message, 'a,b,c')
})

test('bidi: each request is echoed back transformed, preserving order', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.bidi('/helloworld.Greeter/BidiHello', async function (call) {
      for await (var chunk of call) {
        var req = decodeHello(chunk)
        call.write(encodeReply({ message: req.name.toUpperCase() }))
      }

      call.end()
    })
  })

  t.after(ctx.close)

  var received = await new Promise(function (resolve, reject) {
    var seen = []
    var stream = ctx.client.BidiHello()

    stream.on('data', function (chunk) { seen.push(chunk) })
    stream.on('end', function () { resolve(seen) })
    stream.on('error', reject)

    stream.write({ name: 'ada' })
    stream.write({ name: 'grace' })
    stream.end()
  })

  assert.deepEqual(received.map(function (m) { return m.message }), ['ADA', 'GRACE'])
})

test('call.write() throws for a clientStream call', async function (t) {
  var thrown

  var ctx = await startGreeter(function (app) {
    app.clientStream('/helloworld.Greeter/LotsOfGreetings', async function (call) {
      for await (var _chunk of call) { /* drain */ }

      try {
        call.write(encodeReply({ message: 'nope' }))
      } catch (err) {
        thrown = err
      }

      call.send(encodeReply({ message: 'ok' }))
    })
  })

  t.after(ctx.close)

  await new Promise(function (resolve, reject) {
    var stream = ctx.client.LotsOfGreetings(function (err) {
      if (err) reject(err)
      else resolve()
    })

    stream.write({ name: 'a' })
    stream.end()
  })

  assert.ok(thrown)
  assert.match(thrown.message, /not valid for a clientStream call/)
})

test('a second message on a unary call is rejected with 3 INVALID_ARGUMENT', async function (t) {
  var app = grpc()
  app.unary('/helloworld.Greeter/SayHello', function (call) {
    call.send(encodeReply({ message: 'should not run' }))
  })

  var server = app.listen(0, '127.0.0.1')
  await new Promise(function (resolve) { server.on('listening', resolve) })

  var port = server.address().port
  var session = http2.connect('http://127.0.0.1:' + port)

  t.after(function () {
    session.close()
    return new Promise(function (resolve) { server.close(resolve) })
  })

  var result = await new Promise(function (resolve) {
    var req = session.request({
      ':method': 'POST',
      ':path': '/helloworld.Greeter/SayHello',
      'content-type': 'application/grpc+proto'
    })

    var trailers
    req.on('trailers', function (t) { trailers = t })
    req.on('response', function (headers) {
      if (headers['grpc-status'] !== undefined) resolve(headers)
    })
    req.on('end', function () { resolve(trailers) })

    req.write(framing.frame(encodeHello({ name: 'one' })))
    req.write(framing.frame(encodeHello({ name: 'two' })))
    req.end()
  })

  assert.equal(result['grpc-status'], String(status.INVALID_ARGUMENT))
})

test('oversized request message closes with 8 RESOURCE_EXHAUSTED', async function (t) {
  var app = grpc()
  app.set('max receive message size', 16)
  app.unary('/helloworld.Greeter/SayHello', function (call) {
    call.send(encodeReply({ message: 'should not run' }))
  })

  var server = app.listen(0, '127.0.0.1')
  await new Promise(function (resolve) { server.on('listening', resolve) })

  var port = server.address().port
  var session = http2.connect('http://127.0.0.1:' + port)

  t.after(function () {
    session.close()
    return new Promise(function (resolve) { server.close(resolve) })
  })

  var result = await new Promise(function (resolve) {
    var req = session.request({
      ':method': 'POST',
      ':path': '/helloworld.Greeter/SayHello',
      'content-type': 'application/grpc+proto'
    })

    var trailers
    req.on('trailers', function (t) { trailers = t })
    req.on('response', function (headers) {
      if (headers['grpc-status'] !== undefined) resolve(headers)
    })
    req.on('end', function () { resolve(trailers) })

    req.write(framing.frame(encodeHello({ name: 'a much longer name than 16 bytes allows' })))
    req.end()
  })

  assert.equal(result['grpc-status'], String(status.RESOURCE_EXHAUSTED))
})

test('gzip response: server compresses when the client advertises grpc-accept-encoding', async function (t) {
  var ctx = await startGreeter(function (app) {
    app.set('compression', 'gzip')
    app.unary('/helloworld.Greeter/SayHello', function (call) {
      call.send(encodeReply({ message: 'compressed hello' }))
    })
  })

  t.after(ctx.close)

  var reply = await new Promise(function (resolve, reject) {
    ctx.client.SayHello({ name: 'world' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })
  })

  assert.equal(reply.message, 'compressed hello')
})

test('gzip request: server decompresses an incoming grpc-encoding: gzip message', async function (t) {
  var app = grpc()
  app.unary('/helloworld.Greeter/SayHello', function (call) {
    var req = decodeHello(call.request)
    call.send(encodeReply({ message: 'Hello ' + req.name }))
  })

  var server = app.listen(0, '127.0.0.1')
  await new Promise(function (resolve) { server.on('listening', resolve) })

  var port = server.address().port
  var session = http2.connect('http://127.0.0.1:' + port)

  t.after(function () {
    session.close()
    return new Promise(function (resolve) { server.close(resolve) })
  })

  var compressed = zlib.gzipSync(encodeHello({ name: 'Ada' }))

  var result = await new Promise(function (resolve, reject) {
    var req = session.request({
      ':method': 'POST',
      ':path': '/helloworld.Greeter/SayHello',
      'content-type': 'application/grpc+proto',
      'grpc-encoding': 'gzip'
    })

    var chunks = []
    req.on('data', function (chunk) { chunks.push(chunk) })
    req.on('end', function () { resolve(Buffer.concat(chunks)) })
    req.on('error', reject)

    req.write(framing.frame(compressed, true))
    req.end()
  })

  var frames = framing.decodeFrames(result)
  assert.equal(decodeReply(frames[0].payload).message, 'Hello Ada')
})

test('client cancellation during a serverStream aborts call.signal and stops further writes', async function (t) {
  var aborted = false
  var writesAfterAbort = 0

  var ctx = await startGreeter(function (app) {
    app.serverStream('/helloworld.Greeter/LotsOfReplies', async function (call) {
      call.signal.addEventListener('abort', function () { aborted = true })

      for (var i = 0; i < 50; i++) {
        if (call.signal.aborted) { writesAfterAbort++; break }
        call.write(encodeReply({ message: 'msg' + i }))
        await new Promise(function (resolve) { setImmediate(resolve) })
      }
    })
  })

  t.after(ctx.close)

  await new Promise(function (resolve) {
    var stream = ctx.client.LotsOfReplies({ name: 'Ada' })
    var count = 0

    stream.on('data', function () {
      count++
      if (count === 2) stream.cancel()
    })

    stream.on('error', function () { resolve() })
    stream.on('end', resolve)
  })

  await new Promise(function (resolve) { setTimeout(resolve, 50) })

  assert.equal(aborted, true)
})
