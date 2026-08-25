/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var fs = require('node:fs')
var path = require('node:path')

var grpcJs = require('@grpc/grpc-js')
var protoLoader = require('@grpc/proto-loader')

var grpc = require('../lib/grpc')

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')
var CERT_PATH = path.join(__dirname, 'fixtures', 'tls-cert.pem')
var KEY_PATH = path.join(__dirname, 'fixtures', 'tls-key.pem')

test('app.listen() over TLS answers a real gRPC client', async function (t) {
  var app = grpc()

  app.set('tls', {
    cert: fs.readFileSync(CERT_PATH),
    key: fs.readFileSync(KEY_PATH)
  })

  app.use(function (call) {
    call.send(Buffer.from(''))
  })

  var server = app.listen(0, '127.0.0.1')

  await new Promise(function (resolve) { server.on('listening', resolve) })

  var port = server.address().port

  var packageDef = protoLoader.loadSync(PROTO_PATH, { keepCase: false })
  var descriptor = grpcJs.loadPackageDefinition(packageDef)
  var credentials = grpcJs.credentials.createSsl(fs.readFileSync(CERT_PATH))
  var client = new descriptor.helloworld.Greeter(
    '127.0.0.1:' + port,
    credentials,
    { 'grpc.ssl_target_name_override': 'localhost', 'grpc.default_authority': 'localhost' }
  )

  t.after(function () {
    client.close()
    return new Promise(function (resolve) { server.close(resolve) })
  })

  var reply = await new Promise(function (resolve, reject) {
    client.SayHello({ name: 'world' }, function (err, response) {
      if (err) reject(err)
      else resolve(response)
    })
  })

  assert.deepEqual(reply, {})
})
