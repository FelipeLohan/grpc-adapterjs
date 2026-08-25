/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var test = require('node:test')
var assert = require('node:assert/strict')
var path = require('node:path')
var fs = require('node:fs')
var os = require('node:os')
var execFileSync = require('node:child_process').execFileSync

var PROTO_PATH = path.join(__dirname, 'fixtures', 'helloworld.proto')
var PBJS = path.join(__dirname, '..', 'node_modules', '.bin', 'pbjs')
var PBTS = path.join(__dirname, '..', 'node_modules', '.bin', 'pbts')

/**
 * Plan §7.5's last Phase 4 deliverable: `.d.ts` generation from a `.proto`
 * via protobufjs's own `pbjs`/`pbts` CLI (the `protobufjs-cli` package),
 * so `call.request` gets real autocomplete in an editor -- "Express, but
 * type-safe". This isn't framework code; it's confirming the existing
 * tooling actually produces useful output for a service registered through
 * app.service(), end to end.
 */

test('pbjs + pbts generate a .d.ts with interfaces for every message and service', function () {
  var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'express-grpc-dts-'))
  var jsOut = path.join(dir, 'helloworld.pb.js')
  var dtsOut = path.join(dir, 'helloworld.pb.d.ts')

  try {
    execFileSync(PBJS, ['-t', 'static-module', '-w', 'commonjs', '-o', jsOut, PROTO_PATH])
    execFileSync(PBTS, ['-o', dtsOut, jsOut])

    var dts = fs.readFileSync(dtsOut, 'utf8')

    assert.match(dts, /interface IHelloRequest/)
    assert.match(dts, /interface IHelloReply/)
    assert.match(dts, /class Greeter extends \$protobuf\.rpc\.Service/)
    assert.match(dts, /lotsOfReplies: helloworld\.Greeter\.LotsOfReplies/)
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
})
