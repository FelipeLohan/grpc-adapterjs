/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

var protobuf = require('protobufjs')
var codecs = require('./codec/protobuf')
var status = require('./status').status

/**
 * `app.service(serviceOrPath, impl)` (plan §7.1): register every rpc of a
 * `.proto` service in one call, deriving the three things a user would
 * otherwise register by hand -- the path, the codec, and the call type
 * (plan §5.3) -- from the service's own reflection instead.
 *
 * @param {protobuf.Service|string} serviceOrPath a resolved Service, or a
 *   path to a `.proto` file containing exactly one service
 * @param {object} [impl] `{ [methodName]: handler }`
 * @return {app} for chaining
 * @public
 */

function service (serviceOrPath, impl) {
  var svc = resolveService(serviceOrPath)
  var methods = svc.methodsArray
  var handlers = impl || {}

  var validNames = new Set(methods.map(function (m) { return m.name }))

  Object.keys(handlers).forEach(function (name) {
    if (!validNames.has(name)) {
      throw new Error(
        'app.service(): "' + name + '" is not a method of ' + serviceName(svc) + ' (check for a typo)'
      )
    }
  })

  var self = this

  methods.forEach(function (method) {
    var path = '/' + serviceName(svc) + '/' + method.name
    var callType = callTypeFor(method)
    var route = self.route(path)

    route.codec(
      codecs.createCodec(method.resolvedRequestType),
      codecs.createCodec(method.resolvedResponseType)
    )

    var handler = handlers[method.name]

    if (typeof handler === 'function') {
      route[callType](handler)
    } else {
      console.warn(
        '[express-grpc] ' + path + ' is declared in the .proto but not implemented; ' +
        'registering a stub that returns UNIMPLEMENTED'
      )

      route[callType](function unimplementedStub (call) {
        call.fail(status.UNIMPLEMENTED, path + ' is not implemented')
      })
    }
  })

  return this
}

/**
 * @param {protobuf.Service} svc
 * @return {string} fully-qualified name without protobufjs's leading dot
 * @private
 */

function serviceName (svc) {
  return svc.fullName.replace(/^\./, '')
}

/**
 * @param {protobuf.Method} method
 * @return {'unary'|'serverStream'|'clientStream'|'bidi'}
 * @private
 */

function callTypeFor (method) {
  if (method.requestStream && method.responseStream) return 'bidi'
  if (method.requestStream) return 'clientStream'
  if (method.responseStream) return 'serverStream'
  return 'unary'
}

/**
 * Accept either an already-resolved `protobuf.Service`, or a path to a
 * `.proto` file that contains exactly one service (loaded and resolved on
 * the spot). Anything more exotic -- multiple services in one file, a
 * service nested deeper than convenient -- means the caller should load the
 * root themselves and pass `root.lookupService('pkg.Service')`.
 *
 * @param {protobuf.Service|string} serviceOrPath
 * @return {protobuf.Service}
 * @private
 */

function resolveService (serviceOrPath) {
  if (serviceOrPath instanceof protobuf.Service) return serviceOrPath

  if (typeof serviceOrPath === 'string') {
    var root = codecs.loadSync(serviceOrPath)
    var found = collectServices(root)

    if (found.length === 0) {
      throw new Error('app.service(): no service found in ' + serviceOrPath)
    }

    if (found.length > 1) {
      throw new Error(
        'app.service(): ' + serviceOrPath + ' declares multiple services (' +
        found.map(serviceName).join(', ') + '); pass root.lookupService(\'pkg.Service\') explicitly'
      )
    }

    return found[0]
  }

  throw new TypeError('app.service() requires a protobufjs Service or a path to a .proto file')
}

/**
 * @param {protobuf.NamespaceBase} ns
 * @return {protobuf.Service[]}
 * @private
 */

function collectServices (ns) {
  var found = []

  if (!ns.nestedArray) return found

  ns.nestedArray.forEach(function (child) {
    if (child instanceof protobuf.Service) found.push(child)
    else if (child.nestedArray) found = found.concat(collectServices(child))
  })

  return found
}

module.exports = service
