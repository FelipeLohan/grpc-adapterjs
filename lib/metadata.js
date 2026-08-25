/*!
 * express-grpc
 * MIT Licensed
 */

'use strict'

/**
 * Keys owned by the protocol. They travel in the same HEADERS block as user
 * metadata but are not exposed as metadata, exactly like `req.headers` in
 * Express exposes transport headers while `req.get('host')` is special-cased.
 */

var RESERVED = new Set([
  'content-type',
  'grpc-timeout',
  'grpc-encoding',
  'grpc-accept-encoding',
  'grpc-message-encoding',
  'grpc-status',
  'grpc-message',
  'grpc-status-details-bin',
  'te'
])

var LEGAL_KEY = /^[0-9a-z_.\-]+$/
var LEGAL_NON_BIN_VALUE = /^[ -~]*$/

/**
 * Case-insensitive multimap of gRPC metadata.
 *
 * Mirrors the read semantics of `req.get()` (`lib/request.js:63-84`): keys are
 * matched case-insensitively. Unlike HTTP headers, keys ending in `-bin` carry
 * binary payloads and are base64 encoded on the wire; `get()` hands those back
 * as `Buffer`s and `toHeaders()` encodes them again.
 *
 * @param {object} [headers] raw http2 headers to seed from
 * @public
 */

class Metadata {
  constructor (headers) {
    this.map = new Map()

    if (headers) this.merge(headers)
  }

  /**
   * Absorb raw http2 headers (or another Metadata / plain object).
   *
   * Pseudo-headers (`:path`, ...) and protocol-reserved keys are skipped.
   *
   * @param {object|Metadata} headers
   * @return {Metadata} for chaining
   * @public
   */

  merge (headers) {
    if (headers == null) return this

    if (headers instanceof Metadata) {
      for (var [k, values] of headers.map) {
        for (var value of values) this.add(k, value)
      }
      return this
    }

    for (var key of Object.keys(headers)) {
      var lower = key.toLowerCase()

      if (lower.charCodeAt(0) === 0x3a /* ':' */) continue
      if (RESERVED.has(lower)) continue

      var raw = headers[key]
      var list = Array.isArray(raw) ? raw : [raw]

      for (var item of list) {
        this.add(lower, lower.endsWith('-bin') && typeof item === 'string'
          ? Buffer.from(item, 'base64')
          : item)
      }
    }

    return this
  }

  /**
   * All values for `key`, in insertion order. Always an array — gRPC metadata
   * is a multimap, so collapsing to a single value would be lossy.
   *
   * @param {string} key
   * @return {Array<string|Buffer>}
   * @public
   */

  get (key) {
    return this.map.get(normalizeKey(key)) || []
  }

  /**
   * First value for `key`, or `undefined`. The `req.get()` shaped accessor.
   *
   * @param {string} key
   * @return {string|Buffer|undefined}
   * @public
   */

  getFirst (key) {
    var values = this.map.get(normalizeKey(key))
    return values === undefined ? undefined : values[0]
  }

  /**
   * @param {string} key
   * @return {boolean}
   * @public
   */

  has (key) {
    return this.map.has(normalizeKey(key))
  }

  /**
   * Replace every value for `key`.
   *
   * @param {string} key
   * @param {string|Buffer|number} value
   * @return {Metadata} for chaining
   * @public
   */

  set (key, value) {
    var k = normalizeKey(key)
    this.map.set(k, [validateValue(k, value)])
    return this
  }

  /**
   * Append a value to `key` without dropping existing ones.
   *
   * @param {string} key
   * @param {string|Buffer|number} value
   * @return {Metadata} for chaining
   * @public
   */

  add (key, value) {
    var k = normalizeKey(key)
    var v = validateValue(k, value)
    var values = this.map.get(k)

    if (values === undefined) this.map.set(k, [v])
    else values.push(v)

    return this
  }

  /**
   * @param {string} key
   * @return {Metadata} for chaining
   * @public
   */

  remove (key) {
    this.map.delete(normalizeKey(key))
    return this
  }

  /**
   * @return {Array<string>} every key present, lowercased
   * @public
   */

  keys () {
    return Array.from(this.map.keys())
  }

  /**
   * @return {number} number of distinct keys
   * @public
   */

  get size () {
    return this.map.size
  }

  /**
   * Plain object of first values — the ergonomic shape for logging.
   *
   * @return {object}
   * @public
   */

  toJSON () {
    var out = Object.create(null)
    for (var [k, values] of this.map) out[k] = values.length === 1 ? values[0] : values.slice()
    return out
  }

  /**
   * Wire form: an http2 headers object, with `-bin` values base64 encoded.
   *
   * @return {object}
   * @public
   */

  toHeaders () {
    var out = Object.create(null)

    for (var [k, values] of this.map) {
      var encoded = k.endsWith('-bin')
        ? values.map(function (v) { return Buffer.isBuffer(v) ? v.toString('base64') : String(v) })
        : values.map(String)

      out[k] = encoded.length === 1 ? encoded[0] : encoded
    }

    return out
  }

  /**
   * @param {object} headers
   * @return {Metadata}
   * @public
   */

  static fromHeaders (headers) {
    return new Metadata(headers)
  }

  clone () {
    return new Metadata(this)
  }

  [Symbol.iterator] () {
    return this.map[Symbol.iterator]()
  }
}

function normalizeKey (key) {
  if (typeof key !== 'string') {
    throw new TypeError('metadata key must be a string')
  }

  var lower = key.toLowerCase()

  if (!LEGAL_KEY.test(lower)) {
    throw new Error('illegal metadata key: ' + JSON.stringify(key))
  }

  if (lower.charCodeAt(0) === 0x3a) {
    throw new Error('metadata key must not be a pseudo-header: ' + key)
  }

  return lower
}

function validateValue (key, value) {
  if (key.endsWith('-bin')) {
    if (Buffer.isBuffer(value)) return value
    if (value instanceof Uint8Array) return Buffer.from(value)
    throw new TypeError('metadata value for "' + key + '" must be a Buffer (-bin keys are binary)')
  }

  if (Buffer.isBuffer(value)) {
    throw new TypeError('binary metadata requires a key ending in "-bin", got "' + key + '"')
  }

  var str = typeof value === 'string' ? value : String(value)

  if (!LEGAL_NON_BIN_VALUE.test(str)) {
    throw new Error('metadata value for "' + key + '" contains non-printable ASCII; use a "-bin" key')
  }

  return str
}

module.exports = Metadata
module.exports.RESERVED = RESERVED
