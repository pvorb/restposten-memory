var append = require('append');
var Cache = require('persistence-cache').Cache;

exports.stores = {};
exports.caches = {};

/**
 * Connects to memory db.
 * 
 * This is only needed for similarity to other db layers.
 * 
 * @param options
 *            [optional]
 * @param callback
 *            (err, db)
 */
exports.connect = function(options, callback) {
  if (typeof options == 'function') {
    callback = options;
    options = {};
  }

  callback(null, new Memory(options));
};

/**
 * Constructor
 */
var Memory = exports.Memory = function Memory(options) {
  var counter = 0;
  options = options || {};
  this.uri = options.uri;

  this.increment = function() {
    return ++counter;
  };

  if (typeof this.uri == 'string') { // Application-wide store
    if (!exports.stores[this.uri]) {
      this.store = exports.stores[this.uri] = {};
      this.cache = exports.caches[this.uri] = new Cache();
    } else {
      // Use store that was created before
      this.store = exports.stores[this.uri];
      this.cache = exports.caches[this.uri];
    }
  } else { // Connection-wide store
    this.store = {};
    this.cache = new Cache();
  }
};

Memory.prototype.protocol = 'memory';

/**
 * Loads some data into this store.
 */
Memory.prototype.load = function(data) {
  if (data instanceof Array) {
    var tmp = {};
    data.forEach(function(e) {
      tmp[e.id] = JSON.stringify(e);
    });
    data = tmp;
  }

  this.store = data;

  // Update cache
  if (this.uri) {
    exports.stores[this.uri] = JSON.parse(JSON.stringify(this.store));
  }

  return this;
};

/**
 * Request a function.
 */
Memory.prototype.request = function(fn) {
  var self = this;

  process.nextTick(function() {
    fn.call(self);
  });
};

/**
 * Save the record at the given key.
 * 
 * @see Memory.prototype.put
 */
Memory.prototype.save = function(key, val, callback) {
  var args = Array.prototype.slice.call(arguments);
  var callback = args.pop(), val = args.pop();
  if (!args.length || !key) {
    key = this.increment();
    val.id = key;
  }

  // Forces key to be a string
  key += '';
  val.id += '';

  this.request(function() {
    this.store[key] = JSON.stringify(val);
    callback(null, val);
  });
};

/**
 * Save the record at the given key.
 * 
 * @see Memory.prototype.save
 */
Memory.prototype.put = function() {
  this.save.apply(this, arguments);
};

/**
 * Updates a record.
 * 
 * TODO Should behave like a save, but a file must.
 */
Memory.prototype.update = function(key, obj, callback) {
  var current = JSON.parse(this.store[key] || "{}");
  this.put(key, append(current, obj), callback);
};

/**
 * 
 */
Memory.prototype.get = function(key, callback) {
  this.request(function() {
    key = key.toString();
    return (key in this.store) ? callback(null, JSON.parse(this.store[key]
        || "null")) : callback({
      status : 404
    });
  });
};

Memory.prototype.destroy = function(key, callback) {
  this.request(function() {
    delete this.store[key];
    return callback(null, {
      status : 204
    });
  });
};

Memory.prototype.find = function(conditions, callback) {
  this.filter(function(obj) {
    return Object.keys(conditions).every(function(k) {
      return conditions[k] === obj[k];
    });
  }, callback);
};

Memory.prototype.filter = function(filter, callback) {
  this.request(function() {
    var result = [], store = this.store;

    Object.keys(this.store).forEach(function(k) {
      var obj = JSON.parse(store[k]);
      if (filter(obj)) {
        obj.id = obj.id.split('/').slice(1).join('/');
        result.push(obj);
      }
    });

    callback(null, result);
  });
};

Memory.prototype.sync = function(factory, callback) {
  process.nextTick(function() {
    callback();
  });
};
