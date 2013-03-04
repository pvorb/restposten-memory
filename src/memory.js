'use strict';

/**
 * A database API for plain JavaScript objects that is compatible to
 * [persistence-mongodb](https://github.com/n-fuse/persistence-memory) and thus
 * can be used as a database abstraction layer for
 * [persistence](https://github.com/n-fuse/persistence).
 * 
 * @module persistence-memory
 */

// TODO needed?
var append = require('append');
var filtr = require('filtr');

var stores = {};

/**
 * Connects to the in-memory database.
 * 
 * @param {Object}
 *            [options] ignored (only for compatibility with other
 *            implementations)
 * @param {Function(err,
 *            db)} callback is called when an error occurs or when the database
 *            connection has been established.
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {DB}
 *            callback.db the `DB` instance
 * 
 * @see {@link http://mongodb.github.com/node-mongodb-native/markdown-docs/database.html#server-options}
 *      and
 *      {@link http://mongodb.github.com/node-mongodb-native/markdown-docs/database.html#db-options}
 *      for more information on the possible options
 */
exports.connect = function(options, callback) {
  if (arguments.length == 1) {
    callback = options;
    options = {};
  }

  callback(null, new DB(options));
};

/**
 * @classdesc Dummy wrapper around an array of objects.
 * @constructor
 * 
 * @property {String} protocol Database protocol
 */
function DB(options) {
  var counter = 0;
  options = options || {};

  this.increment = function() {
    return ++counter;
  };

  // create empty document store
  this._store = {};
};

Memory.prototype.protocol = 'memory';

/**
 * Creates/gets a collection.
 * 
 * @param {String}
 *            name collection's name
 * @param {Array}
 *            indexes ignored
 * @param {Function(err,
 *            collection)} callback is called when an error occurs or when the
 *            collection is returned.
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Collection}
 *            callback.collection the `Collection` instance
 */
DB.prototype.getCollection = function(name, indexes, callback) {
  if (arguments.length == 2) {
    callback = indexes;
  }

  if (typeof this._store[name] == 'undefined')
    this._store[name] = {};
  var collection = this._store[name];

  process.nextTick(function() {
    return callback(null, collection);
  });
};

/**
 * @classdesc Dummy wrapper around a collection object.
 * @constructor
 * 
 * @param {Object}
 *            collection collection object
 */
function Collection(coll) {
  this._coll = coll;
}

/**
 * Finds all records that match a given query.
 * 
 * @param {Object|String}
 *            query resulting objects must match this query.
 * @param {String[]}
 *            [fields] specifies the fields of the resulting objects
 * @param {Object}
 *            [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *            res)} callback is called when an error occurs or when the
 *            record(s) return
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.saved the record, if it has been inserted and `1` if the
 *            record has been updated
 */
Collection.prototype.find = function(query, fields, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = fields;
    fields = [];
    options = {};
  } else if (arguments.length == 3) {
    callback = options;
    options = fields;
    fields = [];
  }

  var results = [];

  // match all keys
  if (typeof query._id != 'undefined') {
    if (typeof this._coll[query._id] != 'undefined') {
      var obj = this._coll[query._id];
      var keys = Object.keys(query);

      var match = true;
      for ( var i = 0; i < keys.length; i++) {
        var key = keys[i];
        // FIXME
      }

      results.push(this._coll[query._id]);
    }
  } else {
    // FIXME
  }

  process.nextTick(function() {
    callback(null, results);
  });
};

function matches(query, record) {
  var keys = Object.keys(query);
  var len = keys.length;

  var match = true;
  for ( var i = 0; i < len; i++) {
    var key = keys[i];
    if (record[key] != query[key])
  }

  results.push(this._coll[query._id]);
}

/**
 * Finds the first record that matches a given query. Use this method, if you
 * know that there will be only one resulting document. (E.g. when you want to
 * find a result by its `_id`.)
 * 
 * @param {Object|String}
 *            query resulting objects must match this query. Consult the
 *            [node-mongodb-native
 *            documentation](http://mongodb.github.com/node-mongodb-native/markdown-docs/queries.html#query-object)
 * @param {String|Int|ObjectId}
 *            [query._id] If specified, MongoDB will search by ID
 * @param {String[]}
 *            [fields] specifies the fields of the resulting objects
 * @param {Object}
 *            [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *            res)} callback is called when an error occurs or when the
 *            record(s) return
 * 
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#find}
 */
Collection.prototype.findOne = function(query, fields, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = fields;
    fields = null;
    options = {};
  } else if (arguments.length == 3) {
    callback = options;
    options = fields;
    fields = null;
  }

  // call the query
  if (fields == null)
    this._coll.findOne(query, options, callback);
  else
    this._coll.findOne(query, fields, options, callback);
};

/**
 * Saves a record. If an object with the same `_id` exists already, this will
 * overwrite it completely.
 * 
 * @param {Object}
 *            record record that shall be saved. This parameter can be an
 *            arbitrary _non-circular_ JS object that contains only primitive
 *            values or arrays and objects, no functions.
 * @param {String|Int|ObjectId}
 *            [record._id] ID that is used by MongoDB. If no ID is specified,
 *            the a default MongoDB ID will be generated.
 * @param {Object}
 *            [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *            saved)} callback is called when an error occurs or when the record
 *            has been saved
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.saved the record, if it has been inserted and `1` if the
 *            record has been updated
 * 
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#save}
 */
Collection.prototype.save = function(record, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = options;
    options = {};
  }

  options.safe = true;

  this._coll.save(record, options, callback);
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
 * Gets a record.
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
