'use strict';

/**
 * A database API for plain JavaScript objects that is compatible to
 * [persistence-mongodb](https://github.com/n-fuse/persistence-memory) and thus
 * can be used as a database abstraction layer for
 * [persistence](https://github.com/n-fuse/persistence).
 * 
 * @module persistence-memory
 */

var filtr = require('filtr');
var uuid = require('node-uuid');

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

DB.prototype.protocol = 'memory';

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
    callback(null, new Collection(collection));
  });
};

/**
 * Closes the connection to the database.
 * 
 * @param {Boolean}
 *            [force] force to close the connect so that it cannot be reused.
 * @param {Function(err,
 *            results)} callback is called when the db has been closed or an
 *            error occurred
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object}
 *            callback.results
 */
DB.prototype.close = function(force, callback) {
  process.nextTick(function () {
    callback(null);
  });
}

/**
 * @classdesc Dummy wrapper around an object.
 * @constructor
 * 
 * @param {Object}
 *            collection collection object
 */
function Collection(coll) {
  this._coll = coll;
}

/**
 * Check if the record matches the query.
 * 
 * @param {Object}
 *            query query object
 * @param {Object}
 *            record record
 * 
 * @returns {Boolean}
 */
function matches(query, record) {
  var keys = Object.keys(query);
  var len = keys.length;

  var matches = true;
  for ( var i = 0; i < len; i++) {
    var key = keys[i];
    if (record[key] !== query[key]) {
      matches = false;
      break;
    }
  }

  return matches;
}

/**
 * Filters fields of an object.
 * 
 * @param {Object}
 *            record
 * @param {String[]}
 *            fields String array that specifies all requested keys
 * @returns {Object} filtered object
 */
function filterFields(record, fields) {
  // walk through the array and only inherit the properties in fields
  var len = fields.length;
  if (len > 0) {
    var resultRec = {};
    for ( var i = 0; i < len; i++) {
      var key = fields[j];
      resultRec[key] = record[key];
    }
    return resultRec;
  } else {
    // if fields is empty, return the record as is.
    return record;
  }
}

/**
 * Finds all records that match a given query.
 * 
 * @param {Object|String}
 *            query resulting objects must match this query.
 * @param {String[]}
 *            [fields] specifies the fields of the resulting objects
 * @param {Object}
 *            [options] ignored
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
  // TODO handle options

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
  var Q = filtr(query);

  // find records
  if (typeof query._id != 'undefined') {
    // check if the record with query._id matches the query
    if (typeof this._coll[query._id] != 'undefined') {
      var rec = this._coll[query._id];
      if (Q.test(rec, {
        type : 'single'
      }))
        results.push(filterFields(rec, fields));
    }
  } else {
    // match against all records
    var keys = Object.keys(this._coll);
    var len = keys.length;
    for ( var i = 0; i < len; i++) {
      var rec = this._coll[keys[i]];
      if (Q.test(rec, {
        type : 'single'
      }))
        results.push(filterFields(rec, fields));
    }
  }

  process.nextTick(function() {
    callback(null, results);
  });
};

/**
 * Finds the first record that matches a given query. Use this method, if you
 * know that there will be only one resulting document. (E.g. when you want to
 * find a result by its `_id`.)
 * 
 * @param {Object|String}
 *            query resulting objects must match this query.
 * @param {String[]}
 *            [fields] specifies the fields of the resulting objects
 * @param {Object}
 *            [options] ignored
 * @param {Function(err,
 *            res)} callback is called when an error occurs or when the
 *            record(s) return
 */
Collection.prototype.findOne = function(query, fields, options, callback) {
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

  var result = null;
  var Q = filtr(query);

  // find records
  if (typeof query._id != 'undefined') {
    // check if the record with query._id matches the query
    if (typeof this._coll[query._id] != 'undefined') {
      var rec = this._coll[query._id];
      if (Q.test(rec, {
        type : 'single'
      }))
        result = filterFields(rec, fields);
    }
  } else {
    // match against all records
    var keys = Object.keys(this._coll);
    var len = keys.length;
    for ( var i = 0; i < len; i++) {
      var rec = this._coll[keys[i]];
      if (Q.test(rec, {
        type : 'single'
      })) {
        result = filterFields(rec, fields);
        break;
      }
    }
  }

  // callback results
  process.nextTick(function() {
    callback(null, result);
  });
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
 *            [options] ignored
 * @param {Function(err,
 *            saved)} callback is called when an error occurs or when the record
 *            has been saved
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.saved the record, if it has been inserted and `1` if the
 *            record has been updated
 */
Collection.prototype.save = function(record, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = options;
    options = {};
  }

  var _id, result;
  if (typeof record._id == 'undefined') {
    _id = uuid.v1();
    record._id = _id;
    result = record;
  } else {
    _id = record._id;
    result = 1;
  }

  this._coll[_id] = record;
  
  process.nextTick(function() {
    callback(null, result);
  });
};

/**
 * Removes all records that match the given query object from the collection.
 * 
 * @param {Object|String}
 *            query records that match this query will be deleted
 * @param {Object}
 *            [options] ignored
 * @param {Function(err,
 *            deleted)} callback is called when an error occurs or when the
 *            record(s) have been deleted
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.deleted the number of records that have been deleted
 */
Collection.prototype.delete = function(query, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = options;
    options = {};
  }

  var Q = filtr(query);
  var numRemoved = 0;
  
  // find records
  if (typeof query._id != 'undefined') {
    // check if the record with query._id matches the query
    if (typeof this._coll[query._id] != 'undefined') {
      var rec = this._coll[query._id];
      if (Q.test(rec, {
        type : 'single'
      })) {
        numRemoved++;
        delete this._coll[query._id];
      }
    }
  } else {
    // match against all records
    var keys = Object.keys(this._coll);
    var len = keys.length;
    for ( var i = 0; i < len; i++) {
      var rec = this._coll[keys[i]];
      if (Q.test(rec, {
        type : 'single'
      })) {
        numRemoved++;
        delete this._coll[rec._id];
      }
    }
  }

  // callback results
  process.nextTick(function() {
    callback(null, numRemoved);
  });
};
