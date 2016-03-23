var _ = require('underscore')
var async = require('async')
var ucfirst = require('ucfirst')
var db3 = require('db3')
var db

var pull = function (db, done) {
  var dbConfig = {table: {}}
  db.query('select database() as db', function (err, data) {
    db.select({table: 'information_schema.columns', where: {table_schema: data[0].db}, orderBy: ['table_name', 'ordinal_position']}, function (err, data) {
      _.each(data, function (data) {
        var field = {dataType: data.DATA_TYPE}
        if (data.DATA_TYPE == 'varchar')
          field.size = data.CHARACTER_MAXIMUM_LENGTH
        if (data.IS_NULLABLE == 'NO')
          field.notNull = true
        if (data.COLUMN_DEFAULT != null)
          field.default = data.COLUMN_DEFAULT
        if (field.dataType.match(/int/))
          field.default = +field.default
        dbConfig.table[data.TABLE_NAME] = dbConfig.table[data.TABLE_NAME] || {field: {}}
        dbConfig.table[data.TABLE_NAME].field[data.COLUMN_NAME] = field
      })
      done(err, dbConfig)
    })
  })
}

var push = function (db, dbConfig, done) {
  async.each(_.keys(dbConfig.table), function (tableId, done) {
    var table = _.extend(dbConfig.table[tableId], {id: tableId})
    console.log(createTable(table))
    db.query(createTable(table), function (err, data) {
      done(err, data)
    })
  }, done)
}

function Db3Config (opts) {
  if (this.constructor !== Db3Config)
    return new Db3Config(opts)
  opts.host = opts.host || 'localhost'
  opts.user = opts.user || 'root'
  opts.password = opts.password || ''
  this.opts = opts
  db = this.db = db3.connect(opts)
  /*
  pull(db, function (err, data) {
    console.log(JSON.stringify(data, null, '  '))
    db.dropTable('user', function (err, data) {
      push(db, require('./sample'), _.noop)
    })
  })
  */
}

Db3Config.prototype.pull = function pull (done) {
  var db3Config = this

  getTables(db3Config, function (err, tables) {
    if (err) return done(err)
    var
      output = [],
      count = 0,
      target = tables.length

    if (!tables.length)
      return done(null, formatOutput([]))
    tables.forEach(function (table) {
      describeTable(db3Config, table, function (err, fields) {
        if (err) return done(err)
        output.push(fields)

        if (++count === target) {
          return done(null, formatOutput(output))
        }
      })
    })
  })
}

Db3Config.prototype.push = function push (config, done) {
  var
    db3Config = this,
    db = { table: { } },
    queries = []

  db3Config.pull(function (err, localDb) {
    _.each(config.table, function (config, table) {
      if (_.has(config, 'field')) {
        db.table[table] = _.pick(config, ['field', 'key'])
      }
    })
    _.each(db.table, function (table, id) {

      // if not present in old db, create
      if (!localDb.table[id]) {
        db.table[id].id = id
        queries.push(create(table))

      // table presents in old db
      } else {

        // check for new field in new config
        _.each(db.table[id].field, function (field, fieldId) {

          // if new field not present in local db, add
          if (!localDb.table[id].field[fieldId]) {
            var fieldObj = {}
            fieldObj[fieldId] = field
            queries.push(add(id, fieldObj))
          }
        })

        // check for old local db
        _.each(localDb.table[id].field, function (field, fieldId) {

          // if old field not present in new config, drop
          if (!db.table[id].field[fieldId]) {
            queries.push(drop(id, fieldId))
          }
        })
      }
    })
    var
      count = 0,
      target = queries.length
    queries.forEach(function (query) {
      db3Config.db.query(query, function (err) {
        if (err) return done(err)
        if (++count === target) {
          return done()
        }
      })
    })
  })
}

module.exports = Db3Config

var expand = {
  toList: function (list, key) {
    var value = list[key]
    if (value === true)
      value = key
    if (_.isNaN(value) || _.isNull(value) || _.isUndefined(value) || _.isNumber(value) || _.isBoolean(value) || _.isString(value))
      value = [value]
    if (_.isArray(value))
      value = _.mapObject(_.invert(value), function () {return true})
    list[key] = value
    return value
  },
  field: function (value, key, list) {
    if (value === true)
      value = {}
    if (_.isString(value))
      value = {dataType: value}
    if (key)
      value.id = key
    if (value.id.match(/Id$/))
      _.defaults(value, {dataType: 'bigint'})
    if (value.id == 'id')
      _.defaults(value, {dataType: 'bigint', primaryKey: true, autoIncrement: true})
    if (value.id == 'name')
      _.defaults(value, {dataType: 'text'})
    if (value.dataType == 'varchar')
      _.defaults(value, {size: 32})
    if (list)
      list[key] = value
    return value
  },
  key: function (value, key, list) {
    if (value === true)
      value = {}
    if (_.isNumber(value))
      value = {field: _.object([[key, value]])}
    if (key)
      value.id = key
    if (!value.field)
      value.field = value.id
    expand.toList(value, 'field')
    if (list)
      list[key] = value
    return value
  },
  table: function (value, key, list) {
    if (key)
      value.id = key
    value.field = value.field || []
    expand.toList(value, 'field')
    if (value.key)
      expand.toList(value, 'key')
    _.each(['name', 'id'], function (key) {
      if (!value['no' + ucfirst(key)]) {
        var field = _.object([[key, value.field[key] || true]])
        value.field = _.extend(field, value.field)
      }
    })
  }
}

var qs = {
  field: function (field) {
    expand.field(field)
    var query = db.format('??', field.id) + ' ' + field.dataType
    if (field.size)
      query += '(' + field.size + ')'
    if (field.notNull)
      query += ' not null'
    if (!_.isUndefined(field.default) && (field.dataType != 'text'))
      query += db.format(' default ?', field.default)
    if (field.primaryKey)
      query += ' primary key'
    if (field.autoIncrement)
      query += ' auto_increment'
    return query
  },
  key: function (key) {
    expand.key(key)
    var query = ''
    if (key.unique)
      query += 'unique '
    query += 'key ' + db.format('??', key.id) + '(' +
    _.map(key.field, function (value, key) {
      var query = db.format('??', key)
      if (_.isNumber(value))
        query += '(' + value + ')'
      return query
    }).join(', ') + ')'
    return query
  },
  createTable: function (table) {
    expand.table(table)
    return 'create table `' + table.id + '` (' +
      [].concat(
        _.map(table.field, function (value, key) {return qs.field(expand.field(value, key, table.field))}),
        _.map(table.key, function (value, key) {return qs.key(expand.key(value, key, table.key))})
      ).join(', ') + ')'
  }
}

module.exports.qs = qs

function add(table, fields) {
  var query = 'ALTER TABLE `' + table + '` ADD ' +
    createQuery(fields).join(', ADD ')
  return query
}

function drop(table, fields) {
  fields = _.isArray(fields) ? fields : [fields]
  var query = 'ALTER TABLE `' + table + '` DROP `' + fields.join('`, DROP `') + '`'
  return query
}
