var _ = require('underscore')
var async = require('async')
var ucfirst = require('ucfirst')
var chalk = require('chalk')
var format = require('sqlstring').format

var Db3Config = module.exports = function (d) {
  if (this.constructor !== Db3Config)
    return new Db3Config(d)
  this.db = d
  this.qs = qs
}

Db3Config.prototype.pull = function (done) {
  var db = this.db
  var dbConfig = {table: {}}
  var database
  async.series([
    function (done) {
      db.query('select database() as db', function (err, data) {
        database = data[0].db
        done()
      })
    },
    function (done) {
      db.select({table: 'information_schema.columns', where: {table_schema: database}, orderBy: ['table_name', 'ordinal_position']}, function (err, data) {
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
        done()
      })
    },
    function (done) {
      db.select({table: 'information_schema.statistics', where: {table_schema: database, index_name: {'!=': 'PRIMARY'}}, orderBy: ['table_name', 'index_name', 'seq_in_index']}, function (err, data) {
        _.each(data, function (data) {
          dbConfig.table[data.TABLE_NAME].key = dbConfig.table[data.TABLE_NAME].key || {}
          dbConfig.table[data.TABLE_NAME].key[data.INDEX_NAME] = dbConfig.table[data.TABLE_NAME].key[data.INDEX_NAME] || {field: []}
          dbConfig.table[data.TABLE_NAME].key[data.INDEX_NAME].field.push(data.COLUMN_NAME)
          if (!+data.NON_UNIQUE)
            dbConfig.table[data.TABLE_NAME].key[data.INDEX_NAME].unique = true
        })
        done()
      })
    }
  ],
  function (err) {
    done(err, dbConfig)
  })
}

Db3Config.prototype.diff = function (config, dbConfig) {
  var diff = {table: {}}
  expand.db(config)
  _.each(_.difference(_.keys(config.table), _.keys(dbConfig.table)), function (tableId) {
    diff.table[tableId] = _.extend({create: true}, config.table[tableId])
  })
  _.each(_.difference(_.keys(dbConfig.table), _.keys(config.table)), function (tableId) {
    diff.table[tableId] = {drop: true}
  })
  _.each(config.table, function (table, tableId) {
    if (diff.table[tableId])
      return
    diff.table[tableId] = {}
    _.each(['field', 'key'], function (fieldKey) {
      diff.table[tableId][fieldKey] = {}
      _.each(_.difference(_.keys(table[fieldKey]), _.keys(dbConfig.table[tableId][fieldKey])), function (id) {
        diff.table[tableId].alter = true
        diff.table[tableId][fieldKey][id] = _.extend({alter: 'add'}, table[fieldKey][id])
      })
      _.each(_.difference(_.keys(dbConfig.table[tableId][fieldKey]), _.keys(table[fieldKey])), function (id) {
        diff.table[tableId].alter = true
        diff.table[tableId][fieldKey][id] = {alter: 'drop'}
      })
    })
  })
  return diff
}

Db3Config.prototype.push = function (diff, done) {
  var db = this.db
  _.defaults(diff, {push: {}})
  _.defaults(diff.push, {
    log: _.noop,
  })
  var icon = function (action) {
    if (action == 'ok')
      return chalk.green('✓')
    if (action == 'drop')
      return chalk.red('×')
    return chalk.yellow('!')
  }
  expand.db(diff)
  async.eachSeries(_.keys(diff.table), function (tableId, done) {
    var table = diff.table[tableId]
    var action = _.find(['create', 'drop', 'alter'], function (d) {return table[d]})
    if (!action) {
      diff.push.log(icon('ok'), tableId)
      return done()
    }
    if (diff.push.stash && (action == 'drop'))
      action = 'stash'
    var query = qs[action + 'Table'](table)
    diff.push.log(icon(action), tableId)
    diff.push.log(query)
    if (diff.push.test)
      return done()
    if (diff.push.noDrop && (action == 'drop'))
      return done()
    db.query(query, function (err) {
      if (err)
        diff.push.log(chalk.red(err))
      done()
    })
  }, done)
}

Db3Config.prototype.sync = function (d, done) {
  var self = this
  self.pull(function (err, data) {
    self.push(_.extend(self.diff(d.config, data), d), done)
  })
}

var expand = {
  toList: function (list, key, expandChild) {
    var value = list[key] || []
    if (value === true)
      value = key
    if (_.isNaN(value) || _.isNull(value) || _.isUndefined(value) || _.isNumber(value) || _.isBoolean(value) || _.isString(value))
      value = [value]
    if (_.isArray(value))
      value = _.mapObject(_.invert(value), function () {return true})
    if (expandChild)
      _.each(value, expand[key])
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
    if (value.dataType == 'varchar')
      _.defaults(value, {size: 32})
    _.defaults(value, {dataType: 'text'})
    if (list)
      list[key] = value
    return value
  },
  key: function (value, key, list) {
    if (value === true)
      value = {}
    if (_.isNumber(value))
      value = {field: _.object([[key, value]])}
    if (_.isString(value))
      value = [value]
    if (_.isArray(value))
      value = {field: value}
    if (key)
      value.id = key
    if (!value.field)
      value.field = value.id
    if (_.isArray(value.field))
      value.field = _.map(value.field, function (field) {
        if (!field.match(/\(\d+\)$/))
          return field
        return field.re
      })
    expand.toList(value, 'field')
    if (list)
      list[key] = value
    return value
  },
  table: function (value, key, list) {
    if (value === true)
      value = {}
    if (key)
      value.id = key
    value.field = value.field || []
    expand.toList(value, 'field')
    if (!value.noId)
      value.field = _.extend(_.object([['id', value.field.id || true]]), value.field)
    _.each(value.field, expand.field)

    if (value.key)
      expand.toList(value, 'key', true)

    if (list)
      list[key] = value
    return value
  },
  db: function (value, key, list) {
    expand.toList(value, 'table', true)
  }
}

var qs = {
  field: function (field) {
    expand.field(field)
    var query = format('??', field.id)
    if (field.alter == 'drop')
      return query
    query += ' ' + field.dataType
    if (field.size)
      query += '(' + field.size + ')'
    if (field.notNull)
      query += ' not null'
    if (!_.isUndefined(field.default) && (field.dataType != 'text'))
      query += format(' default ?', field.default)
    if (field.primaryKey)
      query += ' primary key'
    if (field.autoIncrement)
      query += ' auto_increment'
    return query
  },
  key: function (key) {
    expand.key(key)
    var query = 'key ' + format('??', key.id)
    if (key.alter == 'drop')
      return query
    if (key.unique)
      query = 'unique ' + query
    query += '(' +
    _.map(key.field, function (value, key) {
      var query = format('??', key)
      if (_.isNumber(value))
        query += '(' + value + ')'
      return query
    }).join(', ') + ')'
    return query
  },
  createTable: function (table) {
    expand.table(table)
    return format('create table ?? ', table.id) + '(' +
      [].concat(
        _.map(table.field, function (value, key) {return qs.field(value)}),
        _.map(table.key, function (value, key) {return qs.key(value)})
      ).join(', ') + ')'
  },
  dropTable: function (table) {
    expand.table(table)
    return format('drop table ?? ', table.id)
  },
  alterTable: function (table) {
    expand.table(table)
    return format('alter table ?? ', table.id) +
    [].concat(
      _.map(_.filter(table.field, function (d) {return d.alter}), function (d) {return d.alter + ' ' + qs.field(d)}),
      _.map(_.filter(table.key, function (d) {return d.alter}), function (d) {return d.alter + ' ' + qs.key(d)})
    ).join(', ')
  },
  stashTable: function (table) {
    expand.table(table)
    return format('rename table ?? to stash.??', [table.id, (+new Date) + '_' + table.id])
  }
}
