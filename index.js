var _ = require('underscore')
var async = require('async')
var ucfirst = require('ucfirst')
var chalk = require('chalk')
var db

module.exports = function (d) {
  db = d
  return module.exports
}

module.exports.pull = function (done) {
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

module.exports.diff = function (config, done) {
  var diff = {table: {}}
  expand.db(config)
  var end = function (err, dbConfig) {
    //expand.db(dbConfig)
    _.each(_.difference(_.keys(config.table), _.keys(dbConfig.table)), function (tableId) {
      diff.table[tableId] = {create: true}
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
          diff.table[tableId][fieldKey][id] = {alter: 'add'}
        })
        _.each(_.difference(_.keys(dbConfig.table[tableId][fieldKey]), _.keys(table[fieldKey])), function (id) {
          diff.table[tableId].alter = true
          diff.table[tableId][fieldKey][id] = {alter: 'drop'}
        })
      })
    })
    done(err, diff)
  }
  if (config.diff)
    return end(null, config.diff)
  module.exports.pull(end)
}

module.exports.push = function (diff, done) {
  expand.db(diff)
  async.eachSeries(_.keys(diff.table), function (tableId, done) {
    var table = diff.table[tableId]
    var icon = function (action) {
      if (action == 'ok')
        return chalk.green('✓')
      if (action == 'drop')
        return chalk.red('×')
      return chalk.yellow('!')
    }
    var action = _.find(['create', 'drop', 'alter'], function (d) {return table[d]})
    if (!action) {
      console.log(icon('ok'), tableId)
      return done()
    }
    console.log(icon(action), tableId)
    console.log(qs[action + 'Table'](table))
    done()
  }, done)
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
    if (value === true)
      value = {}
    if (key)
      value.id = key
    value.field = value.field || []
    expand.toList(value, 'field')
    _.each(['name', 'id'], function (key) {
      if (!value['no' + ucfirst(key)]) {
        var field = _.object([[key, value.field[key] || true]])
        value.field = _.extend(field, value.field)
      }
    })
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
    return db.format('create table ?? ', table.id) + '(' +
      [].concat(
        _.map(table.field, function (value, key) {return qs.field(value)}),
        _.map(table.key, function (value, key) {return qs.key(value)})
      ).join(', ') + ')'
  },
  dropTable: function (table) {
    expand.table(table)
    return db.format('drop table ?? ', table.id)
  },
  alterTable: function (table) {
    expand.table(table)
    return db.format('alter table ?? ', table.id) +
    [].concat(
      _.map(_.filter(table.field, function (d) {return d.alter}), function (d) {return d.alter + ' ' + qs.field(d)}),
      _.map(_.filter(table.key, function (d) {return d.alter}), function (d) {return d.alter + ' ' + qs.key(d)})
    ).join(', ')
  }
}

module.exports.qs = qs
