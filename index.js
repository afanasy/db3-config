var
  db3 = require('db3'),
  qs = require('querystring'),
  _ = require('underscore')


function Db3Config (opts) {
  if (this.constructor !== Db3Config)
    return new Db3Config(opts)
  opts.host = opts.host || 'localhost'
  opts.user = opts.user || 'root'
  opts.password = opts.password || ''
  this.opts = opts
  this.db = db3.connect(opts)
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

function getTables (db3Config, done) {
  var key = 'Tables_in_' + db3Config.opts.database
  db3Config.db.query('show tables', function (err, tables) {
    if (err) return done(err)
    tables = tables.map(function (name) {
      return name[key]
    })
    return done(null, tables)
  })
}

function describeTable (db3Config, table, done) {
  var output = {table: table}
  return db3Config.db.query('describe ' + table, function (err, fields) {
    if (err) return done(err)
    output.fields = fields.map(function (field) {
      return formatField(field)
    })
    done(null, output)
  })
}

function formatField (field) {
  var output = { key: field.Field }, value
  if (field.Key === 'PRI') {
    value = true
  } else if (field.Default === null && field.Key === '') {
    value = field.Type
  } else {
    value = {
      dataType: field.Type,
      default: field.Default
    }
    if (field.Key === 'MUL') {
      value.key = true
    }
  }
  output.value = value
  return output
}

function formatOutput (tables) {
  var output = {
    table: {}
  }
  tables.forEach(function (table) {
    var field = {}, key = {}
    table.fields.forEach(function (innerField) {
      if (innerField.value && innerField.value.key) {
        key[innerField.key] = true
        delete innerField.value.key
      }

      field[innerField.key] = innerField.value
    })
    output.table[table.table] = {
      field: field,
      key: key,
    }
  })
  return output
}

function createQuery(fields, keys) {
  keys = keys || {}
  var
    queries = [],
    queryKeys = []

  _.each(fields, function (attr, field) {

    // create array, later will be joined
    var query = ['`' + field + '`']

    // if true, field is primary key
    if (attr === true) {
      query.push('bigint(20) NOT NULL AUTO_INCREMENT')
      queryKeys.push('PRIMARY KEY (`' + field + '`)')

    // if string, field is data type
    } else if (_.isString(attr)) {
      query.push(attr)

    // if object, create based on key / property
    } else if (_.isObject(attr)) {
      var dataType = attr.dataType || 'text'
      query.push(dataType)

      // unless specified, always put not null
      if (attr.notNull === false) query.push('NOT NULL')
      if (_.has(attr, 'default')) query.push('DEFAULT ' + qs.escape(attr.default))
    }

    // if field has key option
    if (_.has(keys, field)) {
      var attr = keys[field]

      // if true, field is indexed
      if (attr === true) {
        queryKeys.push('KEY `' + field + '` (`' + field + '`)')

      // if object, check if it is unique index
      } else if (_.isObject(attr)) {
        var
          keyFields = [],
          keyType = 'KEY'

        // if no field option, use field name
        if (!_.has(attr, 'field')) {
          keyFields = [field]

        // if there is field option, find field name
        } else {

          // if true, use field name
          if (attr.field === true) {
            keyFields.push(field)

          // if string, use it as field name
          } else if (_.isString(attr.field)) {
            keyFields.push(attr.field)

          // if array, use all
          } else if (_.isArray(attr.field)) {
            keyFields = keyFields.concat(attr.field)
          }
        }

        if (attr.unique)
          keyType = 'UNIQUE KEY'
        queryKeys.push(keyType + ' `' + field + '` (`' + keyFields.join('`, `') + '`)')

      }
    }
    queries.push(query.join(' '))
  })
  queries = queries.concat(queryKeys)
  return queries
}

function create(table) {
  var query = 'CREATE table `' + table.id + '` (' +
    createQuery(table.field, table.key).join(', ') +
    ') ENGINE=myisam'
  return query
}

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
