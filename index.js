var
  db3 = require('db3'),
  _ = require('underscore')


function Db3Config (opts) {
  if (this.constructor !== Db3Config)
    return new Db3Config(opts)
  if (!opts.database)
    throw (new Error('Please specify database name'))
  opts.host = opts.host || 'localhost'
  opts.user = opts.user || 'root'
  opts.password = opts.password || ''
  this.opts = opts
  this.db = db3.connect(opts)
}

Db3Config.prototype.pull = pull
Db3Config.prototype.push = push

function pull (done) {
  var
    db3Config = this

  getTables(db3Config, function (err, tables) {
    if (err) return done(err)
    var
      output = [],
      count = 0,
      target = tables.length

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

function push (config, done) {
  done()
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
  } else if (field.Null === 'NO' && field.Default === null && field.Key === '') {
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
