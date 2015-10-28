var
  db3 = require('db3'),
  _ = require('underscore'),
  Promise = require('bluebird')


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
  Promise.promisifyAll(this.db)
}

Db3Config.prototype.pull = pull
Db3Config.prototype.push = push

function pull (done) {
  var
    db3Config = this,
    data = {}

  getTables(db3Config)
    .then(describeTables(db3Config))
    .then(formatOutput)
    .then(function (output) {
      done(null, output)
    })
    .catch(function (err) {
      done(err)
    })
}

function push (config, done) {
  done()
}

module.exports = Db3Config

function getTables (db3Config) {
  var key = 'Tables_in_' + db3Config.opts.database
  return db3Config.db.queryAsync('show tables')
    .then(function (tables) {
      return tables.map(function (name) {
        return name[key]
      })
    })
}

function describeTables (db3Config) {
  return function (tables) {
    return Promise.all(tables.map(function (table) {
      var output = {table: table}
      return db3Config.db.queryAsync('describe ' + table)
        .then(function (fields) {
          output.fields = fields.map(function (field) {
            return formatField(field)
          })
          return output
        })
    }))
  }
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
