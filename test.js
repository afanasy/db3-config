require('dotenv').load()
var
  expect = require('expect.js'),
  Db3Config = require('./index.js'),
  opts = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
  },
  config = { "table": { "user": { "field": { "id": true, "username": "text", "password": "varchar(32)", "subscribed": { "dataType": "tinyint(4)", "default": "0" } }, "key": { "subscribed": true } } } },
  alteredConfig = { "table": { "user": { "field": { "id": true, "username": "text", "password": "varchar(32)", "new": "text", "subscribed": { "dataType": "tinyint(4)", "default": "0" } }, "key": { "subscribed": true } } } },
  expected = {
    pull: { "table": { "user": { "field": { "id": true, "username": "text", "password": "varchar(32)", "new": "text", "subscribed": { "dataType": "tinyint(4)", "default": "0" } }, "key": { "subscribed": true } } } },
  }

describe('Db3Config module', function () {
  this.timeout(10 * 1000)
  var db3Config
  before(function(done) {
    db3Config = new Db3Config(opts)
    db3Config.db.dropTable('user', function (err) {
      done()
    })
  });
  describe('basic initialization', function () {
    it('always called using new', function () {
      expect(db3Config.constructor).to.be(Db3Config)
    })
    it('throws error when required parameters are not provided', function () {
      expect(Db3Config).withArgs().to.throwError()
    })
  })
  describe('push method', function () {
    it('alter db from JSON', function (done) {
      db3Config.push(alteredConfig, function (err, data) {
        if (err) return done(err)
        db3Config.pull(function (err, newLocalDb) {
          if (err) return done(err)
          expect(newLocalDb).to.eql(alteredConfig)
          done()
        })
      })
    })
  })
  describe('pull method', function () {
    it('reads data from db into JSON', function (done) {
      db3Config.pull(function (err, data) {
        if (err) {
          return done(err)
        }
        expect(data.table.user.field.id).to.be.ok
        expect(data).to.eql(expected.pull)
        done()
      })
    })
  })
})
