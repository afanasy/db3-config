var _ = require('underscore')
var expect = require('expect.js')
var db = require('db3').connect(require('solid-config').db3)
var db3Config = require('./')(db)

describe('db3Config', function () {
  describe('#qs', function () {
    describe('#key', function () {
      _.each({
        'key `userId`(`userId`)': {id: 'userId'},
        'key `userId`(`userId`)': {id: 'userId', field: 'userId'},
        'key `name`(`name`(1))': {id: 'name', field: {name: 1}},
        'unique key `name`(`name`(1))': {id: 'name', field: {name: 1}, unique: true},
      },
      function (value, key) {
        it(JSON.stringify(value), function () {
          expect(key).to.be(db3Config.qs.key(value))
        })
      })
    })
    describe('#field', function () {
      _.each({
        '`id` bigint primary key auto_increment': {id: 'id'},
        '`name` text': {id: 'name'},
        '`userId` bigint': {id: 'userId'},
        '`hash` varchar(32)': {id: 'hash', dataType: 'varchar'}
      },
      function (value, key) {
        it(JSON.stringify(value), function () {
          expect(key).to.be(db3Config.qs.field(value))
        })
      })
    })
    describe('#createTable', function () {
      _.each({
        'create table `user` (`id` bigint primary key auto_increment, `name` text)': {id: 'user', field: {id: true, name: true}},
        'create table `user` (`id` bigint primary key auto_increment)': {id: 'user'},
        'create table `user` (`name` text)': {id: 'user', noId: true, field: 'name'},
        'create table `user` (`id` bigint primary key auto_increment, `userId` bigint)': {id: 'user', field: ['userId']},
        'create table `user` (`userId` bigint, key `userId`(`userId`))': {id: 'user', noId: true, field: ['userId'], key: {userId: true}},
        'create table `user` (`name` text, key `name`(`name`(1)))': {id: 'user', noId: true, field: 'name', key: {name: 1}},
        'create table `user` (`userId` bigint, key `userId`(`userId`))': {id: 'user', noId: true, field: 'userId', key: 'userId'}
      },
      function (value, key) {
        it(JSON.stringify(value), function () {
          expect(key).to.be(db3Config.qs.createTable(value))
        })
      })
    })
    describe('#alterTable', function () {
      _.each({
        'alter table `user` add `id` bigint primary key auto_increment': {id: 'user', field: {id: {alter: 'add'}}}
      },
      function (value, key) {
        it(JSON.stringify(value), function () {
          expect(key).to.be(db3Config.qs.alterTable(value))
        })
      })
    })
  })
  describe('#diff', function () {
    _.each([
      {
        config: {table: 'user'},
        dbConfig: {table: {user2: true}},
        out: {
          table: {
            user: {
              create: true,
              id: 'user',
              field: {
                id: {
                  id: 'id',
                  dataType: 'bigint',
                  primaryKey: true,
                  autoIncrement: true
                }
              }
            },
            user2: {
              drop: true
            }
          }
        }
      },
      {
        config: {
          table: {
            user: {
              field: 'userId',
              key: 'userId'
            }
          }
        },
        dbConfig: {
          table: {
            user: true
          }
        },
        out: {
          table: {
            user: {
              field: {
                id: {
                  alter: 'add',
                  id: 'id',
                  dataType: 'bigint',
                  primaryKey: true,
                  autoIncrement: true
                },
                userId: {
                  alter: 'add',
                  id: 'userId',
                  dataType: 'bigint'
                }
              },
              alter: true,
              key: {
                userId: {
                  alter: 'add',
                  id: 'userId',
                  field: {
                    userId: true
                  }
                }
              }
            }
          }
        }
      }
    ],
    function (value, key) {
      it(JSON.stringify(value.config) + ' diff ' + JSON.stringify(value.dbConfig), function () {
        expect(value.out).to.eql(db3Config.diff(value.config, value.dbConfig))
      })
    })
  })
  describe.skip('#sync', function () {
    it('syncs', function (done) {
      db3Config.sync({
        config: {table: require('./test')},
        pushLog: console.log
      }, function (err, data) {
        done()
      })
    })
  })
})
