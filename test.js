require('dotenv').load()
var _ = require('underscore')
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

Db3Config(opts)

describe('db3Config', function () {
  describe('#keyQuery', function () {
    _.each({
      'key `userId`(`userId`)': {id: 'userId'},
      'key `userId`(`userId`)': {id: 'userId', field: 'userId'},
      'key `name`(`name`(1))': {id: 'name', field: {name: 1}},
      'unique key `name`(`name`(1))': {id: 'name', field: {name: 1}, unique: true},
    }, function (value, key) {
      it(JSON.stringify(value), function () {
        expect(key).to.be(Db3Config.qs.key(value))
      })
    })
  })
  describe('#fieldQuery', function () {
    _.each({
      '`id` bigint primary key auto_increment': {id: 'id'},
      '`name` text': {id: 'name'},
      '`userId` bigint': {id: 'userId'},
      '`hash` varchar(32)': {id: 'hash', dataType: 'varchar'}
    }, function (value, key) {
      it(JSON.stringify(value), function () {
        expect(key).to.be(Db3Config.qs.field(value))
      })
    })
  })
  describe('#createTable', function () {
    _.each({
      'create table `user` (`id` bigint primary key auto_increment, `name` text)': {id: 'user', field: {id: true}},
      'create table `user` (`id` bigint primary key auto_increment)': {id: 'user', noName: true},
      'create table `user` (`name` text)': {id: 'user', noId: true},
      'create table `user` (`id` bigint primary key auto_increment, `name` text, `userId` bigint)': {id: 'user', field: ['userId']},
      'create table `user` (`userId` bigint, key `userId`(`userId`))': {id: 'user', noId: true, noName: true, field: ['userId'], key: {userId: true}},
      'create table `user` (`name` text, key `name`(`name`(1)))': {id: 'user', noId: true, key: {name: 1}},
      'create table `user` (`userId` bigint, key `userId`(`userId`))': {id: 'user', noId: true, noName: true, field: 'userId', key: 'userId'},
    }, function (value, key) {
      it(JSON.stringify(value), function () {
        expect(key).to.be(Db3Config.qs.createTable(value))
      })
    })
  })
})
