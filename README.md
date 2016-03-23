
Db3 config is a tool for syncing SQL database structure with JSON config.

## Why do I need it?
To store your database structure as JSON and sync it with real database.

## Init
```js
var db = require('db3').connect()
var db3Config = require('db3-config')(db)
```

## .pull(done)
Reads data from db into JSON.
```js
db3Config.pull(function (err, data) {
  console.log(data) //outputs JSON config
})
```

## .diff(config, done)
Calculates diff
```js
db3Config.diff(config, function (err, data) {
  //data is diff
})
```

## .push(diff, done)
Apply diff to the db
```js
db3Config.push(diff, function (err) {})
```

## Config format
```js
{
  "table": {
    "user": {
      "field": {
        "id": true, //will be bigint primary key, auto_increment
        "username": "text",
        "password": "varchar(32)",
        "subscribed": {
          "dataType": "tinyint",
          "default": 0
        }
      },
      "key": {
        "subscribed": true
      }
    }
  }
}
```
