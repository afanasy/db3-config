
Db3 config is a tool for syncing SQL database structure with JSON config.

## Why do I need it?
To store your database structure as JSON and sync it with real database.

## new
```js
var Db3Config = require('db3-config')
var db3Config = new Db3Config({
  host: 'localhost',
  user: 'me',
  password: 'secret',
  database: 'my_db'
})
```

## .pull(done)
Reads data from db into JSON.
```js
db3Config.pull(function (err, data) {
  console.log(data) //outputs JSON config
})
```

## .push(config)
Reads JSON and updates tables accordingly.
```js
db3Config.push(config)
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
