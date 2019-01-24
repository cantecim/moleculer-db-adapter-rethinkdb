![Moleculer logo](http://moleculer.services/images/banner.png)

# moleculer-db-adapter-rethinkdb  
RethinkDB adapter for Moleculer DB service.  

[![Coverage Status](https://coveralls.io/repos/github/cantecim/moleculer-db-adapter-rethinkdb/badge.svg?branch=master)](https://coveralls.io/github/cantecim/moleculer-db-adapter-rethinkdb?branch=master)
[![Build Status](https://travis-ci.org/cantecim/moleculer-db-adapter-rethinkdb.svg?branch=master)](https://travis-ci.org/cantecim/moleculer-db-adapter-rethinkdb)
[![NPM version](https://img.shields.io/npm/v/moleculer-db-adapter-rethinkdb.svg)](https://www.npmjs.com/package/moleculer-db-adapter-rethinkdb)
![Downloads](https://img.shields.io/npm/dt/moleculer-db-adapter-rethinkdb.svg?colorB=green)

## Install

```bash
$ npm install moleculer-db moleculer-db-adapter-rethinkdb --save
```

## Usage

```js
"use strict";

const { ServiceBroker } = require("moleculer");
const DbService = require("moleculer-db");
const RethinkDBAdapter = require("moleculer-db-adapter-rethinkdb");

const broker = new ServiceBroker();

// Create a RethinkDB service for `post` entities
broker.createService({
    name: "posts",
    mixins: [DbService],
    adapter: new RethinkDBAdapter({host: "127.0.0.1" || "", port: 29015}),
    collection: "posts"
});


broker.start()
// Create a new post
.then(() => broker.call("posts.create", {
    title: "My first post",
    content: "Lorem ipsum...",
    votes: 0
}))

// Get all posts
.then(() => broker.call("posts.find").then(console.log));

// Change feeds
const { client: conn } = this.schema.adapter;
// Lets get a rethinkdb.table instance
const rTable = this.schema.adapter.getTable();
// You can also get a rethinkdb instance with below
// const rethinkdb = this.schema.adapter.getR();

rTable.changes().run(conn, function(err, cursor) {
    cursor.each(console.log);
});

// Map Reduce with same way
rTable.map((user) => 1).run(conn);

// You can access all underlying API
```

## Options

**Example with connection options**
```js
new RethinkDBAdapter({
    host: "localhost",
    port: 29015
})
```
Above options is used as default when you dont specify any option or pass empty

# Test
```
$ npm test
```

In development with watching

```
$ npm run ci
```

# License
The project is available under the [MIT license](https://tldrlegal.com/license/mit-license).
