![Moleculer logo](http://moleculer.services/images/banner.png)

# moleculer-db-adapter-rethinkdb [![NPM version](https://img.shields.io/npm/v/moleculer-db-adapter-rethinkdb.svg)](https://www.npmjs.com/package/moleculer-db-adapter-rethinkdb)

RethinkDB adapter for Moleculer DB service.

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