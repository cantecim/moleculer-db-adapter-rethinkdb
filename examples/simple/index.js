"use strict";

let {ServiceBroker} = require("moleculer");
let StoreService = require("moleculer-db");
let ModuleChecker = require("moleculer-db/test/checker");
let RethinkDBAdapter = require("../../index");
let Promise = require("bluebird");
let r = require("rethinkdb");

// Create broker
let broker = new ServiceBroker({
    logger: console,
    logLevel: "debug"
});
let adapter;

// Load my service
broker.createService(StoreService, {
    name: "posts",
    adapter: new RethinkDBAdapter(),
    database: "posts",
    table: "posts",
    settings: {},

    afterConnected() {
        this.logger.info("Connected successfully");
        adapter = this.adapter;
        return this.adapter.clear().then(() => start());
    }
});

const checker = new ModuleChecker(21);

// Start checks
function start() {
    return Promise.resolve()
        .delay(500)
        .then(() => checker.execute())
        .catch(console.error)
        .then(() => broker.stop())
        .then(() => checker.printTotal());
}

// --- TEST CASES ---

let ids = [];
let date = new Date();

// Count of posts
checker.add("COUNT", () => adapter.count(), res => {
    console.log(res);
    return res == 0;
});

// Insert a new Post
checker.add("INSERT", () => adapter.insert({
    title: "Hello",
    content: "Post content",
    votes: 3,
    status: true,
    createdAt: date
}), doc => {
    ids[0] = doc.id;
    console.log("Saved: ", ids);
    return true
});

// Find
checker.add("FIND", () => adapter.find({}), res => {
    console.log(res);
    return res.length == 1 && res[0].id == ids[0];
});

// Find by ID
checker.add("GET", () => adapter.findById(ids[0]), res => {
    console.log(res);
    return res.id == ids[0];
});

// Count of posts
checker.add("COUNT", () => adapter.count(), res => {
    console.log(res);
    return res == 1;
});

// Insert many new Posts
checker.add("INSERT MANY", () => adapter.insertMany([
    {title: "Second", content: "Second post content", votes: 8, status: true, createdAt: new Date()},
    {title: "Last", content: "Last document", votes: 1, status: false, createdAt: new Date()}
]), docs => {
    console.log("Saved: ", docs);
    ids[1] = docs[1].id;
    ids[2] = docs[0].id;

    return [
        docs.length == 2,
        ids[1],
        ids[2]
    ];
});

// Count of posts
checker.add("COUNT", () => adapter.count(), res => {
    console.log(res);
    return res == 3;
});

// Find
checker.add("FIND by query", () => adapter.find({query: {title: "Last"}}), res => {
    console.log(res);
    return res.length == 1 && res[0].id == ids[2];
});

// Find
checker.add("FIND by limit, sort, query", () => adapter.find({
    limit: 1,
    sort: {dir: "desc", key: "title"},
    offset: 1
}), res => {
    console.log(res);
    return res.length == 1 && res[0].id == ids[2];
});

// Find
checker.add("FIND by query ($gt)", () => adapter.find({query: r.row("votes").gt(2)}), res => {
    console.log(res);
    return res.length == 2;
});

// Find
checker.add("COUNT by query ($gt)", () => adapter.count({query: r.row("votes").gt(2)}), res => {
    console.log(res);
    return res == 2;
});

// Find by IDs
checker.add("GET BY IDS", () => adapter.findByIds([ids[2], ids[0]]), res => {
    console.log(res);
    return res.length == 2;
});

// Update a posts
checker.add("UPDATE", () => adapter.updateById(ids[2], {
    title: "Last 2",
    updatedAt: new Date(),
    status: true
}), doc => {
    console.log("Updated: ", doc);
    return doc.title == "Last 2";
});

// Update by query
checker.add("UPDATE BY QUERY", () => adapter.updateMany(r.row("votes").lt(5), {
    status: false
}), count => {
    console.log("Updated: ", count);
    return count.replaced == 2;
});

// Remove by query
checker.add("REMOVE BY QUERY", () => adapter.removeMany(r.row("votes").lt(5)), count => {
    console.log("Removed: ", count);
    return count.deleted == 2;
});

// Count of posts
checker.add("COUNT", () => adapter.count(), res => {
    console.log(res);
    return res == 1;
});

// Remove by ID
checker.add("REMOVE BY ID", () => adapter.removeById(ids[1]), doc => {
    console.log("Removed: ", doc);
    return doc.id == ids[1];
});

// Count of posts
checker.add("COUNT", () => adapter.count(), res => {
    console.log(res);
    return res == 0;
});

// Clear
checker.add("CLEAR", () => adapter.clear(), res => {
    console.log(res);
    return true;
});

broker.start();