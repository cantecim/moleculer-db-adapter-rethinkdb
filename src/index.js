"use strict";
const _ = require("lodash");
const Promise = require("bluebird");
const r = require("rethinkdb");
const {Cursor: RethinkDBCursor} = require("rethinkdb/cursor");

class RethinkDBAdapter {

    /**
     * Creates an instance of RethinkDBAdapter.
     * @param {any} opts
     *
     * @memberof RethinkDBAdapter
     */
    constructor(...opts) {
        this.opts = opts;
    }

    /**
     * Initialize adapter
     *
     * @param {ServiceBroker} broker
     * @param {Service} service
     *
     * @memberof RethinkDBAdapter
     */
    init(broker, service) {
        this.broker = broker;
        this.service = service;
        this.service.schema.settings.idField = "id";

        if (!this.service.schema.database) {
            /* istanbul ignore next */
            throw new Error("Missing `database` definition in schema of service!");
        }

        if (!this.service.schema.table) {
            /* istanbul ignore next */
            throw new Error("Missing `table` definition in schema of service!");
        }
    }

    /**
     * Connect to database
     *
     * @returns {Promise}
     *
     * @memberof RethinkDBAdapter
     */
    connect() {
        let {database, table} = this.service.schema;
        this.database = database;
        this.table = table;

        return new Promise(function (resolve, reject) {
            r.connect(...this.opts, function (err, conn) {
                if (err) {
                    reject(err);
                    return;
                }
                this.client = conn;
                conn.on("close", () => {
                    this.service.logger.warn("Disconnected from db");
                });
                conn.use(database);

                Promise.resolve()
                    .then(() => {
                        return new Promise(function (resolve, reject) {
                            r.dbList().run(conn, function (err, res) {
                                if (err) reject(err);
                                if (res.indexOf(database) < 0) {
                                    r.dbCreate(database).run(conn, function (err, res) {
                                        if (err) reject(err);
                                        resolve(res);
                                    });
                                } else {
                                    resolve(true);
                                }
                            });
                        });
                    })
                    .then(() => {
                        return new Promise(function (resolve, reject) {
                            r.db(database).tableList().run(conn, function (err, res) {
                                if (err) reject(err);
                                if (res.indexOf(table) < 0) {
                                    r.db(database).tableCreate(table).run(conn, function (err, res) {
                                        if (err) reject(err);
                                        resolve(res);
                                    });
                                } else {
                                    resolve(true);
                                }
                            });
                        });
                    })
                    .then(() => {
                        this.service.logger.warn("Connected to db");
                        resolve(true);
                    });
            }.bind(this));
        }.bind(this));
    }

    /**
     * Disconnect from database
     *
     * @returns {Promise}
     *
     * @memberof RethinkDBAdapter
     */
    disconnect() {
        if (this.client) {
            return this.client.close();
        }
        return Promise.resolve();
    }

    /**
     * Find all entities by filters.
     *
     * Available filter props:
     *    - limit
     *  - offset
     *  - sort
     *  - search
     *  - searchFields
     *  - query
     *
     * @param {Object} filters
     * @returns {Promise<Array>}
     *
     * @memberof RethinkDBAdapter
     */
    find(filters) {
        return this.createCursor(filters, false).run(this.client).then((cursor) => cursor.toArray());
    }

    /**
     * Find an entity by query
     *
     * @param {Object} query
     * @returns {Promise}
     * @memberof RethinkDBAdapter
     */
    findOne(query) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).filter(query).limit(1).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(res ? res.toArray(): null);
            });
        }.bind(this));
    }

    /**
     * Find an entities by ID.
     *
     * @param {String} _id
     * @returns {Promise<Object>} Return with the found document.
     *
     * @memberof RethinkDBAdapter
     */
    findById(_id) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).get(_id).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(res);
            });
        }.bind(this));
    }

    /**
     * Find any entities by IDs.
     *
     * @param {Array} idList
     * @returns {Promise<Array>} Return with the found documents in an Array.
     *
     * @memberof RethinkDBAdapter
     */
    findByIds(idList) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).getAll(r.args(idList)).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(res ? res.toArray(): []);
            });
        }.bind(this));
    }

    /**
     * Get count of filtered entites.
     *
     * Available query props:
     *  - search
     *  - searchFields
     *  - query
     *
     * @param {Object} [filters={}]
     * @returns {Promise<Number>} Return with the count of documents.
     *
     * @memberof RethinkDBAdapter
     */
    count(filters = {}) {
        return this.createCursor(filters, true).run(this.client);
    }

    /**
     * Insert an entity.
     *
     * @param {Object} entity
     * @returns {Promise<Object>} Return with the inserted document.
     *
     * @memberof RethinkDBAdapter
     */
    insert(entity) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).insert(entity).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(
                    this.findById(res.generated_keys[0])
                );
            }.bind(this));
        }.bind(this));
    }

    /**
     * Insert many entities
     *
     * @param {Array} entities
     * @returns {Promise<Array<Object>>} Return with the inserted documents in an Array.
     *
     * @memberof RethinkDBAdapter
     */
    insertMany(entities) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).insert(entities).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(
                    this.findByIds(res.generated_keys)
                );
            }.bind(this));
        }.bind(this));
    }

    /**
     * Update many entities by `query` and `update`
     *
     * @param {Object} query
     * @param {Object} update
     * @returns {Promise<Number>} Return with the count of modified documents.
     *
     * @memberof RethinkDBAdapter
     */
    updateMany(query, update) {
        return new Promise(function (resolve, reject) {
            if ("$set" in update)
                update = update["$set"];
            r.table(this.table).filter(query).update(update).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(res);
            });
        }.bind(this));
    }

    /**
     * Update an entity by ID and `update`
     *
     * @param {String} _id - ObjectID as hexadecimal string.
     * @param {Object} update
     * @returns {Promise<Object>} Return with the updated document.
     *
     * @memberof RethinkDBAdapter
     */
    updateById(_id, update) {
        return new Promise(function (resolve, reject) {
            if ("$set" in update)
                update = update["$set"];
            r.table(this.table).get(_id).update(update).run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(
                    this.findById(_id)
                );
            }.bind(this));
        }.bind(this));
    }

    /**
     * Remove entities which are matched by `query`
     *
     * @param {Object} query
     * @returns {Promise<Number>} Return with the count of deleted documents.
     *
     * @memberof RethinkDBAdapter
     */
    removeMany(query) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).filter(query).delete().run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(res);
            });
        }.bind(this));
    }

    /**
     * Remove an entity by ID
     *
     * @param {String} _id - ObjectID as hexadecimal string.
     * @returns {Promise<Object>} Return with the removed document.
     *
     * @memberof RethinkDBAdapter
     */
    removeById(_id) {
        return new Promise(function (resolve, reject) {
            r.table(this.table).get(_id).delete().run(this.client, function (err, res) {
                if (err) reject(err);
                const resp = {};
                const idField = this.service.schema.settings.idField;
                resp[idField] = _id;
                resolve(resp);
            }.bind(this));
        }.bind(this));
    }

    /**
     * Clear all entities from collection
     *
     * @returns {Promise}
     *
     * @memberof RethinkDBAdapter
     */
    clear() {
        return new Promise(function (resolve, reject) {
            r.table(this.table).delete().run(this.client, function (err, res) {
                if (err) reject(err);
                resolve(res);
            });
        }.bind(this));
    }

    /**
     * Convert DB entity to JSON object. It converts the `_id` to hexadecimal `String`.
     *
     * @param {Object} entity
     * @returns {Object}
     * @memberof RethinkDBAdapter
     */
    entityToObject(entity) {
        return Object.assign({}, entity);
    }

    /**
     * Create a filtered cursor.
     *
     * Available filters in `params`:
     *  - search
     *    - sort
     *    - limit
     *    - offset
     *  - query
     *
     * @param {Object} params
     * @param {Boolean} isCounting
     * @returns {RethinkDBCursor}
     */
    createCursor(params, isCounting = false) {
        if (params) {
            let q;
            if (isCounting)
                q = r.table(this.table).filter(params.query || {}).count();
            else
                q = r.table(this.table).filter(params.query || {});

            // Sort
            if (params.sort && q.orderBy) {
                q = q.orderBy(r[params.sort.dir](params.sort.key))
            }

            // Offset
            if (_.isNumber(params.offset) && params.offset > 0)
                q = q.skip(params.offset);

            // Limit
            if (_.isNumber(params.limit) && params.limit > 0)
                q = q.limit(params.limit);

            return q;
        }

        // If not params
        if (isCounting)
            return r.table(this.table).count();
        else
            return r.table(this.table);
    }

    /**
     * Transforms 'idField' into MongoDB's '_id'
     * @param {Object} entity
     * @param {String} idField
     * @memberof RethinkDBAdapter
     * @returns {Object} Modified entity
     */
    beforeSaveTransformID(entity, idField) {
        return entity
    }

    /**
     * Transforms MongoDB's '_id' into user defined 'idField'
     * @param {Object} entity
     * @param {String} idField
     * @memberof RethinkDBAdapter
     * @returns {Object} Modified entity
     */
    afterRetrieveTransformID(entity, idField) {
        return entity;
    }

    /**
     * Return rethinkdb instance
     *
     * @returns {*|rethinkdb}
     */
    getR() {
        return r;
    }

    /**
     * Return rethinkdb.table instance
     *
     * @returns {*|rethinkdb}
     */
    getTable() {
        return r.table(this.table);
    }

}

module.exports = RethinkDBAdapter;