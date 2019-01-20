"use strict";
const Promise = require("bluebird");
const r = require("rethinkdb");

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

        return new Promise(function (resolve, reject) {
            r.connect(this.opts, function (err, conn) {
                if (err) reject(err);
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
                        resolve(true);
                    });
            });
        }.bind(this));
    }

}

module.exports = RethinkDBAdapter;