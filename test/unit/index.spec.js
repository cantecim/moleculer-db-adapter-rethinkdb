"use strict";
const {ServiceBroker} = require("moleculer");
jest.mock("rethinkdb");
const RethinkDBAdapter = require("../../src");
const rethinkdb = require("rethinkdb");

function protectReject(err) {
    if (err && err.stack) {
        console.error(err);
        console.error(err.stack);
    }
    expect(err).toBe(true);
}

const fakeConnection = {
    use: jest.fn(),
    close: jest.fn(() => Promise.resolve())
};

const fakeCursor = {
    toArray: jest.fn()
};
let fakeRunResult = jest.fn(() => []);
const fakeRunnablePromise = {
    run: jest.fn((conn, cb) => {
        if (cb)
            cb(null, fakeRunResult());
        else
            return Promise.resolve(fakeCursor);
    })
};

rethinkdb.connect = jest.fn((opts, cb) => {
    cb(null, fakeConnection);
});
rethinkdb.db = jest.fn(() => rethinkdb);
rethinkdb.dbList = jest.fn(() => fakeRunnablePromise);
rethinkdb.dbCreate = jest.fn((database) => fakeRunnablePromise);
rethinkdb.tableList = jest.fn(() => fakeRunnablePromise);
rethinkdb.tableCreate = jest.fn((database) => fakeRunnablePromise);

const fakeFilter = {
    orderBy: jest.fn(),
    skip: jest.fn(() => fakeFilter),
    limit: jest.fn(() => fakeFilter),
    count: jest.fn(),
    run: jest.fn(() => fakeRunnablePromise),
    update: jest.fn(() => fakeRunnablePromise),
    delete: jest.fn(() => fakeRunnablePromise)
};
const fakeTable = {
    filter: jest.fn(() => fakeFilter),
    count: jest.fn(),
    get: jest.fn(() => fakeRunnablePromise),
    getAll: jest.fn(() => fakeRunnablePromise),
    insert: jest.fn(() => fakeRunnablePromise),
    delete: jest.fn(() => fakeRunnablePromise)
};
rethinkdb.table = jest.fn(() => fakeTable);

describe("Test RethinkDBAdapter", () => {
    const broker = new ServiceBroker({logger: false});
    const service = broker.createService({
        settings: {
            idField: "_id"
        },
        name: "store",
        table: "posts",
        database: "posts"
    });

    const opts = {
        host: "localhost",
        port: 29015
    };
    const adapter = new RethinkDBAdapter(opts);

    it("should be created", () => {
        expect(adapter).toBeDefined();
        expect(adapter.opts[0]).toBe(opts);

        expect(adapter.init).toBeInstanceOf(Function);
        expect(adapter.connect).toBeInstanceOf(Function);
        expect(adapter.disconnect).toBeInstanceOf(Function);
        expect(adapter.find).toBeInstanceOf(Function);
        expect(adapter.findOne).toBeInstanceOf(Function);
        expect(adapter.findById).toBeInstanceOf(Function);
        expect(adapter.findByIds).toBeInstanceOf(Function);
        expect(adapter.count).toBeInstanceOf(Function);
        expect(adapter.insert).toBeInstanceOf(Function);
        expect(adapter.insertMany).toBeInstanceOf(Function);
        expect(adapter.updateMany).toBeInstanceOf(Function);
        expect(adapter.updateById).toBeInstanceOf(Function);
        expect(adapter.removeMany).toBeInstanceOf(Function);
        expect(adapter.removeById).toBeInstanceOf(Function);
        expect(adapter.clear).toBeInstanceOf(Function);
        expect(adapter.beforeSaveTransformID).toBeInstanceOf(Function);
        expect(adapter.afterRetrieveTransformID).toBeInstanceOf(Function)
    });

    it("throw error in init if 'database' is not defined", () => {
        expect(() => {
            service.schema.database = undefined;
            adapter.init(broker, service);
            service.schema.database = "posts";
        }).toThrow("Missing `database` definition in schema of service!");
    });

    it("throw error in init if 'table' is not defined", () => {
        expect(() => {
            service.schema.database = "posts";
            service.schema.table = undefined;
            adapter.init(broker, service);
            service.schema.table = "posts";
        }).toThrow("Missing `table` definition in schema of service!");
    });

    it("call init", () => {
        service.schema.database = "posts";
        service.schema.table = "posts";
        adapter.init(broker, service);
        expect(adapter.broker).toBe(broker);
        expect(adapter.service).toBe(service);
    });

    it("call connect with opts", () => {
        fakeConnection.use.mockClear();
        fakeRunnablePromise.run.mockClear();

        return adapter.connect().catch(protectReject).then(() => {
            expect(rethinkdb.connect).toHaveBeenCalledTimes(1);

            expect(fakeConnection.use).toHaveBeenCalledTimes(1);

            expect(rethinkdb.dbList).toHaveBeenCalledTimes(1);
            expect(rethinkdb.dbCreate).toHaveBeenCalledTimes(1);
            expect(rethinkdb.dbCreate).toHaveBeenCalledWith(service.schema.database);

            expect(rethinkdb.tableList).toHaveBeenCalledTimes(1);
            expect(rethinkdb.tableCreate).toHaveBeenCalledTimes(1);
            expect(rethinkdb.tableCreate).toHaveBeenCalledWith(service.schema.table);
        });
    });

    it("call disconnect", () => {
        fakeConnection.close.mockClear();

        return adapter.disconnect().catch(protectReject).then(() => {
            expect(fakeConnection.close).toHaveBeenCalledTimes(1);
        });
    });

    describe("Test createCursor method", () => {
        it("call without parameters", () => {
            adapter.createCursor();
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
        });

        it("call without parameters & count", () => {
            rethinkdb.table.mockClear();

            adapter.createCursor(null, true);
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
            expect(fakeTable.count).toHaveBeenCalledTimes(1);
        });

        it("call with query", () => {
            rethinkdb.table.mockClear();

            const query = {};
            adapter.createCursor({query});
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
        });

        it("call with query & count", () => {
            rethinkdb.table.mockClear();
            fakeTable.filter.mockClear();

            const query = {};
            adapter.createCursor({query}, true);
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
            expect(fakeFilter.count).toHaveBeenCalledTimes(1);
        });

        it("call with sort object", () => {
            rethinkdb.table.mockClear();
            fakeTable.filter.mockClear();

            const query = {};
            const sort = {
                dir: "asc",
                key: "name"
            };
            rethinkdb.asc = jest.fn((key) => key);

            adapter.createCursor({query, sort});
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
            expect(fakeFilter.orderBy).toHaveBeenCalledTimes(1);
            expect(fakeFilter.orderBy).toHaveBeenCalledWith(sort.key);
        });

        it("call with limit & offset", () => {
            rethinkdb.table.mockClear();
            fakeTable.filter.mockClear();

            const query = {};
            const [limit, offset] = [10, 2];
            rethinkdb.asc = jest.fn((key) => key);

            adapter.createCursor({query, limit, offset});
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
            expect(fakeFilter.skip).toHaveBeenCalledTimes(1);
            expect(fakeFilter.skip).toHaveBeenCalledWith(offset);
            expect(fakeFilter.limit).toHaveBeenCalledTimes(1);
            expect(fakeFilter.limit).toHaveBeenCalledWith(limit);
        });
    });

    it("call find", () => {
        adapter.createCursor = jest.fn(() => fakeRunnablePromise);

        let params = {};
        return adapter.find(params).catch(protectReject).then(() => {
            expect(adapter.createCursor).toHaveBeenCalledTimes(1);
            expect(adapter.createCursor).toHaveBeenCalledWith(params, false);

            expect(fakeCursor.toArray).toHaveBeenCalledTimes(1);
        });
    });

    it("call findOne", () => {
        fakeRunResult = jest.fn(() => fakeCursor);
        rethinkdb.table.mockClear();
        fakeTable.filter.mockClear();

        let query = {age: 22};
        adapter.findOne(query).catch(protectReject).then(() => {
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
            expect(fakeFilter.limit).toHaveBeenCalledTimes(1);
            expect(fakeCursor.toArray).toHaveBeenCalledTimes(1);
        });
    });

    it("call findById", () => {
        fakeTable.get.mockClear();
        const id = 5;
        return adapter.findById(id).catch(protectReject).then(() => {
            expect(fakeTable.get).toHaveBeenCalledTimes(1);
            expect(fakeTable.get).toHaveBeenCalledWith(id);
        });
    });

    it("call findByIds", () => {
        fakeCursor.toArray.mockClear();
        fakeTable.getAll.mockClear();
        rethinkdb.args = jest.fn(inp => inp);

        const ids = [5, 8, 10];
        return adapter.findByIds(ids).catch(protectReject).then(() => {
            expect(fakeTable.getAll).toHaveBeenCalledTimes(1);
            expect(fakeTable.getAll).toHaveBeenCalledWith(ids);

            expect(fakeCursor.toArray).toHaveBeenCalledTimes(1);
        });
    });

    it("call count", () => {
        adapter.createCursor = jest.fn(() => fakeRunnablePromise);
        fakeRunnablePromise.run.mockClear();

        let params = {};
        return adapter.count(params).catch(protectReject).then(() => {
            expect(adapter.createCursor).toHaveBeenCalledTimes(1);
            expect(adapter.createCursor).toHaveBeenCalledWith(params, true);
            expect(fakeRunnablePromise.run).toHaveBeenCalledTimes(1);
            expect(fakeRunnablePromise.run).toHaveBeenCalledWith(adapter.client);
        });
    });

    it("call insert", () => {
        let entity = {a: 5};

        fakeTable.get.mockClear();
        fakeTable.get.mockReturnValueOnce({
            run: jest.fn((client, cb) => {
                cb(null, entity)
            })
        });

        fakeRunResult = jest.fn(() => ({generated_keys: [1]}));
        return adapter.insert(entity).catch(protectReject).then(res => {
            expect(fakeTable.get).toHaveBeenCalledTimes(1);
            expect(fakeTable.get).toHaveBeenCalledWith(1);
            expect(res).toEqual(entity);
            expect(fakeTable.insert).toHaveBeenCalledTimes(1);
            expect(fakeTable.insert).toHaveBeenCalledWith(entity);
        });
    });

    it("call insertMany", () => {
        let entities = [
            {a: 5},
            {a: 10}
        ];

        fakeTable.insert.mockClear();
        fakeTable.getAll.mockClear();
        fakeTable.getAll.mockReturnValueOnce({
            run: jest.fn((client, cb) => {
                cb(null, {
                    toArray: () => entities
                });
            })
        });

        fakeRunResult = jest.fn(() => ({generated_keys: [1, 2]}));
        return adapter.insertMany(entities).catch(protectReject).then(res => {
            expect(fakeTable.getAll).toHaveBeenCalledTimes(1);
            expect(fakeTable.getAll).toHaveBeenCalledWith([1, 2]);
            expect(res).toEqual(entities);
            expect(fakeTable.insert).toHaveBeenCalledTimes(1);
            expect(fakeTable.insert).toHaveBeenCalledWith(entities);
        });
    });

    it("call updateMany", () => {
        let query = {};
        let update = {};

        fakeTable.filter.mockClear();
        fakeFilter.update.mockClear();
        fakeRunResult = jest.fn(() => ({replaced: 2}));
        return adapter.updateMany(query, update).catch(protectReject).then(res => {
            expect(res.replaced).toEqual(2);
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
            expect(fakeFilter.update).toHaveBeenCalledTimes(1);
            expect(fakeFilter.update).toHaveBeenCalledWith(update);
        });
    });

    it("call updateById", () => {
        let update = {};
        const doc = {id: 1};

        fakeTable.filter.mockClear();
        fakeTable.get.mockClear();
        fakeFilter.update.mockClear();
        fakeRunResult = jest.fn(() => doc);
        let intermediateGet;
        fakeTable.get.mockReturnValueOnce(intermediateGet = {
            update: jest.fn(() => fakeRunnablePromise)
        });
        return adapter.updateById(1, update).catch(protectReject).then(res => {
            expect(res).toEqual(doc);
            expect(fakeTable.get).toHaveBeenCalledTimes(2);
            expect(fakeTable.get).toHaveBeenCalledWith(1);
            expect(intermediateGet.update).toHaveBeenCalledTimes(1);
            expect(intermediateGet.update).toHaveBeenCalledWith(update);
        });
    });

    it("call removeMany", () => {
        let query = {};
        fakeTable.filter.mockClear();

        return adapter.removeMany(query).catch(protectReject).then(() => {
            expect(fakeTable.filter).toHaveBeenCalledTimes(1);
            expect(fakeTable.filter).toHaveBeenCalledWith(query);
            expect(fakeFilter.delete).toHaveBeenCalledTimes(1);
        });
    });

    it("call removeById", () => {
        fakeTable.get.mockClear();
        let intermediateGet;
        fakeTable.get.mockReturnValueOnce(intermediateGet = {
            delete: jest.fn(() => fakeRunnablePromise)
        });
        return adapter.removeById(5).catch(protectReject).then(() => {
            expect(fakeTable.get).toHaveBeenCalledTimes(1);
            expect(fakeTable.get).toHaveBeenCalledWith(5);

            expect(intermediateGet.delete).toHaveBeenCalledTimes(1);
        });
    });

    it("call clear", () => {
        rethinkdb.table.mockClear();

        return adapter.clear().catch(protectReject).then(() => {
            expect(fakeTable.delete).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledTimes(1);
            expect(rethinkdb.table).toHaveBeenCalledWith(service.schema.table);
        });
    });

});