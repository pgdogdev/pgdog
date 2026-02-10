import { Sequelize, DataTypes } from "sequelize";
import assert from "assert";

const PGDOG_HOST = "127.0.0.1";
const PGDOG_PORT = 6432;

function createSequelize(database = "pgdog_sharded", opts = {}) {
  return new Sequelize(database, "pgdog", "pgdog", {
    host: PGDOG_HOST,
    port: PGDOG_PORT,
    dialect: "postgres",
    logging: false,
    pool: { max: 2, min: 1, acquire: 10000, idle: 5000 },
    ...opts,
  });
}

// ---------------------------------------------------------------------------
// Basic sharded CRUD with customer_id
// ---------------------------------------------------------------------------

describe("Sharded CRUD operations", function () {
  let seq;
  let Order;

  before(async function () {
    seq = createSequelize();

    Order = seq.define(
      "Order",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        total: { type: DataTypes.DECIMAL(10, 2) },
        status: { type: DataTypes.STRING },
      },
      { tableName: "orders", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        total DECIMAL(10, 2),
        status TEXT
      )
    `);
    await seq.query("TRUNCATE TABLE orders");
  });

  after(async function () {
    await seq.close();
  });

  it("create orders for multiple customers", async function () {
    const customers = [1, 2, 3, 4, 5];
    let orderId = 1;

    for (const customerId of customers) {
      for (let i = 0; i < 3; i++) {
        await Order.create({
          id: orderId++,
          customer_id: customerId,
          total: 100.0 + i * 10,
          status: "pending",
        });
      }
    }

    const count = await Order.count();
    assert.ok(count >= 15);
  });

  it("findAll by customer_id routes to shard", async function () {
    const orders = await Order.findAll({ where: { customer_id: 3 } });
    assert.strictEqual(orders.length, 3);
    orders.forEach((o) => assert.strictEqual(Number(o.customer_id), 3));
  });

  it("update by customer_id", async function () {
    await Order.update(
      { status: "shipped" },
      { where: { customer_id: 2 } }
    );

    const orders = await Order.findAll({ where: { customer_id: 2 } });
    orders.forEach((o) => assert.strictEqual(o.status, "shipped"));
  });

  it("destroy by customer_id", async function () {
    const beforeCount = await Order.count({ where: { customer_id: 5 } });
    assert.strictEqual(beforeCount, 3);

    await Order.destroy({ where: { customer_id: 5 } });

    const afterCount = await Order.count({ where: { customer_id: 5 } });
    assert.strictEqual(afterCount, 0);
  });

  it("findOne by customer_id", async function () {
    const order = await Order.findOne({ where: { customer_id: 1 } });
    assert.ok(order);
    assert.strictEqual(Number(order.customer_id), 1);
  });
});

// ---------------------------------------------------------------------------
// Sharded transactions
// ---------------------------------------------------------------------------

describe("Sharded transactions", function () {
  let seq;
  let Account;

  before(async function () {
    seq = createSequelize();

    Account = seq.define(
      "Account",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        balance: { type: DataTypes.DECIMAL(10, 2) },
      },
      { tableName: "accounts", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        balance DECIMAL(10, 2)
      )
    `);
    await seq.query("TRUNCATE TABLE accounts");
  });

  after(async function () {
    await seq.close();
  });

  it("transaction commit on single shard", async function () {
    const t = await seq.transaction();

    await Account.create(
      { id: 1, customer_id: 100, balance: 1000.0 },
      { transaction: t }
    );
    await Account.create(
      { id: 2, customer_id: 100, balance: 500.0 },
      { transaction: t }
    );

    await t.commit();

    const accounts = await Account.findAll({ where: { customer_id: 100 } });
    assert.strictEqual(accounts.length, 2);
  });

  it("transaction rollback on single shard", async function () {
    const t = await seq.transaction();

    await Account.create(
      { id: 10, customer_id: 200, balance: 2000.0 },
      { transaction: t }
    );

    await t.rollback();

    const accounts = await Account.findAll({ where: { customer_id: 200 } });
    assert.strictEqual(accounts.length, 0);
  });

  it("update within transaction", async function () {
    await Account.create({ id: 20, customer_id: 300, balance: 100.0 });

    const t = await seq.transaction();

    await Account.update(
      { balance: 200.0 },
      { where: { customer_id: 300 }, transaction: t }
    );

    const inTx = await Account.findOne({
      where: { customer_id: 300 },
      transaction: t,
    });
    assert.strictEqual(parseFloat(inTx.balance), 200.0);

    await t.commit();

    const afterCommit = await Account.findOne({ where: { customer_id: 300 } });
    assert.strictEqual(parseFloat(afterCommit.balance), 200.0);
  });
});

// ---------------------------------------------------------------------------
// Sharded associations
// ---------------------------------------------------------------------------

describe("Sharded associations", function () {
  let seq;
  let Customer, OrderItem;

  before(async function () {
    seq = createSequelize();

    Customer = seq.define(
      "Customer",
      {
        customer_id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        name: { type: DataTypes.STRING },
      },
      { tableName: "customers", timestamps: false }
    );

    OrderItem = seq.define(
      "OrderItem",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        product_name: { type: DataTypes.STRING },
        quantity: { type: DataTypes.INTEGER },
      },
      { tableName: "order_items", timestamps: false }
    );

    Customer.hasMany(OrderItem, { foreignKey: "customer_id", as: "items" });
    OrderItem.belongsTo(Customer, { foreignKey: "customer_id", targetKey: "customer_id", as: "customer" });

    await seq.query(`
      CREATE TABLE IF NOT EXISTS customers (
        customer_id BIGINT PRIMARY KEY,
        name TEXT
      )
    `);
    await seq.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        product_name TEXT,
        quantity INTEGER
      )
    `);
    await seq.query("TRUNCATE TABLE order_items");
    await seq.query("TRUNCATE TABLE customers");
  });

  after(async function () {
    await seq.close();
  });

  it("create customer with items", async function () {
    await Customer.create({ customer_id: 1000, name: "Alice" });
    await OrderItem.create({ id: 1, customer_id: 1000, product_name: "Widget", quantity: 2 });
    await OrderItem.create({ id: 2, customer_id: 1000, product_name: "Gadget", quantity: 1 });

    const items = await OrderItem.findAll({ where: { customer_id: 1000 } });
    assert.strictEqual(items.length, 2);
  });

  it("eager load items for customer", async function () {
    const customer = await Customer.findByPk(1000, {
      include: [{ model: OrderItem, as: "items" }],
    });
    assert.ok(customer);
    assert.strictEqual(customer.items.length, 2);
  });

  it("eager load customer for item", async function () {
    const item = await OrderItem.findOne({
      where: { customer_id: 1000 },
      include: [{ model: Customer, as: "customer" }],
    });
    assert.ok(item);
    assert.strictEqual(item.customer.name, "Alice");
  });
});

// ---------------------------------------------------------------------------
// Sharded aggregates
// ---------------------------------------------------------------------------

describe("Sharded aggregates", function () {
  let seq;
  let Sale;

  before(async function () {
    seq = createSequelize();

    Sale = seq.define(
      "Sale",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        amount: { type: DataTypes.DECIMAL(10, 2) },
        region: { type: DataTypes.STRING },
      },
      { tableName: "sales", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS sales (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        amount DECIMAL(10, 2),
        region TEXT
      )
    `);
    await seq.query("TRUNCATE TABLE sales");

    let id = 1;
    for (const customerId of [10, 20, 30]) {
      for (let i = 0; i < 5; i++) {
        await Sale.create({
          id: id++,
          customer_id: customerId,
          amount: 100.0 + i * 25,
          region: i % 2 === 0 ? "east" : "west",
        });
      }
    }
  });

  after(async function () {
    await seq.close();
  });

  it("sum by customer_id", async function () {
    const total = await Sale.sum("amount", { where: { customer_id: 10 } });
    assert.ok(parseFloat(total) >= 600.0);
  });

  it("count by customer_id", async function () {
    const count = await Sale.count({ where: { customer_id: 20 } });
    assert.strictEqual(count, 5);
  });

  it("max by customer_id", async function () {
    const max = await Sale.max("amount", { where: { customer_id: 30 } });
    assert.strictEqual(parseFloat(max), 200.0);
  });

  it("min by customer_id", async function () {
    const min = await Sale.min("amount", { where: { customer_id: 10 } });
    assert.strictEqual(parseFloat(min), 100.0);
  });
});

// ---------------------------------------------------------------------------
// Sharded data types
// ---------------------------------------------------------------------------

describe("Sharded data types", function () {
  let seq;
  let CustomerData;

  before(async function () {
    seq = createSequelize();

    CustomerData = seq.define(
      "CustomerData",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        metadata: { type: DataTypes.JSONB },
        tags: { type: DataTypes.ARRAY(DataTypes.STRING) },
        created_at: { type: DataTypes.DATE },
        active: { type: DataTypes.BOOLEAN },
      },
      { tableName: "customer_data", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS customer_data (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        metadata JSONB,
        tags TEXT[],
        created_at TIMESTAMP,
        active BOOLEAN
      )
    `);
    await seq.query("TRUNCATE TABLE customer_data");
  });

  after(async function () {
    await seq.close();
  });

  it("JSONB with customer_id", async function () {
    const data = { preferences: { theme: "dark" }, score: 95 };
    await CustomerData.create({
      id: 1,
      customer_id: 500,
      metadata: data,
    });

    const found = await CustomerData.findOne({ where: { customer_id: 500 } });
    assert.deepStrictEqual(found.metadata, data);
  });

  it("array column with customer_id", async function () {
    const tags = ["vip", "early-adopter", "beta"];
    await CustomerData.create({
      id: 2,
      customer_id: 501,
      tags: tags,
    });

    const found = await CustomerData.findOne({ where: { customer_id: 501 } });
    assert.deepStrictEqual(found.tags, tags);
  });

  it("date column with customer_id", async function () {
    const dt = new Date("2024-06-15T12:00:00Z");
    const uniqueId = Date.now();
    await CustomerData.create({
      id: uniqueId,
      customer_id: uniqueId,
      created_at: dt,
    });

    const found = await CustomerData.findOne({ where: { customer_id: uniqueId } });
    assert.ok(found.created_at instanceof Date);
    assert.strictEqual(found.created_at.getUTCFullYear(), 2024);
    assert.strictEqual(found.created_at.getUTCMonth(), 5);
    assert.strictEqual(found.created_at.getUTCDate(), 15);
  });

  it("boolean column with customer_id", async function () {
    await CustomerData.create({ id: 4, customer_id: 503, active: true });
    await CustomerData.create({ id: 5, customer_id: 504, active: false });

    const active = await CustomerData.findOne({ where: { customer_id: 503 } });
    const inactive = await CustomerData.findOne({ where: { customer_id: 504 } });

    assert.strictEqual(active.active, true);
    assert.strictEqual(inactive.active, false);
  });

  it("JSONB query with customer_id", async function () {
    const [rows] = await seq.query(`
      SELECT * FROM customer_data
      WHERE customer_id = 500
      AND metadata->>'score' = '95'
    `);
    assert.strictEqual(rows.length, 1);
  });
});

// ---------------------------------------------------------------------------
// Sharded bulk operations
// ---------------------------------------------------------------------------

describe("Sharded bulk operations", function () {
  let seq;
  let Event;

  before(async function () {
    seq = createSequelize();

    Event = seq.define(
      "Event",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        event_type: { type: DataTypes.STRING },
        payload: { type: DataTypes.TEXT },
      },
      { tableName: "events", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS events (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        event_type TEXT,
        payload TEXT
      )
    `);
    await seq.query("TRUNCATE TABLE events");
  });

  after(async function () {
    await seq.close();
  });

  it("bulkCreate for single customer", async function () {
    const events = [];
    for (let i = 0; i < 10; i++) {
      events.push({
        id: i + 1,
        customer_id: 600,
        event_type: "click",
        payload: `payload_${i}`,
      });
    }

    await Event.bulkCreate(events);

    const count = await Event.count({ where: { customer_id: 600 } });
    assert.strictEqual(count, 10);
  });

  it("bulkCreate for multiple customers", async function () {
    const events = [];
    let id = 100;
    for (const customerId of [700, 701, 702]) {
      for (let i = 0; i < 3; i++) {
        events.push({
          id: id++,
          customer_id: customerId,
          event_type: "view",
          payload: `view_${i}`,
        });
      }
    }

    await Event.bulkCreate(events);

    for (const customerId of [700, 701, 702]) {
      const count = await Event.count({ where: { customer_id: customerId } });
      assert.strictEqual(count, 3);
    }
  });

  it("destroy all for customer", async function () {
    await Event.destroy({ where: { customer_id: 600 } });

    const count = await Event.count({ where: { customer_id: 600 } });
    assert.strictEqual(count, 0);
  });
});

// ---------------------------------------------------------------------------
// Sharded ordering and pagination
// ---------------------------------------------------------------------------

describe("Sharded ordering and pagination", function () {
  let seq;
  let LogEntry;

  before(async function () {
    seq = createSequelize();

    LogEntry = seq.define(
      "LogEntry",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        level: { type: DataTypes.STRING },
        message: { type: DataTypes.TEXT },
        created_at: { type: DataTypes.DATE },
      },
      { tableName: "log_entries", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS log_entries (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        level TEXT,
        message TEXT,
        created_at TIMESTAMP
      )
    `);
    await seq.query("TRUNCATE TABLE log_entries");

    const now = new Date();
    for (let i = 0; i < 20; i++) {
      await LogEntry.create({
        id: i + 1,
        customer_id: 800,
        level: i % 3 === 0 ? "error" : "info",
        message: `log message ${i}`,
        created_at: new Date(now.getTime() - i * 60000),
      });
    }
  });

  after(async function () {
    await seq.close();
  });

  it("order by created_at DESC", async function () {
    const logs = await LogEntry.findAll({
      where: { customer_id: 800 },
      order: [["created_at", "DESC"]],
      limit: 5,
    });

    assert.strictEqual(logs.length, 5);
    for (let i = 1; i < logs.length; i++) {
      assert.ok(logs[i - 1].created_at >= logs[i].created_at);
    }
  });

  it("order by id ASC with limit", async function () {
    const logs = await LogEntry.findAll({
      where: { customer_id: 800 },
      order: [["id", "ASC"]],
      limit: 3,
    });

    assert.strictEqual(logs.length, 3);
    assert.strictEqual(Number(logs[0].id), 1);
    assert.strictEqual(Number(logs[1].id), 2);
    assert.strictEqual(Number(logs[2].id), 3);
  });

  it("pagination with offset", async function () {
    const page1 = await LogEntry.findAll({
      where: { customer_id: 800 },
      order: [["id", "ASC"]],
      limit: 5,
      offset: 0,
    });
    const page2 = await LogEntry.findAll({
      where: { customer_id: 800 },
      order: [["id", "ASC"]],
      limit: 5,
      offset: 5,
    });

    assert.strictEqual(page1.length, 5);
    assert.strictEqual(page2.length, 5);
    assert.strictEqual(Number(page1[4].id) + 1, Number(page2[0].id));
  });

  it("filter and order combined", async function () {
    const errors = await LogEntry.findAll({
      where: { customer_id: 800, level: "error" },
      order: [["id", "DESC"]],
    });

    assert.ok(errors.length > 0);
    errors.forEach((e) => assert.strictEqual(e.level, "error"));
  });
});

// ---------------------------------------------------------------------------
// Cross-shard queries (scatter-gather)
// ---------------------------------------------------------------------------

describe("Cross-shard queries", function () {
  let seq;
  let Metric;

  before(async function () {
    seq = createSequelize();

    Metric = seq.define(
      "Metric",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        customer_id: { type: DataTypes.BIGINT, allowNull: false },
        name: { type: DataTypes.STRING },
        value: { type: DataTypes.INTEGER },
      },
      { tableName: "metrics", timestamps: false }
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS metrics (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        name TEXT,
        value INTEGER
      )
    `);
    await seq.query("TRUNCATE TABLE metrics");

    let id = 1;
    for (const customerId of [900, 901, 902, 903, 904]) {
      for (let i = 0; i < 4; i++) {
        await Metric.create({
          id: id++,
          customer_id: customerId,
          name: `metric_${i}`,
          value: customerId + i,
        });
      }
    }
  });

  after(async function () {
    await seq.close();
  });

  it("count all rows across shards", async function () {
    const count = await Metric.count();
    assert.ok(count >= 20);
  });

  it("findAll without customer_id filter", async function () {
    const metrics = await Metric.findAll({
      order: [["id", "ASC"]],
    });
    assert.ok(metrics.length >= 20);
  });

  it("findAll with limit across shards", async function () {
    const metrics = await Metric.findAll({
      order: [["id", "ASC"]],
      limit: 10,
    });
    assert.strictEqual(metrics.length, 10);
  });

  it("sum across all shards", async function () {
    const total = await Metric.sum("value");
    assert.ok(total > 0);
  });

  it("findAll with name filter across shards", async function () {
    const metrics = await Metric.findAll({
      where: { name: "metric_0" },
    });
    assert.ok(metrics.length >= 5);
  });
});
