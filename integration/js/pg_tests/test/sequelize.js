import { Sequelize, DataTypes } from "sequelize";
import assert from "assert";

const PGDOG_HOST = "127.0.0.1";
const PGDOG_PORT = 6432;

function createSequelize(database = "pgdog", opts = {}) {
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
// Regular (non-sharded) ORM tests
// ---------------------------------------------------------------------------

describe("Sequelize regular ORM", function () {
  let seq;
  let Item;

  before(async function () {
    seq = createSequelize();

    Item = seq.define(
      "Item",
      {
        name: { type: DataTypes.STRING, allowNull: false },
        quantity: { type: DataTypes.INTEGER, defaultValue: 0 },
      },
      { tableName: "sequelize_items", timestamps: true }
    );

    await Item.sync({ force: true });
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "sequelize_items"');
    await seq.close();
  });

  it("authenticate", async function () {
    await seq.authenticate();
  });

  it("create and findByPk", async function () {
    const item = await Item.create({ name: "widget", quantity: 5 });
    assert.ok(item.id);

    const found = await Item.findByPk(item.id);
    assert.strictEqual(found.name, "widget");
    assert.strictEqual(found.quantity, 5);
  });

  it("findAll", async function () {
    await Item.create({ name: "gadget", quantity: 3 });
    const items = await Item.findAll({ where: { name: "gadget" } });
    assert.ok(items.length >= 1);
    assert.strictEqual(items[0].name, "gadget");
  });

  it("update", async function () {
    const item = await Item.create({ name: "gizmo", quantity: 1 });
    await item.update({ quantity: 10 });

    const found = await Item.findByPk(item.id);
    assert.strictEqual(found.quantity, 10);
  });

  it("destroy", async function () {
    const item = await Item.create({ name: "doomed", quantity: 0 });
    const id = item.id;
    await item.destroy();

    const found = await Item.findByPk(id);
    assert.strictEqual(found, null);
  });

  it("bulkCreate and count", async function () {
    const before = await Item.count();
    await Item.bulkCreate([
      { name: "bulk_a", quantity: 1 },
      { name: "bulk_b", quantity: 2 },
      { name: "bulk_c", quantity: 3 },
    ]);
    const after = await Item.count();
    assert.strictEqual(after, before + 3);
  });

  it("transaction commit", async function () {
    const t = await seq.transaction();
    const item = await Item.create(
      { name: "tx_item", quantity: 42 },
      { transaction: t }
    );
    await t.commit();

    const found = await Item.findByPk(item.id);
    assert.strictEqual(found.name, "tx_item");
  });

  it("transaction rollback", async function () {
    const t = await seq.transaction();
    const item = await Item.create(
      { name: "rollback_item", quantity: 99 },
      { transaction: t }
    );
    const id = item.id;
    await t.rollback();

    const found = await Item.findByPk(id);
    assert.strictEqual(found, null);
  });
});

// ---------------------------------------------------------------------------
// Multi-statement SET tests
// ---------------------------------------------------------------------------

describe("Sequelize multi-statement SET", function () {
  it("multi-statement SET via raw query", async function () {
    const seq = createSequelize();
    const t = await seq.transaction();

    await seq.query(
      "SET statement_timeout TO '30s'; SET lock_timeout TO '10s'",
      { transaction: t }
    );

    const stResult = await seq.query("SHOW statement_timeout", {
      transaction: t,
    });
    assert.strictEqual(stResult[0].statement_timeout, "30s");

    const ltResult = await seq.query("SHOW lock_timeout", {
      transaction: t,
    });
    assert.strictEqual(ltResult[0].lock_timeout, "10s");

    await t.commit();
    await seq.close();
  });

  it("multi-statement SET with timezone interval", async function () {
    const seq = createSequelize();
    const t = await seq.transaction();

    await seq.query(
      "SET client_min_messages TO warning;SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE",
      { transaction: t }
    );

    const cmResult = await seq.query("SHOW client_min_messages", {
      transaction: t,
    });
    assert.strictEqual(cmResult[0].client_min_messages, "warning");

    const tzResult = await seq.query("SHOW timezone", { transaction: t });
    const tz = tzResult[0].TimeZone || tzResult[0].timezone;
    assert.ok(
      tz.includes("00:00") || tz === "UTC" || tz === "Etc/UTC",
      `expected UTC-equivalent timezone, got ${tz}`
    );

    await t.commit();
    await seq.close();
  });

  it("mixed SET and non-SET returns error", async function () {
    const seq = createSequelize();

    try {
      await seq.query("SET statement_timeout TO '10s'; SELECT 1");
      assert.fail("expected error for mixed SET + SELECT");
    } catch (err) {
      assert.ok(
        err.message.includes(
          "multi-statement queries cannot mix SET with other commands"
        ),
        `unexpected error: ${err.message}`
      );
    }

    const [rows] = await seq.query("SELECT 1 AS val");
    assert.strictEqual(rows[0].val, 1);

    await seq.close();
  });
});

// ---------------------------------------------------------------------------
// Sharded ORM tests
// ---------------------------------------------------------------------------

describe("Sequelize sharded ORM", function () {
  let seq;
  let Sharded;

  before(async function () {
    seq = createSequelize("pgdog_sharded");

    Sharded = seq.define(
      "Sharded",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: false },
        value: DataTypes.TEXT,
      },
      { tableName: "sharded", timestamps: false }
    );

    await seq.query(
      "CREATE TABLE IF NOT EXISTS sharded (id BIGINT PRIMARY KEY, value TEXT)"
    );
    await seq.query("TRUNCATE TABLE sharded");
  });

  after(async function () {
    await seq.close();
  });

  it("create rows via ORM", async function () {
    for (let i = 0; i < 10; i++) {
      await Sharded.create({ id: i, value: `seq_${i}` });
    }
  });

  it("findByPk routes to correct shard", async function () {
    for (let i = 0; i < 10; i++) {
      const row = await Sharded.findByPk(i);
      assert.ok(row, `row ${i} not found`);
      assert.strictEqual(row.value, `seq_${i}`);
    }
  });

  it("update via ORM", async function () {
    const row = await Sharded.findByPk(0);
    await row.update({ value: "updated_0" });

    const found = await Sharded.findByPk(0);
    assert.strictEqual(found.value, "updated_0");
  });

  it("destroy via ORM", async function () {
    await Sharded.create({ id: 100, value: "to_delete" });
    const row = await Sharded.findByPk(100);
    assert.ok(row);

    await row.destroy();
    const gone = await Sharded.findByPk(100);
    assert.strictEqual(gone, null);
  });

  it("findAll with where clause", async function () {
    const rows = await Sharded.findAll({ where: { id: 5 } });
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].value, "seq_5");
  });
});
