import postgres from "postgres";
import pg from "pg";
import assert from "assert";

const ADMIN_URL = "postgresql://admin:pgdog@127.0.0.1:6432/admin";

async function adminSet(setting, value) {
  const client = new pg.Client({ connectionString: ADMIN_URL });
  await client.connect();
  await client.query(`SET ${setting} TO '${value}'`);
  await client.end();
}

const sql = postgres("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded");
const sqlNoPrepare = postgres(
  "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded",
  { prepare: false },
);

async function setupAdmin() {
  const client = new pg.Client({ connectionString: ADMIN_URL });
  await client.connect();
  await client.query("SET rewrite_enabled TO true");
  await client.query("SET rewrite_split_inserts TO rewrite");
  await client.query("SET reload_schema_on_ddl TO true");
  await client.end();
}

async function teardownAdmin() {
  const client = new pg.Client({ connectionString: ADMIN_URL });
  await client.connect();
  await client.query("RELOAD");
  await client.end();
}

before(async function () {
  await setupAdmin();
});

after(async function () {
  await teardownAdmin();
  await sql.end();
  await sqlNoPrepare.end();
});

describe("postgres.js sharded CRUD", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_sh_orders_9k (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      total DECIMAL(10, 2),
      status TEXT
    )`;
    await sql`TRUNCATE TABLE pjs_sh_orders_9k`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_sh_orders_9k`;
  });

  it("insert with RETURNING", async function () {
    const customers = [1, 2, 3, 4, 5];
    let orderId = 1;
    for (const customerId of customers) {
      for (let i = 0; i < 5; i++) {
        const [row] = await sql`
          INSERT INTO pjs_sh_orders_9k (id, customer_id, total, status)
          VALUES (${orderId}, ${customerId}, ${100.0 + i * 10}, ${"pending"})
          RETURNING *
        `;
        assert.strictEqual(row.id, orderId.toString());
        assert.strictEqual(Number(row.customer_id), customerId);
        orderId++;
      }
    }
  });

  it("select by customer_id routes to shard", async function () {
    const rows = await sql`
      SELECT * FROM pjs_sh_orders_9k WHERE customer_id = ${3}
    `;
    assert.strictEqual(rows.length, 5);
    rows.forEach((r) => assert.strictEqual(Number(r.customer_id), 3));
  });

  it("update by customer_id", async function () {
    await sql`
      UPDATE pjs_sh_orders_9k SET status = 'shipped'
      WHERE customer_id = ${1} AND id = ${1}
    `;
    const [row] = await sql`
      SELECT * FROM pjs_sh_orders_9k WHERE customer_id = ${1} AND id = ${1}
    `;
    assert.strictEqual(row.status, "shipped");
  });

  it("delete by customer_id", async function () {
    const beforeRows = await sql`
      SELECT * FROM pjs_sh_orders_9k WHERE customer_id = ${5}
    `;
    assert.strictEqual(beforeRows.length, 5);

    await sql`DELETE FROM pjs_sh_orders_9k WHERE customer_id = ${5} AND id = ${25}`;
    const afterRows = await sql`
      SELECT * FROM pjs_sh_orders_9k WHERE customer_id = ${5}
    `;
    assert.strictEqual(afterRows.length, 4);
  });

  it("cross-shard count", async function () {
    const [{ count }] =
      await sql`SELECT COUNT(*)::int AS count FROM pjs_sh_orders_9k`;
    assert.strictEqual(count, 24);
  });

  it("cross-shard select all", async function () {
    const rows = await sql`SELECT * FROM pjs_sh_orders_9k ORDER BY id`;
    assert.strictEqual(rows.length, 24);
  });
});

describe("postgres.js sharded transactions", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_sh_tx_9k (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      value TEXT
    )`;
    await sql`TRUNCATE TABLE pjs_sh_tx_9k`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_sh_tx_9k`;
  });

  it("transaction within shard", async function () {
    await sql.begin(async (tx) => {
      await tx`INSERT INTO pjs_sh_tx_9k (id, customer_id, value)
               VALUES (${1}, ${100}, ${"tx_val"})`;
      await tx`UPDATE pjs_sh_tx_9k SET value = 'tx_updated'
               WHERE id = ${1} AND customer_id = ${100}`;
    });

    const [row] = await sql`
      SELECT * FROM pjs_sh_tx_9k WHERE id = ${1} AND customer_id = ${100}
    `;
    assert.strictEqual(row.value, "tx_updated");
  });

  it("transaction rollback within shard", async function () {
    try {
      await sql.begin(async (tx) => {
        await tx`INSERT INTO pjs_sh_tx_9k (id, customer_id, value)
                 VALUES (${2}, ${200}, ${"will_rollback"})`;
        throw new Error("force rollback");
      });
    } catch (e) {
      assert.strictEqual(e.message, "force rollback");
    }

    const rows = await sql`
      SELECT * FROM pjs_sh_tx_9k WHERE id = ${2} AND customer_id = ${200}
    `;
    assert.strictEqual(rows.length, 0);
  });
});

describe("postgres.js sharded unsafe (simple protocol)", function () {
  before(async function () {
    await adminSet("prepared_statements", "extended_anonymous");
    await sql`CREATE TABLE IF NOT EXISTS pjs_sh_unsafe_9k (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      value TEXT
    )`;
    await sql`TRUNCATE TABLE pjs_sh_unsafe_9k`;
    for (let i = 0; i < 10; i++) {
      await sql`INSERT INTO pjs_sh_unsafe_9k (id, customer_id, value) VALUES (${i}, ${(i % 3) + 1}, ${"unsafe_" + i})`;
    }
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_sh_unsafe_9k`;
    await adminSet("prepared_statements", "extended");
  });

  it("unsafe select by customer_id", async function () {
    const rows = await sql.unsafe(
      "SELECT * FROM pjs_sh_unsafe_9k WHERE customer_id = $1",
      [1],
    );
    assert.ok(rows.length > 0);
    rows.forEach((r) => assert.strictEqual(Number(r.customer_id), 1));
  });

  it("unsafe cross-shard count", async function () {
    const rows = await sql.unsafe(
      "SELECT COUNT(*)::int AS count FROM pjs_sh_unsafe_9k",
    );
    assert.strictEqual(rows[0].count, 10);
  });

  it("unsafe insert with RETURNING", async function () {
    const rows = await sql.unsafe(
      "INSERT INTO pjs_sh_unsafe_9k (id, customer_id, value) VALUES ($1, $2, $3) RETURNING *",
      [100, 1, "unsafe_100"],
    );
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].value, "unsafe_100");
  });
});

describe("postgres.js sharded prepare: false", function () {
  before(async function () {
    await adminSet("prepared_statements", "extended_anonymous");
    await sqlNoPrepare`CREATE TABLE IF NOT EXISTS pjs_sh_noprep_9k (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      value TEXT
    )`;
    await sqlNoPrepare`TRUNCATE TABLE pjs_sh_noprep_9k`;
    for (let i = 0; i < 10; i++) {
      await sqlNoPrepare`INSERT INTO pjs_sh_noprep_9k (id, customer_id, value) VALUES (${i}, ${(i % 3) + 1}, ${"noprep_" + i})`;
    }
  });

  after(async function () {
    await sqlNoPrepare`DROP TABLE IF EXISTS pjs_sh_noprep_9k`;
  });

  it("select by customer_id without named prepare", async function () {
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_sh_noprep_9k WHERE customer_id = ${1}`;
    assert.ok(rows.length > 0);
    rows.forEach((r) => assert.strictEqual(Number(r.customer_id), 1));
  });

  it("cross-shard select without named prepare", async function () {
    const rows = await sqlNoPrepare`SELECT * FROM pjs_sh_noprep_9k ORDER BY id`;
    assert.strictEqual(rows.length, 10);
  });

  it("insert with RETURNING without named prepare", async function () {
    const [row] = await sqlNoPrepare`
      INSERT INTO pjs_sh_noprep_9k (id, customer_id, value)
      VALUES (${200}, ${1}, ${"noprep_200"})
      RETURNING *
    `;
    assert.strictEqual(row.value, "noprep_200");
  });

  it("repeated queries reuse unnamed prepare", async function () {
    for (let i = 0; i < 5; i++) {
      const rows =
        await sqlNoPrepare`SELECT * FROM pjs_sh_noprep_9k WHERE customer_id = ${(i % 3) + 1}`;
      assert.ok(rows.length > 0);
    }
  });
});
