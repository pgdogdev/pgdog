import postgres from "postgres";
import pg from "pg";
import assert from "assert";

const ADMIN_URL = "postgresql://admin:pgdog@127.0.0.1:6432/admin";

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
        const expectedTotal = 100.0 + i * 10;
        const [row] = await sql`
          INSERT INTO pjs_sh_orders_9k (id, customer_id, total, status)
          VALUES (${orderId}, ${customerId}, ${expectedTotal}, ${"pending"})
          RETURNING *
        `;
        assert.strictEqual(row.id, orderId.toString());
        assert.strictEqual(Number(row.customer_id), customerId);
        assert.strictEqual(Number(row.total), expectedTotal);
        assert.strictEqual(row.status, "pending");
        orderId++;
      }
    }

    // Read everything back to confirm all 25 rows landed on the right
    // shard and survived the round-trip.
    const all = await sql`SELECT * FROM pjs_sh_orders_9k ORDER BY id`;
    assert.strictEqual(all.length, 25);
    all.forEach((r, idx) => {
      const expectedId = idx + 1;
      const expectedCustomer = Math.floor(idx / 5) + 1;
      const expectedTotal = 100.0 + (idx % 5) * 10;
      assert.strictEqual(Number(r.id), expectedId);
      assert.strictEqual(Number(r.customer_id), expectedCustomer);
      assert.strictEqual(Number(r.total), expectedTotal);
      assert.strictEqual(r.status, "pending");
    });
  });

  it("select by customer_id routes to shard", async function () {
    const rows = await sql`
      SELECT * FROM pjs_sh_orders_9k WHERE customer_id = ${3} ORDER BY id
    `;
    // customer 3 owns ids 11..15 — verify the exact set, not just a count.
    assert.strictEqual(rows.length, 5);
    rows.forEach((r, i) => {
      assert.strictEqual(Number(r.id), 11 + i);
      assert.strictEqual(Number(r.customer_id), 3);
      assert.strictEqual(Number(r.total), 100.0 + i * 10);
      assert.strictEqual(r.status, "pending");
    });
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
    // The other fields must be untouched.
    assert.strictEqual(Number(row.id), 1);
    assert.strictEqual(Number(row.customer_id), 1);
    assert.strictEqual(Number(row.total), 100.0);

    // The sibling rows owned by customer 1 must remain pending.
    const siblings = await sql`
      SELECT id, status FROM pjs_sh_orders_9k
      WHERE customer_id = ${1} AND id != ${1} ORDER BY id
    `;
    assert.strictEqual(siblings.length, 4);
    siblings.forEach((r) => assert.strictEqual(r.status, "pending"));
  });

  it("delete by customer_id", async function () {
    const beforeRows = await sql`
      SELECT id FROM pjs_sh_orders_9k WHERE customer_id = ${5} ORDER BY id
    `;
    assert.deepStrictEqual(
      beforeRows.map((r) => Number(r.id)),
      [21, 22, 23, 24, 25],
    );

    await sql`DELETE FROM pjs_sh_orders_9k WHERE customer_id = ${5} AND id = ${25}`;

    const afterRows = await sql`
      SELECT id FROM pjs_sh_orders_9k WHERE customer_id = ${5} ORDER BY id
    `;
    assert.deepStrictEqual(
      afterRows.map((r) => Number(r.id)),
      [21, 22, 23, 24],
    );

    // And id=25 must be gone everywhere, not just on customer 5's shard.
    const lookup = await sql`SELECT id FROM pjs_sh_orders_9k WHERE id = ${25}`;
    assert.strictEqual(lookup.length, 0);
  });

  it("cross-shard count", async function () {
    const [{ count }] =
      await sql`SELECT COUNT(*)::int AS count FROM pjs_sh_orders_9k`;
    assert.strictEqual(count, 24);
  });

  it("cross-shard select all", async function () {
    const rows = await sql`SELECT * FROM pjs_sh_orders_9k ORDER BY id`;
    assert.strictEqual(rows.length, 24);
    // ids 1..24 — id 25 was deleted.
    assert.deepStrictEqual(
      rows.map((r) => Number(r.id)),
      Array.from({ length: 24 }, (_, i) => i + 1),
    );
    // The shipped row from the update test must still be the shipped one.
    const shipped = rows.filter((r) => r.status === "shipped");
    assert.strictEqual(shipped.length, 1);
    assert.strictEqual(Number(shipped[0].id), 1);
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
    assert.strictEqual(Number(row.id), 1);
    assert.strictEqual(Number(row.customer_id), 100);
    assert.strictEqual(row.value, "tx_updated");

    // The committed row must also be visible cross-shard.
    const [{ count }] =
      await sql`SELECT COUNT(*)::int AS count FROM pjs_sh_tx_9k WHERE id = ${1}`;
    assert.strictEqual(count, 1);
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

    // And the rolled-back id must not exist on any shard.
    const lookup = await sql`SELECT id FROM pjs_sh_tx_9k WHERE id = ${2}`;
    assert.strictEqual(lookup.length, 0);
  });
});

describe("postgres.js sharded unsafe (simple protocol)", function () {
  // Default `extended` mode — the split Parse/Describe/Sync + Bind/Execute/Sync
  // path that postgres.js uses for unsafe/parameterised queries must work
  // without needing `extended_anonymous`.
  before(async function () {
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
  });

  it("unsafe select by customer_id", async function () {
    const rows = await sql.unsafe(
      "SELECT * FROM pjs_sh_unsafe_9k WHERE customer_id = $1 ORDER BY id",
      [1],
    );
    // ids 1, 4, 7 hash to customer 1 ((i % 3) + 1 == 1 → i ∈ {0, 3, 6, 9}).
    const expectedIds = [0, 3, 6, 9];
    assert.strictEqual(rows.length, expectedIds.length);
    rows.forEach((r, idx) => {
      assert.strictEqual(Number(r.id), expectedIds[idx]);
      assert.strictEqual(Number(r.customer_id), 1);
      assert.strictEqual(r.value, "unsafe_" + expectedIds[idx]);
    });
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
    assert.strictEqual(Number(rows[0].id), 100);
    assert.strictEqual(Number(rows[0].customer_id), 1);
    assert.strictEqual(rows[0].value, "unsafe_100");

    // Read it back to confirm it actually persisted on the right shard.
    const lookup = await sql.unsafe(
      "SELECT * FROM pjs_sh_unsafe_9k WHERE id = $1 AND customer_id = $2",
      [100, 1],
    );
    assert.strictEqual(lookup.length, 1);
    assert.strictEqual(lookup[0].value, "unsafe_100");
  });
});

describe("postgres.js sharded prepare: false", function () {
  // Default `extended` mode — `prepare: false` makes postgres.js use the
  // unnamed prepared statement pattern (Parse/Describe/Sync, then
  // Bind/Execute/Sync). PgDog needs to inject the saved Parse before the
  // Bind/Execute so the routed shard can bind to it.
  before(async function () {
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
      await sqlNoPrepare`SELECT * FROM pjs_sh_noprep_9k WHERE customer_id = ${1} ORDER BY id`;
    const expectedIds = [0, 3, 6, 9];
    assert.strictEqual(rows.length, expectedIds.length);
    rows.forEach((r, idx) => {
      assert.strictEqual(Number(r.id), expectedIds[idx]);
      assert.strictEqual(Number(r.customer_id), 1);
      assert.strictEqual(r.value, "noprep_" + expectedIds[idx]);
    });
  });

  it("cross-shard select without named prepare", async function () {
    const rows = await sqlNoPrepare`SELECT * FROM pjs_sh_noprep_9k ORDER BY id`;
    assert.strictEqual(rows.length, 10);
    rows.forEach((r, i) => {
      assert.strictEqual(Number(r.id), i);
      assert.strictEqual(Number(r.customer_id), (i % 3) + 1);
      assert.strictEqual(r.value, "noprep_" + i);
    });
  });

  it("insert with RETURNING without named prepare", async function () {
    const [row] = await sqlNoPrepare`
      INSERT INTO pjs_sh_noprep_9k (id, customer_id, value)
      VALUES (${200}, ${1}, ${"noprep_200"})
      RETURNING *
    `;
    assert.strictEqual(Number(row.id), 200);
    assert.strictEqual(Number(row.customer_id), 1);
    assert.strictEqual(row.value, "noprep_200");

    // Confirm the row landed on the right shard by reading it back through
    // the same unnamed-prepare path.
    const lookup =
      await sqlNoPrepare`SELECT * FROM pjs_sh_noprep_9k WHERE id = ${200} AND customer_id = ${1}`;
    assert.strictEqual(lookup.length, 1);
    assert.strictEqual(lookup[0].value, "noprep_200");
  });

  it("repeated queries reuse unnamed prepare", async function () {
    // The same prepared-statement slot is reused across iterations but the
    // bind values change, so each call must route to a different shard and
    // return only its rows.
    const expectedByCustomer = {
      1: [0, 3, 6, 9, 200],
      2: [1, 4, 7],
      3: [2, 5, 8],
    };
    for (let i = 0; i < 9; i++) {
      const customerId = (i % 3) + 1;
      const rows =
        await sqlNoPrepare`SELECT id FROM pjs_sh_noprep_9k WHERE customer_id = ${customerId} ORDER BY id`;
      assert.deepStrictEqual(
        rows.map((r) => Number(r.id)),
        expectedByCustomer[customerId],
        `customer ${customerId} on iteration ${i}`,
      );
    }
  });
});
