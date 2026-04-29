import postgres from "postgres";
import pg from "pg";
import assert from "assert";

const ADMIN_URL = "postgresql://admin:pgdog@127.0.0.1:6432/admin";

const sql = postgres("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog");
const sqlNoPrepare = postgres("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog", {
  prepare: false,
});

async function adminSet(setting, value) {
  const client = new pg.Client({ connectionString: ADMIN_URL });
  await client.connect();
  await client.query(`SET ${setting} TO '${value}'`);
  await client.end();
}

after(async function () {
  await sql.end();
  await sqlNoPrepare.end();
});

describe("postgres.js basic", function () {
  it("can connect and query", async function () {
    const [row] = await sql`SELECT 1 AS one`;
    assert.strictEqual(row.one, 1);
  });

  it("parameterized query", async function () {
    const value = 42;
    const [row] = await sql`SELECT ${value}::bigint AS num`;
    assert.strictEqual(row.num, "42");
  });

  it("multiple rows", async function () {
    const rows = await sql`SELECT generate_series(1, 5)::int AS n ORDER BY n`;
    assert.strictEqual(rows.length, 5);
    assert.strictEqual(rows[0].n, 1);
    assert.strictEqual(rows[4].n, 5);
  });
});

describe("postgres.js CRUD", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_items_9k (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      quantity INT DEFAULT 0
    )`;
    await sql`TRUNCATE TABLE pjs_items_9k`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_items_9k`;
  });

  it("insert and select", async function () {
    const [item] =
      await sql`INSERT INTO pjs_items_9k (name, quantity) VALUES ('widget', 5) RETURNING *`;
    assert.ok(item.id);
    assert.strictEqual(item.name, "widget");
    assert.strictEqual(item.quantity, 5);

    const [found] = await sql`SELECT * FROM pjs_items_9k WHERE id = ${item.id}`;
    assert.strictEqual(found.name, "widget");
  });

  it("update", async function () {
    const [item] =
      await sql`INSERT INTO pjs_items_9k (name, quantity) VALUES ('gizmo', 1) RETURNING *`;
    await sql`UPDATE pjs_items_9k SET quantity = 10 WHERE id = ${item.id}`;

    const [found] = await sql`SELECT * FROM pjs_items_9k WHERE id = ${item.id}`;
    assert.strictEqual(found.quantity, 10);
  });

  it("delete", async function () {
    const [item] =
      await sql`INSERT INTO pjs_items_9k (name, quantity) VALUES ('doomed', 0) RETURNING *`;
    await sql`DELETE FROM pjs_items_9k WHERE id = ${item.id}`;

    const rows = await sql`SELECT * FROM pjs_items_9k WHERE id = ${item.id}`;
    assert.strictEqual(rows.length, 0);
  });

  it("bulk insert and count", async function () {
    await sql`TRUNCATE TABLE pjs_items_9k`;
    for (const item of [
      { name: "a", quantity: 1 },
      { name: "b", quantity: 2 },
      { name: "c", quantity: 3 },
    ]) {
      await sql`INSERT INTO pjs_items_9k (name, quantity) VALUES (${item.name}, ${item.quantity})`;
    }

    const [{ count }] =
      await sql`SELECT COUNT(*)::int AS count FROM pjs_items_9k`;
    assert.strictEqual(count, 3);
  });
});

describe("postgres.js transactions", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_tx_9k (
      id SERIAL PRIMARY KEY,
      value TEXT
    )`;
    await sql`TRUNCATE TABLE pjs_tx_9k`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_tx_9k`;
  });

  it("transaction commit", async function () {
    const [item] = await sql.begin(async (tx) => {
      return await tx`INSERT INTO pjs_tx_9k (value) VALUES ('committed') RETURNING *`;
    });

    const [found] = await sql`SELECT * FROM pjs_tx_9k WHERE id = ${item.id}`;
    assert.strictEqual(found.value, "committed");
  });

  it("transaction rollback", async function () {
    let insertedId;
    try {
      await sql.begin(async (tx) => {
        const [item] =
          await tx`INSERT INTO pjs_tx_9k (value) VALUES ('rollback_me') RETURNING *`;
        insertedId = item.id;
        throw new Error("force rollback");
      });
    } catch (e) {
      assert.strictEqual(e.message, "force rollback");
    }

    const rows = await sql`SELECT * FROM pjs_tx_9k WHERE id = ${insertedId}`;
    assert.strictEqual(rows.length, 0);
  });
});

describe("postgres.js data types", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_types_9k (
      id SERIAL PRIMARY KEY,
      text_val TEXT,
      int_val INT,
      float_val DOUBLE PRECISION,
      bool_val BOOLEAN,
      json_val JSONB,
      arr_val INT[],
      ts_val TIMESTAMPTZ
    )`;
    await sql`TRUNCATE TABLE pjs_types_9k`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_types_9k`;
  });

  it("text, int, float, bool", async function () {
    const [row] = await sql`
      INSERT INTO pjs_types_9k (text_val, int_val, float_val, bool_val)
      VALUES (${"hello"}, ${42}, ${3.14}, ${true})
      RETURNING *
    `;
    assert.strictEqual(row.text_val, "hello");
    assert.strictEqual(row.int_val, 42);
    assert.ok(Math.abs(row.float_val - 3.14) < 0.001);
    assert.strictEqual(row.bool_val, true);
  });

  it("jsonb", async function () {
    const data = { key: "value", nested: { a: 1 } };
    const [row] = await sql`
      INSERT INTO pjs_types_9k (json_val)
      VALUES (${sql.json(data)})
      RETURNING *
    `;
    assert.deepStrictEqual(row.json_val, data);
  });

  it("integer array", async function () {
    const arr = [1, 2, 3];
    const [row] = await sql`
      INSERT INTO pjs_types_9k (arr_val)
      VALUES (${arr})
      RETURNING *
    `;
    assert.deepStrictEqual(row.arr_val, [1, 2, 3]);
  });

  it("timestamptz", async function () {
    const now = new Date();
    const [row] = await sql`
      INSERT INTO pjs_types_9k (ts_val)
      VALUES (${now})
      RETURNING *
    `;
    assert.ok(row.ts_val instanceof Date);
    assert.ok(Math.abs(row.ts_val.getTime() - now.getTime()) < 1000);
  });
});

describe("postgres.js prepared statements", function () {
  it("reuses prepared statement", async function () {
    for (let i = 0; i < 5; i++) {
      const [row] = await sql`SELECT ${i}::int AS val`;
      assert.strictEqual(row.val, i);
    }
  });
});

describe("postgres.js unsafe (simple protocol)", function () {
  before(async function () {
    await adminSet("prepared_statements", "extended_anonymous");
    await sql`CREATE TABLE IF NOT EXISTS pjs_unsafe_9k (
      id SERIAL PRIMARY KEY,
      name TEXT
    )`;
    await sql`TRUNCATE TABLE pjs_unsafe_9k`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_unsafe_9k`;
    await adminSet("prepared_statements", "extended");
  });

  it("unsafe query", async function () {
    const rows = await sql.unsafe("SELECT 1 AS one");
    assert.strictEqual(rows[0].one, 1);
  });

  it("unsafe insert and select", async function () {
    await sql.unsafe("INSERT INTO pjs_unsafe_9k (name) VALUES ('unsafe_item')");
    const rows = await sql.unsafe(
      "SELECT * FROM pjs_unsafe_9k WHERE name = 'unsafe_item'",
    );
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].name, "unsafe_item");
  });

  it("unsafe with parameters", async function () {
    const rows = await sql.unsafe("SELECT $1::int AS val", [99]);
    assert.strictEqual(rows[0].val, 99);
  });

  it("unsafe multi-statement", async function () {
    const rows = await sql.unsafe("SELECT 1 AS a; SELECT 2 AS b");
    // postgres.js returns the last result for multi-statement
    assert.ok(rows.length >= 1);
  });
});

describe("postgres.js prepare: false (unnamed prepared statements)", function () {
  before(async function () {
    await adminSet("prepared_statements", "extended_anonymous");
    await sqlNoPrepare`CREATE TABLE IF NOT EXISTS pjs_noprep_9k (
      id SERIAL PRIMARY KEY,
      value TEXT
    )`;
    await sqlNoPrepare`TRUNCATE TABLE pjs_noprep_9k`;
  });

  after(async function () {
    await sqlNoPrepare`DROP TABLE IF EXISTS pjs_noprep_9k`;
    await adminSet("prepared_statements", "extended");
  });

  it("tagged template without named prepare", async function () {
    const val = "hello";
    const [row] = await sqlNoPrepare`SELECT ${val}::text AS v`;
    assert.strictEqual(row.v, "hello");
  });

  it("CRUD without named prepare", async function () {
    const [item] =
      await sqlNoPrepare`INSERT INTO pjs_noprep_9k (value) VALUES ('noprep') RETURNING *`;
    assert.ok(item.id);

    const [found] =
      await sqlNoPrepare`SELECT * FROM pjs_noprep_9k WHERE id = ${item.id}`;
    assert.strictEqual(found.value, "noprep");

    await sqlNoPrepare`UPDATE pjs_noprep_9k SET value = 'updated' WHERE id = ${item.id}`;
    const [updated] =
      await sqlNoPrepare`SELECT * FROM pjs_noprep_9k WHERE id = ${item.id}`;
    assert.strictEqual(updated.value, "updated");

    await sqlNoPrepare`DELETE FROM pjs_noprep_9k WHERE id = ${item.id}`;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_noprep_9k WHERE id = ${item.id}`;
    assert.strictEqual(rows.length, 0);
  });

  it("repeated queries use unnamed prepare each time", async function () {
    for (let i = 0; i < 5; i++) {
      const [row] = await sqlNoPrepare`SELECT ${i}::int AS val`;
      assert.strictEqual(row.val, i);
    }
  });
});

describe("postgres.js dynamic fragments", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_dyn_9k (
      id SERIAL PRIMARY KEY,
      name TEXT,
      score INT
    )`;
    await sql`TRUNCATE TABLE pjs_dyn_9k`;
    await sql`INSERT INTO pjs_dyn_9k (name, score) VALUES ('alice', 10), ('bob', 20), ('charlie', 30)`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_dyn_9k`;
  });

  it("dynamic table name", async function () {
    const table = "pjs_dyn_9k";
    const rows = await sql`SELECT * FROM ${sql(table)}`;
    assert.strictEqual(rows.length, 3);
  });

  it("dynamic column names", async function () {
    const columns = ["name", "score"];
    const rows =
      await sql`SELECT ${sql(columns)} FROM pjs_dyn_9k ORDER BY score`;
    assert.strictEqual(rows.length, 3);
    assert.strictEqual(rows[0].name, "alice");
    assert.strictEqual(rows[0].score, 10);
  });

  it("dynamic WHERE column", async function () {
    const col = "name";
    const rows = await sql`SELECT * FROM pjs_dyn_9k WHERE ${sql(col)} = 'bob'`;
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].name, "bob");
  });

  it("dynamic ORDER BY", async function () {
    const orderCol = "score";
    const rows =
      await sql`SELECT * FROM pjs_dyn_9k ORDER BY ${sql(orderCol)} DESC`;
    assert.strictEqual(rows[0].name, "charlie");
  });
});

describe("postgres.js cursors", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_cursor_9k (
      id SERIAL PRIMARY KEY,
      value INT
    )`;
    await sql`TRUNCATE TABLE pjs_cursor_9k`;
    for (let i = 1; i <= 20; i++) {
      await sql`INSERT INTO pjs_cursor_9k (value) VALUES (${i})`;
    }
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_cursor_9k`;
  });

  it("cursor iterates all rows", async function () {
    const collected = [];
    await sql`SELECT * FROM pjs_cursor_9k ORDER BY id`.cursor(5, (rows) => {
      collected.push(...rows);
    });
    assert.strictEqual(collected.length, 20);
    assert.strictEqual(collected[0].value, 1);
    assert.strictEqual(collected[19].value, 20);
  });

  it("cursor with early break", async function () {
    const collected = [];
    await sql`SELECT * FROM pjs_cursor_9k ORDER BY id`.cursor(5, (rows) => {
      collected.push(...rows);
      if (collected.length >= 10) return sql.CLOSE;
    });
    assert.ok(collected.length >= 10);
    assert.ok(collected.length <= 15); // may get one extra batch
  });
});

describe("postgres.js reserve (dedicated connection)", function () {
  it("reserve and release", async function () {
    const reserved = await sql.reserve();
    try {
      const [row] = await reserved`SELECT 1 AS one`;
      assert.strictEqual(row.one, 1);

      // SET on dedicated connection persists
      await reserved`SET statement_timeout TO '5s'`;
      const [timeout] = await reserved`SHOW statement_timeout`;
      assert.strictEqual(timeout.statement_timeout, "5s");
    } finally {
      await reserved.release();
    }
  });

  it("multiple queries on same reserved connection", async function () {
    const reserved = await sql.reserve();
    try {
      await reserved`CREATE TABLE IF NOT EXISTS pjs_reserve_9k (id SERIAL PRIMARY KEY, val TEXT)`;
      await reserved`TRUNCATE TABLE pjs_reserve_9k`;
      await reserved`INSERT INTO pjs_reserve_9k (val) VALUES ('reserved')`;
      const [row] =
        await reserved`SELECT * FROM pjs_reserve_9k WHERE val = 'reserved'`;
      assert.strictEqual(row.val, "reserved");
      await reserved`DROP TABLE IF EXISTS pjs_reserve_9k`;
    } finally {
      await reserved.release();
    }
  });
});

describe("postgres.js sql.array()", function () {
  before(async function () {
    await sql`CREATE TABLE IF NOT EXISTS pjs_arr_9k (
      id SERIAL PRIMARY KEY,
      name TEXT,
      tags TEXT[]
    )`;
    await sql`TRUNCATE TABLE pjs_arr_9k`;
    await sql`INSERT INTO pjs_arr_9k (name, tags) VALUES ('a', ARRAY['x','y']), ('b', ARRAY['y','z']), ('c', ARRAY['x','z'])`;
  });

  after(async function () {
    await sql`DROP TABLE IF EXISTS pjs_arr_9k`;
  });

  it("WHERE id = ANY with sql.array()", async function () {
    const ids = [1, 2];
    const rows =
      await sql`SELECT * FROM pjs_arr_9k WHERE id = ANY(${sql.array(ids, 23)})`;
    assert.strictEqual(rows.length, 2);
  });

  it("array overlap query", async function () {
    const tags = ["x"];
    const rows = await sql`SELECT * FROM pjs_arr_9k WHERE tags && ${tags}`;
    assert.strictEqual(rows.length, 2); // a and c
  });
});

describe("postgres.js LIMIT NULL", function () {
  before(async function () {
    await adminSet("prepared_statements", "extended_anonymous");
    await sqlNoPrepare`CREATE TABLE IF NOT EXISTS pjs_limit_9k (
      id SERIAL PRIMARY KEY,
      value TEXT
    )`;
    await sqlNoPrepare`TRUNCATE TABLE pjs_limit_9k`;
    await sqlNoPrepare`INSERT INTO pjs_limit_9k (value) VALUES ('a'), ('b'), ('c'), ('d'), ('e')`;
  });

  after(async function () {
    await sqlNoPrepare`DROP TABLE IF EXISTS pjs_limit_9k`;
    await adminSet("prepared_statements", "extended");
  });

  it("LIMIT with null parameter returns all rows", async function () {
    const limit = null;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_limit_9k ORDER BY id LIMIT ${limit}`;
    assert.strictEqual(rows.length, 5);
  });

  it("LIMIT with non-null parameter limits rows", async function () {
    const limit = 2;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_limit_9k ORDER BY id LIMIT ${limit}`;
    assert.strictEqual(rows.length, 2);
  });

  it("LIMIT null with WHERE clause and multiple params", async function () {
    const value = "a";
    const limit = null;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_limit_9k WHERE value >= ${value} ORDER BY id LIMIT ${limit}`;
    assert.strictEqual(rows.length, 5);
  });

  it("LIMIT and OFFSET both null", async function () {
    const limit = null;
    const offset = null;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_limit_9k ORDER BY id LIMIT ${limit} OFFSET ${offset}`;
    assert.strictEqual(rows.length, 5);
  });

  it("LIMIT null with OFFSET non-null", async function () {
    const limit = null;
    const offset = 3;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_limit_9k ORDER BY id LIMIT ${limit} OFFSET ${offset}`;
    assert.strictEqual(rows.length, 2);
  });

  it("LIMIT non-null with OFFSET null", async function () {
    const limit = 2;
    const offset = null;
    const rows =
      await sqlNoPrepare`SELECT * FROM pjs_limit_9k ORDER BY id LIMIT ${limit} OFFSET ${offset}`;
    assert.strictEqual(rows.length, 2);
  });
});

describe("postgres.js unsafe stress test (50k unique statements, 5 clients)", function () {
  this.timeout(300000);

  const TIMESTAMPTZ_OID = 1184;
  const timestampType = {
    to: TIMESTAMPTZ_OID,
    from: [TIMESTAMPTZ_OID],
    serialize: (value) =>
      (value instanceof Date ? value : new Date(value)).toISOString(),
    parse: (value) => new Date(value),
  };

  const NUM_CLIENTS = 5;
  const clients = [];

  before(async function () {
    await adminSet("prepared_statements", "extended_anonymous");
    for (let i = 0; i < NUM_CLIENTS; i++) {
      const c = postgres("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog", {
        prepare: false,
        connection: { application_name: `stress_client_${i}` },
        types: { timestamp: timestampType },
      });
      await c.unsafe("SELECT 1"); // warmup
      clients.push(c);
    }
  });

  after(async function () {
    await Promise.all(clients.map((c) => c.end()));
    await adminSet("prepared_statements", "extended");
  });

  it("50k mixed queries (unsafe + tagged template) across 5 clients", async function () {
    const TOTAL_QUERIES = 50000;
    const BATCH_SIZE = 100;

    // Build a query with 1..numParams parameters mixing ints and timestamps.
    function buildQuery(i) {
      const numParams = (i % 8) + 1; // 1 to 8 parameters
      const useTimestamp = i % 3 === 0; // every 3rd query includes a timestamp
      const vals = [];
      const selectParts = [];
      const expected = {};

      for (let k = 0; k < numParams; k++) {
        const paramIdx = k + 1;
        if (useTimestamp && k === numParams - 1) {
          const ts = new Date(1700000000000 + i * 1000);
          vals.push(ts);
          selectParts.push(`$${paramIdx}::timestamptz AS ts_q${i}`);
          expected[`ts_q${i}`] = (val) => {
            const got = val instanceof Date ? val : new Date(val);
            assert.ok(
              !isNaN(got.getTime()),
              `invalid timestamp at query ${i}: ${val}`,
            );
            assert.ok(
              Math.abs(got.getTime() - ts.getTime()) < 60000,
              `timestamp mismatch at query ${i}: expected ~${ts.toISOString()}, got ${got.toISOString()}`,
            );
          };
        } else {
          const intVal = (i + k) * 7 + 1;
          vals.push(intVal);
          selectParts.push(`$${paramIdx}::int * ${k + 1} AS c${k}_q${i}`);
          expected[`c${k}_q${i}`] = intVal * (k + 1);
        }
      }

      const queryText = `SELECT ${selectParts.join(", ")}`;
      return { queryText, vals, expected };
    }

    // Tagged template queries that exercise unnamed prepared statements.
    // These reuse the same SQL text with different parameter values,
    // which is the normal postgres.js pattern with prepare: false.
    function taggedQuery(client, i) {
      const variant = i % 6;
      switch (variant) {
        case 0: {
          // Simple parameterized select
          const val = i * 3 + 1;
          return client`SELECT ${val}::int AS v`.then((rows) => {
            assert.strictEqual(rows[0].v, val);
          });
        }
        case 1: {
          // Multiple parameters
          const a = i % 100;
          const b = i % 50;
          return client`SELECT ${a}::int + ${b}::int AS sum`.then((rows) => {
            assert.strictEqual(rows[0].sum, a + b);
          });
        }
        case 2: {
          // String parameter
          const name = `item_${i}`;
          return client`SELECT ${name}::text AS name`.then((rows) => {
            assert.strictEqual(rows[0].name, name);
          });
        }
        case 3: {
          // Boolean + int parameters
          const flag = i % 2 === 0;
          const num = i % 1000;
          return client`SELECT ${flag}::bool AS flag, ${num}::int AS num`.then(
            (rows) => {
              assert.strictEqual(rows[0].flag, flag);
              assert.strictEqual(rows[0].num, num);
            },
          );
        }
        case 4: {
          // Timestamp parameter
          const ts = new Date(1700000000000 + i * 1000);
          return client`SELECT ${ts}::timestamptz AS ts`.then((rows) => {
            const got =
              rows[0].ts instanceof Date ? rows[0].ts : new Date(rows[0].ts);
            assert.ok(Math.abs(got.getTime() - ts.getTime()) < 60000);
          });
        }
        case 5: {
          // Many parameters (4)
          const a = i % 100,
            b = i % 50,
            c = i % 25,
            d = i % 10;
          return client`SELECT ${a}::int AS a, ${b}::int AS b, ${c}::int AS c, ${d}::int AS d`.then(
            (rows) => {
              assert.strictEqual(rows[0].a, a);
              assert.strictEqual(rows[0].b, b);
              assert.strictEqual(rows[0].c, c);
              assert.strictEqual(rows[0].d, d);
            },
          );
        }
      }
    }

    let completed = 0;
    const errors = [];

    for (
      let batchStart = 0;
      batchStart < TOTAL_QUERIES;
      batchStart += BATCH_SIZE
    ) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, TOTAL_QUERIES);
      const promises = [];

      for (let i = batchStart; i < batchEnd; i++) {
        const client = clients[i % NUM_CLIENTS];
        let p;

        if (i % 2 === 0) {
          // Even: unsafe query (unique query text each time)
          const { queryText, vals, expected } = buildQuery(i);
          p = client.unsafe(queryText, vals).then((rows) => {
            for (const [col, val] of Object.entries(expected)) {
              if (typeof val === "function") {
                val(rows[0][col]);
              } else {
                assert.strictEqual(
                  rows[0][col],
                  val,
                  `mismatch at query ${i}, col ${col}`,
                );
              }
            }
          });
        } else {
          // Odd: tagged template (reused SQL text, unnamed prepared statements)
          p = taggedQuery(client, i);
        }

        p = p
          .then(() => {
            completed++;
          })
          .catch((err) => {
            errors.push({ i, err: err.message });
          });

        promises.push(p);
      }

      await Promise.all(promises);
    }

    assert.strictEqual(
      errors.length,
      0,
      `${errors.length} failures, first 5: ${JSON.stringify(errors.slice(0, 5))}`,
    );
    assert.strictEqual(completed, TOTAL_QUERIES);

    // Verify backend prepared statement evictions are happening.
    const res = await fetch("http://127.0.0.1:9090");
    const metrics = await res.text();
    const evictions = metrics
      .split("\n")
      .filter(
        (l) =>
          l.startsWith("pgdog_total_prepared_evictions") &&
          l.includes('database="pgdog"') &&
          l.includes('user="pgdog"'),
      )
      .map((l) => parseInt(l.split(" ").pop(), 10))
      .reduce((a, b) => a + b, 0);
    assert.ok(
      evictions === 0,
      `expected no prepared statement evictions, got ${evictions}`,
    );
  });
});
