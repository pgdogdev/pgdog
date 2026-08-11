import pg from "pg";
import Cursor from "pg-cursor";
import assert from "assert";
const { Client } = pg;

// https://github.com/pgdogdev/pgdog/issues/1330
//
// pg-cursor opens with Parse/Bind/Describe/Flush and no Execute. In
// transaction mode that used to release the backend before the portal was
// used, and the follow-up Execute failed with 34000.
//
// pg-query-stream and knex .stream() are built on pg-cursor.

const URL = "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog";

// Always end the client, otherwise a failure leaves mocha hanging on an
// open handle instead of reporting.
async function withClient(fn) {
  const client = new Client(URL);
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

it("reads the first page of a cursor", async () => {
  const rows = await withClient(async (client) => {
    const cursor = client.query(new Cursor("SELECT $1::bigint AS one", [1]));
    const page = await cursor.read(10);
    await cursor.close();
    return page;
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].one, "1");
});

it("pages through a cursor without losing or repeating rows", async () => {
  const seen = await withClient(async (client) => {
    const cursor = client.query(
      new Cursor("SELECT i FROM generate_series(1, 10) AS i ORDER BY i"),
    );

    const rows = [];
    for (;;) {
      const page = await cursor.read(3);
      if (page.length === 0) {
        break;
      }
      rows.push(...page.map((row) => Number(row.i)));
    }

    await cursor.close();
    return rows;
  });

  // Replaying the Bind on a fresh backend would restart at row 1 every page.
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
});

it("runs two cursors in sequence on the same client", async () => {
  await withClient(async (client) => {
    for (const expected of ["1", "2"]) {
      const cursor = client.query(new Cursor(`SELECT ${expected}::bigint AS n`));
      const rows = await cursor.read(10);
      await cursor.close();
      assert.equal(rows[0].n, expected);
    }

    // The backend was released after each cursor, so the pool still works.
    const res = await client.query("SELECT 3::bigint AS n");
    assert.equal(res.rows[0].n, "3");
  });
});

it("reads a cursor inside an explicit transaction", async () => {
  const rows = await withClient(async (client) => {
    await client.query("BEGIN");

    const cursor = client.query(
      new Cursor("SELECT i FROM generate_series(1, 4) AS i ORDER BY i"),
    );
    const first = await cursor.read(2);
    const second = await cursor.read(2);

    await cursor.close();
    await client.query("COMMIT");

    return [...first, ...second];
  });

  assert.deepEqual(
    rows.map((row) => Number(row.i)),
    [1, 2, 3, 4],
  );
});
