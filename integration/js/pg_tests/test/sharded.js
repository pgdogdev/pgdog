import pg from "pg";
import assert from "assert";
const { Client } = pg;

it("sharded", async () => {
  const client = new Client(
    "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded",
  );
  await client.connect();

  await client.query(
    "CREATE TABLE IF NOT EXISTS sharded (id BIGINT PRIMARY KEY, value TEXT)",
  );
  await client.query("TRUNCATE TABLE sharded");

  for (let i = 0; i < 25; i++) {
    const insert = await client.query(
      "INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING *",
      [i, "test_" + i],
    );

    assert.equal(insert.rows.length, 1);
    assert.equal(insert.rows[0].id, i.toString());
    assert.equal(insert.rows[0].value, "test_" + i);
  }

  for (let i = 0; i < 25; i++) {
    let select = await client.query("SELECT * FROM sharded WHERE id = $1", [i]);
    assert.equal(select.rows.length, 1);
    assert.equal(select.rows[0].id, i.toString());
    assert.equal(select.rows[0].value, "test_" + i);
  }

  await client.end();
});
