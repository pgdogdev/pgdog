import pg from "pg";
import assert from "assert";
const { Client } = pg;

it("can connect", async () => {
  const client = new Client("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog");
  await client.connect();

  const res = await client.query("SELECT $1::bigint AS one", [1]);
  await client.end();

  assert.equal(res.rows[0].one, "1");
});
