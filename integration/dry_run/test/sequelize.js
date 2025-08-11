import { Sequelize, DataTypes } from "@sequelize/core";
import { PostgresDialect } from "@sequelize/postgres";
import { Client } from "pg";
import assert from "assert";

const sequelize = new Sequelize({
  dialect: PostgresDialect,
  database: "pgdog",
  user: "pgdog",
  password: "pgdog",
  host: "127.0.0.1",
  port: 6432,
  ssl: false,
  clientMinMessages: "notice",
});

before(async function () {
  await sequelize.query('DROP TABLE IF EXISTS "User"');
  await sequelize.query(
    'CREATE TABLE IF NOT EXISTS "User" (id BIGSERIAL PRIMARY KEY, email VARCHAR, "createdAt" TIMESTAMPTZ NOT NULL DEFAULT NOW(), "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT NOW())',
  );
});

describe("sequelize", async function () {
  it("should run in dry run mode", async function () {
    const user = sequelize.define("User", {
      email: DataTypes.STRING,
    });

    for (let i = 0; i < 25; i++) {
      const _ = await user.findByPk(i);
    }

    const admin = new Client("postgres://admin:pgdog@127.0.0.1:6432/admin");
    await admin.connect();

    const cache = await admin.query("SHOW QUERY_CACHE");
    let found = false;
    for (let i = 0; i < cache.rows.length; i++) {
      let row = cache.rows[i];
      if (row.query.startsWith('SELECT "id", "email", "createdAt"')) {
        assert(parseInt(row.direct) > 0);
        found = true;
      }
    }
    await admin.end();

    assert(found);
  });
});

after(async function () {
  await sequelize.query('DROP TABLE "User"');
  await sequelize.close();
});
