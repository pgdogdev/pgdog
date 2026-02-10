import { Sequelize, DataTypes } from "sequelize";
import pg from "pg";
import assert from "assert";

const PGDOG_HOST = "127.0.0.1";
const PGDOG_PORT = 6432;
const ADMIN_URL = "postgresql://admin:pgdog@127.0.0.1:6432/admin";

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

// ---------------------------------------------------------------------------
// Global setup/teardown for admin settings
// ---------------------------------------------------------------------------

before(async function () {
  await setupAdmin();
});

after(async function () {
  await teardownAdmin();
});

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
      { tableName: "seq_items_7x", timestamps: true },
    );

    await Item.sync({ force: true });
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "seq_items_7x"');
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
      { transaction: t },
    );
    await t.commit();

    const found = await Item.findByPk(item.id);
    assert.strictEqual(found.name, "tx_item");
  });

  it("transaction rollback", async function () {
    const t = await seq.transaction();
    const item = await Item.create(
      { name: "rollback_item", quantity: 99 },
      { transaction: t },
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
      { transaction: t },
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
      { transaction: t },
    );

    const cmResult = await seq.query("SHOW client_min_messages", {
      transaction: t,
    });
    assert.strictEqual(cmResult[0].client_min_messages, "warning");

    const tzResult = await seq.query("SHOW timezone", { transaction: t });
    const tz = tzResult[0].TimeZone || tzResult[0].timezone;
    assert.ok(
      tz.includes("00:00") || tz === "UTC" || tz === "Etc/UTC",
      `expected UTC-equivalent timezone, got ${tz}`,
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
          "multi-statement queries cannot mix SET with other commands",
        ),
        `unexpected error: ${err.message}`,
      );
    }

    const [rows] = await seq.query("SELECT 1 AS val");
    assert.strictEqual(rows[0].val, 1);

    await seq.close();
  });
});

// ---------------------------------------------------------------------------
// Association tests
// ---------------------------------------------------------------------------

describe("Sequelize associations", function () {
  let seq;
  let Author, Book, Tag, BookTag;

  before(async function () {
    seq = createSequelize();

    Author = seq.define(
      "Author",
      {
        name: { type: DataTypes.STRING, allowNull: false },
      },
      { tableName: "seq_authors_7x", timestamps: false },
    );

    Book = seq.define(
      "Book",
      {
        title: { type: DataTypes.STRING, allowNull: false },
        authorId: {
          type: DataTypes.INTEGER,
          references: { model: Author, key: "id" },
        },
      },
      { tableName: "seq_books_7x", timestamps: false },
    );

    Tag = seq.define(
      "Tag",
      {
        name: { type: DataTypes.STRING, allowNull: false },
      },
      { tableName: "seq_tags_7x", timestamps: false },
    );

    BookTag = seq.define(
      "BookTag",
      {},
      { tableName: "seq_book_tags_7x", timestamps: false },
    );

    Author.hasMany(Book, { foreignKey: "authorId", as: "books" });
    Book.belongsTo(Author, { foreignKey: "authorId", as: "author" });
    Book.belongsToMany(Tag, { through: BookTag, as: "tags" });
    Tag.belongsToMany(Book, { through: BookTag, as: "books" });

    await seq.sync({ force: true });
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "seq_book_tags_7x"');
    await seq.query('DROP TABLE IF EXISTS "seq_books_7x"');
    await seq.query('DROP TABLE IF EXISTS "seq_authors_7x"');
    await seq.query('DROP TABLE IF EXISTS "seq_tags_7x"');
    await seq.close();
  });

  it("hasMany and belongsTo", async function () {
    const author = await Author.create({ name: "Jane Austen" });
    await Book.create({ title: "Pride and Prejudice", authorId: author.id });
    await Book.create({ title: "Emma", authorId: author.id });

    const authorWithBooks = await Author.findByPk(author.id, {
      include: [{ model: Book, as: "books" }],
    });
    assert.strictEqual(authorWithBooks.books.length, 2);
  });

  it("belongsTo eager loading", async function () {
    const author = await Author.create({ name: "George Orwell" });
    const book = await Book.create({ title: "1984", authorId: author.id });

    const bookWithAuthor = await Book.findByPk(book.id, {
      include: [{ model: Author, as: "author" }],
    });
    assert.strictEqual(bookWithAuthor.author.name, "George Orwell");
  });

  it("many-to-many through table", async function () {
    const book = await Book.create({ title: "Test Book" });
    const tag1 = await Tag.create({ name: "fiction" });
    const tag2 = await Tag.create({ name: "classic" });

    await book.addTags([tag1, tag2]);

    const bookWithTags = await Book.findByPk(book.id, {
      include: [{ model: Tag, as: "tags" }],
    });
    assert.strictEqual(bookWithTags.tags.length, 2);

    const tagWithBooks = await Tag.findByPk(tag1.id, {
      include: [{ model: Book, as: "books" }],
    });
    assert.ok(tagWithBooks.books.length >= 1);
  });

  it("nested include", async function () {
    const author = await Author.create({ name: "Nested Author" });
    const book = await Book.create({
      title: "Nested Book",
      authorId: author.id,
    });
    const tag = await Tag.create({ name: "nested-tag" });
    await book.addTag(tag);

    const authorNested = await Author.findByPk(author.id, {
      include: [
        {
          model: Book,
          as: "books",
          include: [{ model: Tag, as: "tags" }],
        },
      ],
    });
    assert.strictEqual(authorNested.books[0].tags[0].name, "nested-tag");
  });
});

// ---------------------------------------------------------------------------
// Advanced query tests
// ---------------------------------------------------------------------------

describe("Sequelize advanced queries", function () {
  let seq;
  let Product;

  before(async function () {
    seq = createSequelize();

    Product = seq.define(
      "Product",
      {
        name: { type: DataTypes.STRING, allowNull: false },
        category: { type: DataTypes.STRING },
        price: { type: DataTypes.DECIMAL(10, 2) },
        stock: { type: DataTypes.INTEGER, defaultValue: 0 },
      },
      { tableName: "seq_products_7x", timestamps: false },
    );

    await Product.sync({ force: true });

    await Product.bulkCreate([
      { name: "Widget A", category: "widgets", price: 10.0, stock: 100 },
      { name: "Widget B", category: "widgets", price: 15.0, stock: 50 },
      { name: "Gadget A", category: "gadgets", price: 25.0, stock: 30 },
      { name: "Gadget B", category: "gadgets", price: 35.0, stock: 20 },
      { name: "Tool A", category: "tools", price: 50.0, stock: 10 },
    ]);
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "seq_products_7x"');
    await seq.close();
  });

  it("aggregate SUM", async function () {
    const total = await Product.sum("stock");
    assert.strictEqual(total, 210);
  });

  it("aggregate with where clause", async function () {
    const widgetStock = await Product.sum("stock", {
      where: { category: "widgets" },
    });
    assert.strictEqual(widgetStock, 150);
  });

  it("findOne with create fallback", async function () {
    let product = await Product.findOne({ where: { name: "New Product" } });
    if (!product) {
      product = await Product.create({
        name: "New Product",
        category: "new",
        price: 99.99,
        stock: 5,
      });
    }
    assert.strictEqual(product.name, "New Product");
  });

  it("findOne returns existing", async function () {
    const product = await Product.findOne({ where: { name: "Widget A" } });
    assert.ok(product);
    assert.strictEqual(product.category, "widgets");
  });

  it("increment field", async function () {
    const product = await Product.findOne({ where: { name: "Widget A" } });
    const originalStock = product.stock;
    await product.increment("stock", { by: 10 });

    const found = await Product.findByPk(product.id);
    assert.strictEqual(found.stock, originalStock + 10);
  });

  it("decrement field", async function () {
    const product = await Product.findOne({ where: { name: "Widget B" } });
    const originalStock = product.stock;
    await product.decrement("stock", { by: 5 });

    const found = await Product.findByPk(product.id);
    assert.strictEqual(found.stock, originalStock - 5);
  });

  it("Op.in operator", async function () {
    const { Op } = await import("sequelize");
    const products = await Product.findAll({
      where: { category: { [Op.in]: ["widgets", "gadgets"] } },
    });
    assert.ok(products.length >= 4);
  });

  it("Op.like operator", async function () {
    const { Op } = await import("sequelize");
    const products = await Product.findAll({
      where: { name: { [Op.like]: "Widget%" } },
    });
    assert.ok(products.length >= 2);
  });

  it("Op.between operator", async function () {
    const { Op } = await import("sequelize");
    const products = await Product.findAll({
      where: { price: { [Op.between]: [10, 30] } },
    });
    assert.ok(products.length >= 3);
  });

  it("ordering and limit", async function () {
    const products = await Product.findAll({
      order: [["price", "DESC"]],
      limit: 3,
    });
    assert.strictEqual(products.length, 3);
    assert.ok(parseFloat(products[0].price) >= parseFloat(products[1].price));
  });

  it("offset pagination", async function () {
    const page1 = await Product.findAll({
      order: [["id", "ASC"]],
      limit: 2,
      offset: 0,
    });
    const page2 = await Product.findAll({
      order: [["id", "ASC"]],
      limit: 2,
      offset: 2,
    });
    assert.notStrictEqual(page1[0].id, page2[0].id);
  });

  it("group by with count", async function () {
    const counts = await Product.findAll({
      attributes: ["category", [seq.fn("COUNT", seq.col("id")), "count"]],
      group: ["category"],
    });
    assert.ok(counts.length >= 3);
  });
});

// ---------------------------------------------------------------------------
// Data type tests
// ---------------------------------------------------------------------------

describe("Sequelize data types", function () {
  let seq;
  let TypeTest;

  before(async function () {
    seq = createSequelize();

    TypeTest = seq.define(
      "TypeTest",
      {
        uuid_col: {
          type: DataTypes.UUID,
          defaultValue: DataTypes.UUIDV4,
        },
        json_col: { type: DataTypes.JSON },
        jsonb_col: { type: DataTypes.JSONB },
        array_col: { type: DataTypes.ARRAY(DataTypes.STRING) },
        int_array_col: { type: DataTypes.ARRAY(DataTypes.INTEGER) },
        date_col: { type: DataTypes.DATEONLY },
        datetime_col: { type: DataTypes.DATE },
        bool_col: { type: DataTypes.BOOLEAN },
        text_col: { type: DataTypes.TEXT },
        float_col: { type: DataTypes.FLOAT },
        enum_col: { type: DataTypes.ENUM("active", "inactive", "pending") },
      },
      { tableName: "seq_types_7x", timestamps: false },
    );

    await TypeTest.sync({ force: true });
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "seq_types_7x"');
    await seq.query('DROP TYPE IF EXISTS "enum_seq_types_7x_enum_col"');
    await seq.close();
  });

  it("UUID auto-generation", async function () {
    const row = await TypeTest.create({});
    assert.ok(row.uuid_col);
    assert.match(
      row.uuid_col,
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
  });

  it("JSON column", async function () {
    const data = { key: "value", nested: { num: 42 } };
    const row = await TypeTest.create({ json_col: data });

    const found = await TypeTest.findByPk(row.id);
    assert.deepStrictEqual(found.json_col, data);
  });

  it("JSONB column with query", async function () {
    const data = { status: "active", count: 10 };
    await TypeTest.create({ jsonb_col: data });

    const [rows] = await seq.query(
      `SELECT * FROM seq_types_7x WHERE jsonb_col->>'status' = 'active'`,
    );
    assert.ok(rows.length >= 1);
  });

  it("string array column", async function () {
    const tags = ["red", "green", "blue"];
    const row = await TypeTest.create({ array_col: tags });

    const found = await TypeTest.findByPk(row.id);
    assert.deepStrictEqual(found.array_col, tags);
  });

  it("integer array column", async function () {
    const nums = [1, 2, 3, 4, 5];
    const row = await TypeTest.create({ int_array_col: nums });

    const found = await TypeTest.findByPk(row.id);
    assert.deepStrictEqual(found.int_array_col, nums);
  });

  it("array contains query", async function () {
    const { Op } = await import("sequelize");
    await TypeTest.create({ array_col: ["alpha", "beta"] });

    const rows = await TypeTest.findAll({
      where: { array_col: { [Op.contains]: ["alpha"] } },
    });
    assert.ok(rows.length >= 1);
  });

  it("date only column", async function () {
    const row = await TypeTest.create({ date_col: "2024-06-15" });

    const found = await TypeTest.findByPk(row.id);
    assert.strictEqual(found.date_col, "2024-06-15");
  });

  it("datetime column", async function () {
    const dt = new Date("2024-06-15T10:30:00Z");
    const row = await TypeTest.create({ datetime_col: dt });

    const found = await TypeTest.findByPk(row.id);
    assert.strictEqual(found.datetime_col.toISOString(), dt.toISOString());
  });

  it("boolean column", async function () {
    const rowTrue = await TypeTest.create({ bool_col: true });
    const rowFalse = await TypeTest.create({ bool_col: false });

    const foundTrue = await TypeTest.findByPk(rowTrue.id);
    const foundFalse = await TypeTest.findByPk(rowFalse.id);

    assert.strictEqual(foundTrue.bool_col, true);
    assert.strictEqual(foundFalse.bool_col, false);
  });

  it("enum column", async function () {
    const row = await TypeTest.create({ enum_col: "pending" });

    const found = await TypeTest.findByPk(row.id);
    assert.strictEqual(found.enum_col, "pending");
  });

  it("enum validation rejects invalid value", async function () {
    try {
      await TypeTest.create({ enum_col: "invalid" });
      assert.fail("should have thrown");
    } catch (err) {
      assert.ok(err);
    }
  });

  it("float column", async function () {
    const row = await TypeTest.create({ float_col: 3.14159 });

    const found = await TypeTest.findByPk(row.id);
    assert.ok(Math.abs(found.float_col - 3.14159) < 0.0001);
  });

  it("text column with long content", async function () {
    const longText = "x".repeat(10000);
    const row = await TypeTest.create({ text_col: longText });

    const found = await TypeTest.findByPk(row.id);
    assert.strictEqual(found.text_col.length, 10000);
  });
});

// ---------------------------------------------------------------------------
// Edge case tests
// ---------------------------------------------------------------------------

describe("Sequelize edge cases", function () {
  it("savepoint with nested transaction", async function () {
    const seq = createSequelize();
    const TempTable = seq.define(
      "Temp",
      { value: DataTypes.STRING },
      { tableName: "seq_temp_7x", timestamps: false },
    );
    await TempTable.sync({ force: true });

    const t = await seq.transaction();
    try {
      await TempTable.create({ value: "outer" }, { transaction: t });

      const savepoint = await seq.transaction({ transaction: t });
      try {
        await TempTable.create({ value: "inner" }, { transaction: savepoint });
        await savepoint.rollback();
      } catch {
        await savepoint.rollback();
      }

      await t.commit();

      const rows = await TempTable.findAll();
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].value, "outer");
    } catch (err) {
      await t.rollback();
      throw err;
    }

    await seq.query('DROP TABLE IF EXISTS "seq_temp_7x"');
    await seq.close();
  });

  it("concurrent inserts", async function () {
    const seq = createSequelize();
    const Counter = seq.define(
      "Counter",
      { value: DataTypes.INTEGER },
      { tableName: "seq_counter_7x", timestamps: false },
    );
    await Counter.sync({ force: true });

    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(Counter.create({ value: i }));
    }
    await Promise.all(promises);

    const count = await Counter.count();
    assert.strictEqual(count, 10);

    await seq.query('DROP TABLE IF EXISTS "seq_counter_7x"');
    await seq.close();
  });

  it("sequential reads and writes", async function () {
    const seq = createSequelize();
    const Mixed = seq.define(
      "Mixed",
      { value: DataTypes.INTEGER },
      { tableName: "seq_mixed_7x", timestamps: false },
    );
    await Mixed.sync({ force: true });

    await Mixed.create({ value: 0 });

    for (let i = 0; i < 5; i++) {
      await Mixed.create({ value: i + 1 });
      await Mixed.findAll();
    }

    const count = await Mixed.count();
    assert.strictEqual(count, 6);

    await seq.query('DROP TABLE IF EXISTS "seq_mixed_7x"');
    await seq.close();
  });

  it("multiple sequential transactions", async function () {
    const seq = createSequelize();
    const SeqTx = seq.define(
      "SeqTx",
      { value: DataTypes.INTEGER },
      { tableName: "seq_seqtx_7x", timestamps: false },
    );
    await SeqTx.sync({ force: true });

    for (let i = 0; i < 5; i++) {
      const t = await seq.transaction();
      await SeqTx.create({ value: i }, { transaction: t });
      await t.commit();
    }

    const count = await SeqTx.count();
    assert.strictEqual(count, 5);

    await seq.query('DROP TABLE IF EXISTS "seq_seqtx_7x"');
    await seq.close();
  });

  it("raw query with parameters", async function () {
    const seq = createSequelize();

    const [rows] = await seq.query("SELECT $1::integer + $2::integer AS sum", {
      bind: [10, 20],
    });
    assert.strictEqual(rows[0].sum, 30);

    await seq.close();
  });

  it("raw query with named replacements", async function () {
    const seq = createSequelize();

    const [rows] = await seq.query(
      "SELECT :a::integer * :b::integer AS product",
      {
        replacements: { a: 6, b: 7 },
      },
    );
    assert.strictEqual(rows[0].product, 42);

    await seq.close();
  });

  it("empty result set", async function () {
    const seq = createSequelize();
    const Empty = seq.define(
      "Empty",
      { value: DataTypes.STRING },
      { tableName: "seq_empty_7x", timestamps: false },
    );
    await Empty.sync({ force: true });

    const rows = await Empty.findAll();
    assert.deepStrictEqual(rows, []);

    const one = await Empty.findOne();
    assert.strictEqual(one, null);

    await seq.query('DROP TABLE IF EXISTS "seq_empty_7x"');
    await seq.close();
  });

  it("special characters in string values", async function () {
    const seq = createSequelize();
    const Special = seq.define(
      "Special",
      { value: DataTypes.TEXT },
      { tableName: "seq_special_7x", timestamps: false },
    );
    await Special.sync({ force: true });

    const specialStrings = [
      "Hello 'world'",
      'Say "hello"',
      "Back\\slash",
      "New\nline",
      "Tab\there",
      "Unicode: \u00e9\u00e8\u00ea",
      "Emoji: \u{1F600}",
    ];

    for (const str of specialStrings) {
      const row = await Special.create({ value: str });
      const found = await Special.findByPk(row.id);
      assert.strictEqual(found.value, str);
    }

    await seq.query('DROP TABLE IF EXISTS "seq_special_7x"');
    await seq.close();
  });

  it("very long transaction", async function () {
    const seq = createSequelize();
    const LongTx = seq.define(
      "LongTx",
      { value: DataTypes.INTEGER },
      { tableName: "seq_longtx_7x", timestamps: false },
    );
    await LongTx.sync({ force: true });

    const t = await seq.transaction();

    for (let i = 0; i < 50; i++) {
      await LongTx.create({ value: i }, { transaction: t });
    }

    const countInTx = await LongTx.count({ transaction: t });
    assert.strictEqual(countInTx, 50);

    const countOutside = await LongTx.count();
    assert.strictEqual(countOutside, 0);

    await t.commit();

    const countAfter = await LongTx.count();
    assert.strictEqual(countAfter, 50);

    await seq.query('DROP TABLE IF EXISTS "seq_longtx_7x"');
    await seq.close();
  });
});

// ---------------------------------------------------------------------------
// Protocol tests (simple vs extended)
// ---------------------------------------------------------------------------

describe("Sequelize protocol tests", function () {
  it("simple protocol query", async function () {
    const seq = createSequelize();

    const [rows] = await seq.query("SELECT 1 + 1 AS result");
    assert.strictEqual(rows[0].result, 2);

    await seq.close();
  });

  it("extended protocol query with bind parameters", async function () {
    const seq = createSequelize();

    const [rows] = await seq.query("SELECT $1::int + $2::int AS result", {
      bind: [1, 1],
    });
    assert.strictEqual(rows[0].result, 2);

    await seq.close();
  });

  it("same query via simple and extended protocol", async function () {
    const seq = createSequelize();

    const simpleResult = await seq.query("SELECT 42 AS value");
    assert.strictEqual(simpleResult[0][0].value, 42);

    const extendedResult = await seq.query("SELECT $1::int AS value", {
      bind: [42],
    });
    assert.strictEqual(extendedResult[0][0].value, 42);

    await seq.close();
  });

  it("model operations via simple and extended protocol", async function () {
    const seq = createSequelize();

    const ProtocolTest = seq.define(
      "ProtocolTest",
      {
        name: { type: DataTypes.STRING, allowNull: false },
        value: { type: DataTypes.INTEGER },
      },
      { tableName: "seq_protocol_test_7x", timestamps: false },
    );

    await ProtocolTest.sync({ force: true });

    // Extended protocol: ORM create uses prepared statements
    const item = await ProtocolTest.create({ name: "test", value: 42 });
    assert.strictEqual(item.name, "test");
    assert.strictEqual(item.value, 42);

    // Extended protocol: ORM findOne with where clause
    const found = await ProtocolTest.findOne({ where: { name: "test" } });
    assert.strictEqual(found.value, 42);

    // Simple protocol: raw query without parameters
    const [simpleRows] = await seq.query(
      "SELECT * FROM seq_protocol_test_7x WHERE name = 'test'",
    );
    assert.strictEqual(simpleRows[0].value, 42);

    // Extended protocol: raw query with bind parameters
    const [extendedRows] = await seq.query(
      "SELECT * FROM seq_protocol_test_7x WHERE name = $1",
      { bind: ["test"] },
    );
    assert.strictEqual(extendedRows[0].value, 42);

    await seq.query('DROP TABLE IF EXISTS "seq_protocol_test_7x"');
    await seq.close();
  });
});

// ---------------------------------------------------------------------------
// Sharded CRUD tests
// ---------------------------------------------------------------------------

describe("Sequelize sharded CRUD", function () {
  let seq;
  let ShardedOrder;

  before(async function () {
    seq = createSequelize("pgdog_sharded");

    ShardedOrder = seq.define(
      "ShardedOrder",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true },
        customerId: {
          type: DataTypes.BIGINT,
          allowNull: false,
          field: "customer_id",
        },
        total: { type: DataTypes.DECIMAL(10, 2) },
        status: { type: DataTypes.STRING },
      },
      { tableName: "seq_sh_orders_7x", timestamps: false },
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS public.seq_sh_orders_7x (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        total DECIMAL(10, 2),
        status TEXT
      )
    `);

    await ShardedOrder.destroy({ where: {} });
  });

  after(async function () {
    await seq.query("DROP TABLE IF EXISTS public.seq_sh_orders_7x");
    await seq.close();
  });

  it("create order with ORM", async function () {
    const order = await ShardedOrder.create({
      id: 1,
      customerId: 1,
      total: 100.0,
      status: "pending",
    });
    assert.strictEqual(String(order.customerId), "1");
    assert.strictEqual(order.status, "pending");
  });

  it("findAll by customer_id", async function () {
    await ShardedOrder.bulkCreate([
      { id: 2, customerId: 1, total: 200.0, status: "pending" },
      { id: 3, customerId: 1, total: 300.0, status: "shipped" },
    ]);

    const orders = await ShardedOrder.findAll({
      where: { customerId: 1 },
    });
    assert.strictEqual(orders.length, 3);
  });

  it("findOne by customer_id", async function () {
    const order = await ShardedOrder.findOne({
      where: { customerId: 1 },
    });
    assert.ok(order);
    assert.strictEqual(String(order.customerId), "1");
  });

  it("findByPk", async function () {
    const order = await ShardedOrder.findByPk(1);
    assert.ok(order);
    assert.strictEqual(String(order.id), "1");
  });

  it("update by id", async function () {
    await ShardedOrder.update({ status: "completed" }, { where: { id: 1 } });
    const order = await ShardedOrder.findByPk(1);
    assert.strictEqual(order.status, "completed");
  });

  it("update many by customer_id", async function () {
    await ShardedOrder.update(
      { status: "archived" },
      { where: { customerId: 1 } },
    );
    const orders = await ShardedOrder.findAll({
      where: { customerId: 1 },
    });
    assert.strictEqual(orders.length, 3);
    orders.forEach((o) => assert.strictEqual(o.status, "archived"));
  });

  it("destroy by id", async function () {
    await ShardedOrder.destroy({ where: { id: 3 } });
    const order = await ShardedOrder.findByPk(3);
    assert.strictEqual(order, null);
  });

  it("destroy many by customer_id", async function () {
    await ShardedOrder.destroy({ where: { customerId: 1 } });
    const orders = await ShardedOrder.findAll({
      where: { customerId: 1 },
    });
    assert.strictEqual(orders.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Sharded aggregation tests
// ---------------------------------------------------------------------------

describe("Sequelize sharded aggregations", function () {
  let seq;
  let ShardedOrder;

  before(async function () {
    seq = createSequelize("pgdog_sharded");

    ShardedOrder = seq.define(
      "ShardedOrder",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true },
        customerId: {
          type: DataTypes.BIGINT,
          allowNull: false,
          field: "customer_id",
        },
        total: { type: DataTypes.DECIMAL(10, 2) },
        status: { type: DataTypes.STRING },
      },
      { tableName: "seq_sh_orders_7x", timestamps: false },
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS public.seq_sh_orders_7x (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        total DECIMAL(10, 2),
        status TEXT
      )
    `);

    await ShardedOrder.destroy({ where: {} });
    await ShardedOrder.bulkCreate([
      { id: 100, customerId: 100, total: 100.0, status: "completed" },
      { id: 101, customerId: 100, total: 200.0, status: "pending" },
      { id: 102, customerId: 100, total: 300.0, status: "completed" },
    ]);
  });

  after(async function () {
    await ShardedOrder.destroy({ where: { customerId: 100 } });
    await seq.close();
  });

  it("count by customer_id", async function () {
    const count = await ShardedOrder.count({
      where: { customerId: 100 },
    });
    assert.strictEqual(count, 3);
  });

  it("sum by customer_id", async function () {
    const sum = await ShardedOrder.sum("total", {
      where: { customerId: 100 },
    });
    assert.strictEqual(parseFloat(sum), 600.0);
  });

  it("max by customer_id", async function () {
    const max = await ShardedOrder.max("total", {
      where: { customerId: 100 },
    });
    assert.strictEqual(parseFloat(max), 300.0);
  });

  it("min by customer_id", async function () {
    const min = await ShardedOrder.min("total", {
      where: { customerId: 100 },
    });
    assert.strictEqual(parseFloat(min), 100.0);
  });

  it("group by status for customer", async function () {
    const result = await ShardedOrder.findAll({
      attributes: ["status", [seq.fn("COUNT", seq.col("id")), "count"]],
      where: { customerId: 100 },
      group: ["status"],
    });
    assert.strictEqual(result.length, 2);
  });
});

// ---------------------------------------------------------------------------
// Sharded transaction tests
// ---------------------------------------------------------------------------

describe("Sequelize sharded transactions", function () {
  let seq;
  let ShardedOrder;

  before(async function () {
    seq = createSequelize("pgdog_sharded");

    ShardedOrder = seq.define(
      "ShardedOrder",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true },
        customerId: {
          type: DataTypes.BIGINT,
          allowNull: false,
          field: "customer_id",
        },
        total: { type: DataTypes.DECIMAL(10, 2) },
        status: { type: DataTypes.STRING },
      },
      { tableName: "seq_sh_orders_7x", timestamps: false },
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS public.seq_sh_orders_7x (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        total DECIMAL(10, 2),
        status TEXT
      )
    `);
  });

  after(async function () {
    await seq.close();
  });

  it("transaction with multiple creates", async function () {
    const customerId = 200;
    const t = await seq.transaction();

    await ShardedOrder.create(
      { id: 200, customerId, total: 500.0, status: "pending" },
      { transaction: t },
    );
    await ShardedOrder.create(
      { id: 201, customerId, total: 600.0, status: "pending" },
      { transaction: t },
    );

    await t.commit();

    const orders = await ShardedOrder.findAll({
      where: { customerId },
    });
    assert.strictEqual(orders.length, 2);

    await ShardedOrder.destroy({ where: { customerId } });
  });

  it("transaction rollback", async function () {
    const customerId = 201;
    const t = await seq.transaction();

    await ShardedOrder.create(
      { id: 202, customerId, total: 700.0, status: "pending" },
      { transaction: t },
    );

    await t.rollback();

    const orders = await ShardedOrder.findAll({
      where: { customerId },
    });
    assert.strictEqual(orders.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Cross-shard query tests
// ---------------------------------------------------------------------------

describe("Sequelize cross-shard queries", function () {
  let seq;
  let ShardedMetric;

  before(async function () {
    seq = createSequelize("pgdog_sharded");

    ShardedMetric = seq.define(
      "ShardedMetric",
      {
        id: { type: DataTypes.BIGINT, primaryKey: true },
        customerId: {
          type: DataTypes.BIGINT,
          allowNull: false,
          field: "customer_id",
        },
        name: { type: DataTypes.STRING },
        value: { type: DataTypes.INTEGER },
      },
      { tableName: "seq_sh_metrics_7x", timestamps: false },
    );

    await seq.query(`
      CREATE TABLE IF NOT EXISTS public.seq_sh_metrics_7x (
        id BIGINT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        name TEXT,
        value INT
      )
    `);

    await ShardedMetric.destroy({ where: {} });

    let id = 1;
    for (const customerId of [900, 901, 902, 903, 904]) {
      for (let i = 0; i < 4; i++) {
        await ShardedMetric.create({
          id: id,
          customerId: customerId,
          name: `metric_${i}`,
          value: customerId + i,
        });
        id++;
      }
    }
  });

  after(async function () {
    await seq.query("DROP TABLE IF EXISTS public.seq_sh_metrics_7x");
    await seq.close();
  });

  it("count all rows across shards", async function () {
    const count = await ShardedMetric.count();
    assert.strictEqual(count, 20);
  });

  it("findAll without customer_id filter", async function () {
    const metrics = await ShardedMetric.findAll({
      order: [["id", "ASC"]],
    });
    assert.strictEqual(metrics.length, 20);
  });

  it("findAll with limit across shards", async function () {
    const metrics = await ShardedMetric.findAll({
      order: [["id", "ASC"]],
      limit: 10,
    });
    assert.strictEqual(metrics.length, 10);
  });

  it("sum across all shards", async function () {
    const sum = await ShardedMetric.sum("value");
    const expected = [900, 901, 902, 903, 904].reduce(
      (acc, cid) => acc + cid + (cid + 1) + (cid + 2) + (cid + 3),
      0,
    );
    assert.strictEqual(sum, expected);
  });

  it("group by name across shards", async function () {
    const result = await ShardedMetric.findAll({
      attributes: [
        "name",
        [seq.fn("COUNT", seq.col("id")), "count"],
        [seq.fn("SUM", seq.col("value")), "total"],
      ],
      group: ["name"],
      order: [["name", "ASC"]],
    });
    assert.strictEqual(result.length, 4);
    result.forEach((r) => {
      assert.strictEqual(parseInt(r.dataValues.count), 5);
    });
  });

  it("findAll by single customer returns exact count", async function () {
    const metrics = await ShardedMetric.findAll({
      where: { customerId: 902 },
    });
    assert.strictEqual(metrics.length, 4);
    metrics.forEach((m) => assert.strictEqual(String(m.customerId), "902"));
  });
});
