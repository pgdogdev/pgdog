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
      { tableName: "sequelize_authors", timestamps: false }
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
      { tableName: "sequelize_books", timestamps: false }
    );

    Tag = seq.define(
      "Tag",
      {
        name: { type: DataTypes.STRING, allowNull: false },
      },
      { tableName: "sequelize_tags", timestamps: false }
    );

    BookTag = seq.define(
      "BookTag",
      {},
      { tableName: "sequelize_book_tags", timestamps: false }
    );

    Author.hasMany(Book, { foreignKey: "authorId", as: "books" });
    Book.belongsTo(Author, { foreignKey: "authorId", as: "author" });
    Book.belongsToMany(Tag, { through: BookTag, as: "tags" });
    Tag.belongsToMany(Book, { through: BookTag, as: "books" });

    await seq.sync({ force: true });
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "sequelize_book_tags"');
    await seq.query('DROP TABLE IF EXISTS "sequelize_books"');
    await seq.query('DROP TABLE IF EXISTS "sequelize_authors"');
    await seq.query('DROP TABLE IF EXISTS "sequelize_tags"');
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
    const book = await Book.create({ title: "Nested Book", authorId: author.id });
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
      { tableName: "sequelize_products", timestamps: false }
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
    await seq.query('DROP TABLE IF EXISTS "sequelize_products"');
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
      attributes: [
        "category",
        [seq.fn("COUNT", seq.col("id")), "count"],
      ],
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
      { tableName: "sequelize_type_test", timestamps: false }
    );

    await TypeTest.sync({ force: true });
  });

  after(async function () {
    await seq.query('DROP TABLE IF EXISTS "sequelize_type_test"');
    await seq.query('DROP TYPE IF EXISTS "enum_sequelize_type_test_enum_col"');
    await seq.close();
  });

  it("UUID auto-generation", async function () {
    const row = await TypeTest.create({});
    assert.ok(row.uuid_col);
    assert.match(
      row.uuid_col,
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
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
      `SELECT * FROM sequelize_type_test WHERE jsonb_col->>'status' = 'active'`
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
      { tableName: "sequelize_temp", timestamps: false }
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

    await seq.query('DROP TABLE IF EXISTS "sequelize_temp"');
    await seq.close();
  });

  it("concurrent inserts", async function () {
    const seq = createSequelize();
    const Counter = seq.define(
      "Counter",
      { value: DataTypes.INTEGER },
      { tableName: "sequelize_counter", timestamps: false }
    );
    await Counter.sync({ force: true });

    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(Counter.create({ value: i }));
    }
    await Promise.all(promises);

    const count = await Counter.count();
    assert.strictEqual(count, 10);

    await seq.query('DROP TABLE IF EXISTS "sequelize_counter"');
    await seq.close();
  });

  it("sequential reads and writes", async function () {
    const seq = createSequelize();
    const Mixed = seq.define(
      "Mixed",
      { value: DataTypes.INTEGER },
      { tableName: "sequelize_mixed", timestamps: false }
    );
    await Mixed.sync({ force: true });

    await Mixed.create({ value: 0 });

    for (let i = 0; i < 5; i++) {
      await Mixed.create({ value: i + 1 });
      await Mixed.findAll();
    }

    const count = await Mixed.count();
    assert.strictEqual(count, 6);

    await seq.query('DROP TABLE IF EXISTS "sequelize_mixed"');
    await seq.close();
  });

  it("multiple sequential transactions", async function () {
    const seq = createSequelize();
    const SeqTx = seq.define(
      "SeqTx",
      { value: DataTypes.INTEGER },
      { tableName: "sequelize_seqtx", timestamps: false }
    );
    await SeqTx.sync({ force: true });

    for (let i = 0; i < 5; i++) {
      const t = await seq.transaction();
      await SeqTx.create({ value: i }, { transaction: t });
      await t.commit();
    }

    const count = await SeqTx.count();
    assert.strictEqual(count, 5);

    await seq.query('DROP TABLE IF EXISTS "sequelize_seqtx"');
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
      }
    );
    assert.strictEqual(rows[0].product, 42);

    await seq.close();
  });

  it("empty result set", async function () {
    const seq = createSequelize();
    const Empty = seq.define(
      "Empty",
      { value: DataTypes.STRING },
      { tableName: "sequelize_empty", timestamps: false }
    );
    await Empty.sync({ force: true });

    const rows = await Empty.findAll();
    assert.deepStrictEqual(rows, []);

    const one = await Empty.findOne();
    assert.strictEqual(one, null);

    await seq.query('DROP TABLE IF EXISTS "sequelize_empty"');
    await seq.close();
  });

  it("special characters in string values", async function () {
    const seq = createSequelize();
    const Special = seq.define(
      "Special",
      { value: DataTypes.TEXT },
      { tableName: "sequelize_special", timestamps: false }
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

    await seq.query('DROP TABLE IF EXISTS "sequelize_special"');
    await seq.close();
  });

  it("very long transaction", async function () {
    const seq = createSequelize();
    const LongTx = seq.define(
      "LongTx",
      { value: DataTypes.INTEGER },
      { tableName: "sequelize_longtx", timestamps: false }
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

    await seq.query('DROP TABLE IF EXISTS "sequelize_longtx"');
    await seq.close();
  });
});

