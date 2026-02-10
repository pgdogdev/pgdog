import { PrismaClient } from "@prisma/client";
import pg from "pg";
import assert from "assert";

const DATABASE_URL = "postgresql://pgdog:pgdog@127.0.0.1:6432/pgdog";
const ADMIN_URL = "postgresql://admin:pgdog@127.0.0.1:6432/admin";

function createPrisma() {
  return new PrismaClient({
    datasources: {
      db: { url: DATABASE_URL },
    },
    log: [],
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
// Schema setup - create tables for non-sharded database
// ---------------------------------------------------------------------------

async function setupSchema(prisma) {
  // Prisma ORM always uses schema-qualified names (public.tablename), so we must
  // create tables explicitly in the public schema
  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.prisma_users (
      id SERIAL PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      name TEXT,
      active BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.prisma_posts (
      id SERIAL PRIMARY KEY,
      title TEXT NOT NULL,
      content TEXT,
      published BOOLEAN DEFAULT false,
      author_id INT REFERENCES public.prisma_users(id) ON DELETE CASCADE
    )
  `;

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.prisma_products (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      price DECIMAL(10, 2),
      stock INT DEFAULT 0,
      category TEXT,
      metadata JSONB
    )
  `;

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.prisma_accounts (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      balance DECIMAL(10, 2) DEFAULT 0
    )
  `;
}

// ---------------------------------------------------------------------------
// Basic CRUD using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM CRUD", function () {
  let prisma;

  before(async function () {
    await setupAdmin();
    prisma = createPrisma();
    await prisma.$connect();
    await setupSchema(prisma);
    await prisma.prismaPost.deleteMany();
    await prisma.prismaUser.deleteMany();
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("create user", async function () {
    const user = await prisma.prismaUser.create({
      data: {
        email: "alice@example.com",
        name: "Alice",
      },
    });
    assert.strictEqual(user.email, "alice@example.com");
    assert.strictEqual(user.name, "Alice");
    assert.ok(user.id);
  });

  it("findUnique by email", async function () {
    const user = await prisma.prismaUser.findUnique({
      where: { email: "alice@example.com" },
    });
    assert.ok(user);
    assert.strictEqual(user.name, "Alice");
  });

  it("findFirst", async function () {
    await prisma.prismaUser.create({
      data: { email: "bob@example.com", name: "Bob" },
    });
    const user = await prisma.prismaUser.findFirst({
      where: { name: "Bob" },
    });
    assert.ok(user);
    assert.strictEqual(user.email, "bob@example.com");
  });

  it("findMany", async function () {
    const users = await prisma.prismaUser.findMany();
    assert.ok(users.length >= 2);
  });

  it("update", async function () {
    const updated = await prisma.prismaUser.update({
      where: { email: "alice@example.com" },
      data: { name: "Alice Updated" },
    });
    assert.strictEqual(updated.name, "Alice Updated");
  });

  it("upsert - insert", async function () {
    const user = await prisma.prismaUser.upsert({
      where: { email: "charlie@example.com" },
      update: { name: "Charlie Updated" },
      create: { email: "charlie@example.com", name: "Charlie New" },
    });
    assert.strictEqual(user.name, "Charlie New");
  });

  it("upsert - update", async function () {
    const user = await prisma.prismaUser.upsert({
      where: { email: "charlie@example.com" },
      update: { name: "Charlie Updated" },
      create: { email: "charlie@example.com", name: "Charlie New" },
    });
    assert.strictEqual(user.name, "Charlie Updated");
  });

  it("delete", async function () {
    await prisma.prismaUser.create({
      data: { email: "delete-me@example.com", name: "Delete Me" },
    });
    await prisma.prismaUser.delete({
      where: { email: "delete-me@example.com" },
    });
    const user = await prisma.prismaUser.findUnique({
      where: { email: "delete-me@example.com" },
    });
    assert.strictEqual(user, null);
  });

  it("count", async function () {
    const count = await prisma.prismaUser.count();
    assert.ok(count >= 3);
  });

  it("createMany", async function () {
    await prisma.prismaUser.createMany({
      data: [
        { email: "batch1@example.com", name: "Batch 1" },
        { email: "batch2@example.com", name: "Batch 2" },
        { email: "batch3@example.com", name: "Batch 3" },
      ],
    });
    const users = await prisma.prismaUser.findMany({
      where: { email: { startsWith: "batch" } },
    });
    assert.strictEqual(users.length, 3);
  });

  it("deleteMany", async function () {
    await prisma.prismaUser.deleteMany({
      where: { email: { startsWith: "batch" } },
    });
    const users = await prisma.prismaUser.findMany({
      where: { email: { startsWith: "batch" } },
    });
    assert.strictEqual(users.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Filtering and ordering using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM filtering and ordering", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupSchema(prisma);
    await prisma.prismaProduct.deleteMany();
    await prisma.prismaProduct.createMany({
      data: [
        { name: "Widget A", price: 10.0, stock: 100, category: "widgets" },
        { name: "Widget B", price: 15.0, stock: 50, category: "widgets" },
        { name: "Gadget A", price: 25.0, stock: 30, category: "gadgets" },
        { name: "Gadget B", price: 35.0, stock: 20, category: "gadgets" },
        { name: "Tool A", price: 50.0, stock: 10, category: "tools" },
      ],
    });
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("filter with equals", async function () {
    const products = await prisma.prismaProduct.findMany({
      where: { category: "widgets" },
    });
    assert.strictEqual(products.length, 2);
  });

  it("filter with contains", async function () {
    const products = await prisma.prismaProduct.findMany({
      where: { name: { contains: "Widget" } },
    });
    assert.strictEqual(products.length, 2);
  });

  it("filter with in", async function () {
    const products = await prisma.prismaProduct.findMany({
      where: { category: { in: ["widgets", "gadgets"] } },
    });
    assert.strictEqual(products.length, 4);
  });

  it("filter with gte and lte", async function () {
    const products = await prisma.prismaProduct.findMany({
      where: {
        price: { gte: 15, lte: 35 },
      },
    });
    assert.strictEqual(products.length, 3);
  });

  it("orderBy ASC", async function () {
    const products = await prisma.prismaProduct.findMany({
      orderBy: { price: "asc" },
    });
    for (let i = 1; i < products.length; i++) {
      assert.ok(parseFloat(products[i - 1].price) <= parseFloat(products[i].price));
    }
  });

  it("orderBy DESC", async function () {
    const products = await prisma.prismaProduct.findMany({
      orderBy: { price: "desc" },
    });
    for (let i = 1; i < products.length; i++) {
      assert.ok(parseFloat(products[i - 1].price) >= parseFloat(products[i].price));
    }
  });

  it("take (limit)", async function () {
    const products = await prisma.prismaProduct.findMany({
      take: 3,
      orderBy: { id: "asc" },
    });
    assert.strictEqual(products.length, 3);
  });

  it("skip (offset) pagination", async function () {
    const page1 = await prisma.prismaProduct.findMany({
      take: 2,
      skip: 0,
      orderBy: { id: "asc" },
    });
    const page2 = await prisma.prismaProduct.findMany({
      take: 2,
      skip: 2,
      orderBy: { id: "asc" },
    });
    assert.notStrictEqual(page1[0].id, page2[0].id);
  });

  it("select specific fields", async function () {
    const products = await prisma.prismaProduct.findMany({
      select: { name: true, price: true },
    });
    assert.ok(products[0].name);
    assert.ok(products[0].price);
    assert.strictEqual(products[0].id, undefined);
  });
});

// ---------------------------------------------------------------------------
// Aggregations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM aggregations", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("aggregate sum", async function () {
    const result = await prisma.prismaProduct.aggregate({
      _sum: { stock: true },
    });
    assert.ok(result._sum.stock >= 210);
  });

  it("aggregate avg", async function () {
    const result = await prisma.prismaProduct.aggregate({
      _avg: { price: true },
    });
    assert.ok(parseFloat(result._avg.price) > 0);
  });

  it("aggregate min/max", async function () {
    const result = await prisma.prismaProduct.aggregate({
      _min: { price: true },
      _max: { price: true },
    });
    assert.ok(parseFloat(result._min.price) <= parseFloat(result._max.price));
  });

  it("aggregate count", async function () {
    const result = await prisma.prismaProduct.aggregate({
      _count: true,
    });
    assert.ok(result._count >= 5);
  });

  it("groupBy", async function () {
    const result = await prisma.prismaProduct.groupBy({
      by: ["category"],
      _count: true,
    });
    assert.ok(result.length >= 3);
  });

  it("groupBy with sum", async function () {
    const result = await prisma.prismaProduct.groupBy({
      by: ["category"],
      _sum: { stock: true },
    });
    assert.ok(result.length >= 3);
    result.forEach((r) => assert.ok(r._sum.stock > 0));
  });
});

// ---------------------------------------------------------------------------
// Transactions using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM transactions", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupSchema(prisma);
    await prisma.prismaAccount.deleteMany();
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("$transaction array", async function () {
    await prisma.$transaction([
      prisma.prismaAccount.create({ data: { name: "Account 1", balance: 1000 } }),
      prisma.prismaAccount.create({ data: { name: "Account 2", balance: 500 } }),
    ]);
    const accounts = await prisma.prismaAccount.findMany();
    assert.ok(accounts.length >= 2);
  });

  it("interactive transaction", async function () {
    await prisma.$transaction(async (tx) => {
      await tx.prismaAccount.create({ data: { name: "TX Account", balance: 2000 } });
      const accounts = await tx.prismaAccount.findMany({ where: { name: "TX Account" } });
      assert.strictEqual(accounts.length, 1);
    });

    const accounts = await prisma.prismaAccount.findMany({ where: { name: "TX Account" } });
    assert.strictEqual(accounts.length, 1);
  });

  it("transaction rollback on error", async function () {
    try {
      await prisma.$transaction(async (tx) => {
        await tx.prismaAccount.create({ data: { name: "Rollback Test", balance: 100 } });
        throw new Error("Intentional rollback");
      });
    } catch (e) {
      assert.ok(e.message.includes("Intentional rollback"));
    }

    const accounts = await prisma.prismaAccount.findMany({ where: { name: "Rollback Test" } });
    assert.strictEqual(accounts.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Relations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM relations", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupSchema(prisma);
    await prisma.prismaPost.deleteMany();
    await prisma.prismaUser.deleteMany();
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("create with nested relation", async function () {
    const user = await prisma.prismaUser.create({
      data: {
        email: "author@example.com",
        name: "Author",
        posts: {
          create: [
            { title: "First Post", content: "Content 1" },
            { title: "Second Post", content: "Content 2" },
          ],
        },
      },
      include: { posts: true },
    });
    assert.strictEqual(user.posts.length, 2);
  });

  it("findUnique with include", async function () {
    const user = await prisma.prismaUser.findUnique({
      where: { email: "author@example.com" },
      include: { posts: true },
    });
    assert.ok(user);
    assert.strictEqual(user.posts.length, 2);
  });

  it("findMany posts with author", async function () {
    const posts = await prisma.prismaPost.findMany({
      include: { author: true },
    });
    assert.ok(posts.length >= 2);
    posts.forEach((p) => assert.ok(p.author));
  });

  it("nested filter", async function () {
    const users = await prisma.prismaUser.findMany({
      where: {
        posts: {
          some: { title: { contains: "First" } },
        },
      },
    });
    assert.ok(users.length >= 1);
  });

  it("count relations", async function () {
    const users = await prisma.prismaUser.findMany({
      include: {
        _count: { select: { posts: true } },
      },
    });
    const author = users.find((u) => u.email === "author@example.com");
    assert.strictEqual(author._count.posts, 2);
  });
});

// ---------------------------------------------------------------------------
// JSON operations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM JSON operations", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupSchema(prisma);
    await prisma.prismaProduct.deleteMany({
      where: { name: { startsWith: "JSON" } },
    });
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("create with JSON field", async function () {
    const product = await prisma.prismaProduct.create({
      data: {
        name: "JSON Product",
        price: 99.99,
        stock: 5,
        metadata: { color: "red", tags: ["sale", "featured"], rating: 4.5 },
      },
    });
    assert.ok(product.metadata);
    assert.strictEqual(product.metadata.color, "red");
  });

  it("update JSON field", async function () {
    const product = await prisma.prismaProduct.findFirst({ where: { name: "JSON Product" } });
    const updated = await prisma.prismaProduct.update({
      where: { id: product.id },
      data: {
        metadata: { color: "blue", tags: ["clearance"], rating: 3.5 },
      },
    });
    assert.strictEqual(updated.metadata.color, "blue");
  });

  it("filter by JSON path", async function () {
    const products = await prisma.prismaProduct.findMany({
      where: {
        metadata: { path: ["color"], equals: "blue" },
      },
    });
    assert.ok(products.length >= 1);
  });
});

// ---------------------------------------------------------------------------
// Update operations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM update operations", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupSchema(prisma);
  });

  after(async function () {
    await prisma.$disconnect();
    await teardownAdmin();
  });

  it("updateMany", async function () {
    await prisma.prismaProduct.updateMany({
      where: { category: "widgets" },
      data: { stock: 999 },
    });
    const products = await prisma.prismaProduct.findMany({
      where: { category: "widgets" },
    });
    products.forEach((p) => assert.strictEqual(p.stock, 999));
  });

  it("increment field", async function () {
    const product = await prisma.prismaProduct.findFirst({
      where: { category: "tools" },
    });
    const updated = await prisma.prismaProduct.update({
      where: { id: product.id },
      data: { stock: { increment: 10 } },
    });
    assert.strictEqual(updated.stock, product.stock + 10);
  });

  it("decrement field", async function () {
    const product = await prisma.prismaProduct.findFirst({
      where: { category: "tools" },
    });
    const updated = await prisma.prismaProduct.update({
      where: { id: product.id },
      data: { stock: { decrement: 5 } },
    });
    assert.strictEqual(updated.stock, product.stock - 5);
  });
});
