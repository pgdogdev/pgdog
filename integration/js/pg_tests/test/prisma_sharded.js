import { PrismaClient } from "@prisma/client";
import pg from "pg";
import assert from "assert";

const DATABASE_URL = "postgresql://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded";
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
// Schema setup - create tables for sharded database
// ---------------------------------------------------------------------------

async function setupShardedSchema(prisma) {
  // Prisma ORM always uses schema-qualified names (public.tablename), so we must
  // create tables explicitly in the public schema
  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.sharded_orders (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      total DECIMAL(10, 2),
      status TEXT
    )
  `;

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.sharded_order_items (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      order_id BIGINT NOT NULL,
      product_name TEXT NOT NULL,
      quantity INT,
      price DECIMAL(10, 2)
    )
  `;

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.sharded_customer_events (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      event_type TEXT NOT NULL,
      payload JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS public.sharded_metrics (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      name TEXT,
      value INT
    )
  `;
}

// ---------------------------------------------------------------------------
// Sharded CRUD using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM sharded CRUD", function () {
  let prisma;

  before(async function () {
    await setupAdmin();
    prisma = createPrisma();
    await prisma.$connect();
    await setupShardedSchema(prisma);
    await prisma.shardedOrderItem.deleteMany();
    await prisma.shardedOrder.deleteMany();
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("create order with ORM", async function () {
    const order = await prisma.shardedOrder.create({
      data: {
        id: 1n,
        customerId: 1n,
        total: 100.0,
        status: "pending",
      },
    });
    assert.strictEqual(order.customerId, 1n);
    assert.strictEqual(order.status, "pending");
  });

  it("findMany by customer_id", async function () {
    await prisma.shardedOrder.createMany({
      data: [
        { id: 2n, customerId: 1n, total: 200.0, status: "pending" },
        { id: 3n, customerId: 1n, total: 300.0, status: "shipped" },
      ],
    });

    const orders = await prisma.shardedOrder.findMany({
      where: { customerId: 1n },
    });
    assert.strictEqual(orders.length, 3);
  });

  it("findFirst by customer_id", async function () {
    const order = await prisma.shardedOrder.findFirst({
      where: { customerId: 1n },
    });
    assert.ok(order);
    assert.strictEqual(order.customerId, 1n);
  });

  it("findUnique by id", async function () {
    const order = await prisma.shardedOrder.findUnique({
      where: { id: 1n },
    });
    assert.ok(order);
    assert.strictEqual(order.id, 1n);
  });

  it("update by id", async function () {
    const updated = await prisma.shardedOrder.update({
      where: { id: 1n },
      data: { status: "completed" },
    });
    assert.strictEqual(updated.status, "completed");
  });

  it("updateMany by customer_id", async function () {
    await prisma.shardedOrder.updateMany({
      where: { customerId: 1n },
      data: { status: "archived" },
    });
    const orders = await prisma.shardedOrder.findMany({
      where: { customerId: 1n },
    });
    orders.forEach((o) => assert.strictEqual(o.status, "archived"));
  });

  it("delete by id", async function () {
    await prisma.shardedOrder.delete({
      where: { id: 3n },
    });
    const order = await prisma.shardedOrder.findUnique({
      where: { id: 3n },
    });
    assert.strictEqual(order, null);
  });

  it("deleteMany by customer_id", async function () {
    await prisma.shardedOrder.deleteMany({
      where: { customerId: 1n },
    });
    const orders = await prisma.shardedOrder.findMany({
      where: { customerId: 1n },
    });
    assert.strictEqual(orders.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Sharded aggregations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM sharded aggregations", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupShardedSchema(prisma);

    await prisma.shardedOrder.createMany({
      data: [
        { id: 100n, customerId: 100n, total: 100.0, status: "completed" },
        { id: 101n, customerId: 100n, total: 200.0, status: "pending" },
        { id: 102n, customerId: 100n, total: 300.0, status: "completed" },
      ],
    });
  });

  after(async function () {
    await prisma.shardedOrder.deleteMany({ where: { customerId: 100n } });
    await prisma.$disconnect();
  });

  it("count by customer_id", async function () {
    const count = await prisma.shardedOrder.count({
      where: { customerId: 100n },
    });
    assert.strictEqual(count, 3);
  });

  it("aggregate sum by customer_id", async function () {
    const result = await prisma.shardedOrder.aggregate({
      where: { customerId: 100n },
      _sum: { total: true },
    });
    assert.strictEqual(parseFloat(result._sum.total), 600.0);
  });

  it("aggregate max by customer_id", async function () {
    const result = await prisma.shardedOrder.aggregate({
      where: { customerId: 100n },
      _max: { total: true },
    });
    assert.strictEqual(parseFloat(result._max.total), 300.0);
  });

  it("aggregate min by customer_id", async function () {
    const result = await prisma.shardedOrder.aggregate({
      where: { customerId: 100n },
      _min: { total: true },
    });
    assert.strictEqual(parseFloat(result._min.total), 100.0);
  });

  it("groupBy status for customer", async function () {
    const result = await prisma.shardedOrder.groupBy({
      by: ["status"],
      where: { customerId: 100n },
      _count: true,
    });
    assert.ok(result.length >= 2);
  });
});

// ---------------------------------------------------------------------------
// Sharded transactions using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM sharded transactions", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupShardedSchema(prisma);
  });

  after(async function () {
    await prisma.$disconnect();
  });

  it("transaction with multiple creates", async function () {
    const customerId = 200n;
    await prisma.$transaction([
      prisma.shardedOrder.create({
        data: { id: 200n, customerId, total: 500.0, status: "pending" },
      }),
      prisma.shardedOrder.create({
        data: { id: 201n, customerId, total: 600.0, status: "pending" },
      }),
    ]);

    const orders = await prisma.shardedOrder.findMany({
      where: { customerId },
    });
    assert.strictEqual(orders.length, 2);

    await prisma.shardedOrder.deleteMany({ where: { customerId } });
  });

  it("interactive transaction", async function () {
    const customerId = 201n;
    await prisma.$transaction(async (tx) => {
      await tx.shardedOrder.create({
        data: { id: 202n, customerId, total: 700.0, status: "pending" },
      });
      const orders = await tx.shardedOrder.findMany({
        where: { customerId },
      });
      assert.strictEqual(orders.length, 1);
    });

    const orders = await prisma.shardedOrder.findMany({
      where: { customerId },
    });
    assert.strictEqual(orders.length, 1);

    await prisma.shardedOrder.deleteMany({ where: { customerId } });
  });

  it("transaction rollback on error", async function () {
    const customerId = 202n;
    try {
      await prisma.$transaction(async (tx) => {
        await tx.shardedOrder.create({
          data: { id: 203n, customerId, total: 800.0, status: "pending" },
        });
        throw new Error("Intentional rollback");
      });
    } catch (e) {
      assert.ok(e.message.includes("Intentional rollback"));
    }

    const orders = await prisma.shardedOrder.findMany({
      where: { customerId },
    });
    assert.strictEqual(orders.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Sharded relations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM sharded relations", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupShardedSchema(prisma);

    const customerId = 300n;
    await prisma.shardedOrder.create({
      data: { id: 300n, customerId, total: 250.0, status: "completed" },
    });
    await prisma.shardedOrderItem.createMany({
      data: [
        { id: 300n, customerId, orderId: 300n, productName: "Widget", quantity: 2, price: 50.0 },
        { id: 301n, customerId, orderId: 300n, productName: "Gadget", quantity: 1, price: 150.0 },
      ],
    });
  });

  after(async function () {
    await prisma.shardedOrderItem.deleteMany({ where: { customerId: 300n } });
    await prisma.shardedOrder.deleteMany({ where: { customerId: 300n } });
    await prisma.$disconnect();
  });

  it("findUnique with include items", async function () {
    const order = await prisma.shardedOrder.findUnique({
      where: { id: 300n },
      include: { items: true },
    });
    assert.ok(order);
    assert.strictEqual(order.items.length, 2);
  });

  it("findMany items by orderId", async function () {
    const items = await prisma.shardedOrderItem.findMany({
      where: { orderId: 300n },
    });
    assert.strictEqual(items.length, 2);
  });

  it("aggregate items", async function () {
    const result = await prisma.shardedOrderItem.aggregate({
      where: { orderId: 300n },
      _sum: { quantity: true },
      _count: true,
    });
    assert.strictEqual(result._sum.quantity, 3);
    assert.strictEqual(result._count, 2);
  });
});

// ---------------------------------------------------------------------------
// Sharded JSON operations using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM sharded JSON", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupShardedSchema(prisma);
  });

  after(async function () {
    await prisma.shardedCustomerEvent.deleteMany({ where: { customerId: 400n } });
    await prisma.$disconnect();
  });

  it("create with JSON payload", async function () {
    const event = await prisma.shardedCustomerEvent.create({
      data: {
        id: 400n,
        customerId: 400n,
        eventType: "purchase",
        payload: { items: ["widget", "gadget"], amount: 99.99 },
      },
    });
    assert.ok(event.payload);
    assert.deepStrictEqual(event.payload.items, ["widget", "gadget"]);
  });

  it("findMany with JSON field", async function () {
    const events = await prisma.shardedCustomerEvent.findMany({
      where: { customerId: 400n },
    });
    assert.ok(events.length >= 1);
    assert.ok(events[0].payload);
  });

  it("update JSON payload", async function () {
    const updated = await prisma.shardedCustomerEvent.update({
      where: { id: 400n },
      data: {
        payload: { items: ["tool"], amount: 50.0, discount: true },
      },
    });
    assert.strictEqual(updated.payload.discount, true);
  });
});

// ---------------------------------------------------------------------------
// Cross-shard queries using Prisma ORM
// ---------------------------------------------------------------------------

describe("Prisma ORM cross-shard queries", function () {
  let prisma;

  before(async function () {
    prisma = createPrisma();
    await prisma.$connect();
    await setupShardedSchema(prisma);
    await prisma.shardedMetric.deleteMany();

    let id = 1n;
    for (const customerId of [900n, 901n, 902n, 903n, 904n]) {
      for (let i = 0; i < 4; i++) {
        await prisma.shardedMetric.create({
          data: {
            id: id,
            customerId: customerId,
            name: `metric_${i}`,
            value: Number(customerId) + i,
          },
        });
        id++;
      }
    }
  });

  after(async function () {
    await prisma.$disconnect();
    await teardownAdmin();
  });

  it("count all rows across shards", async function () {
    const count = await prisma.shardedMetric.count();
    assert.ok(count >= 20);
  });

  it("findMany without customer_id filter", async function () {
    const metrics = await prisma.shardedMetric.findMany({
      orderBy: { id: "asc" },
    });
    assert.ok(metrics.length >= 20);
  });

  it("findMany with take (limit) across shards", async function () {
    const metrics = await prisma.shardedMetric.findMany({
      orderBy: { id: "asc" },
      take: 10,
    });
    assert.strictEqual(metrics.length, 10);
  });

  it("aggregate sum across all shards", async function () {
    const result = await prisma.shardedMetric.aggregate({
      _sum: { value: true },
    });
    assert.ok(result._sum.value > 0);
  });

  it("groupBy name across shards", async function () {
    const result = await prisma.shardedMetric.groupBy({
      by: ["name"],
      _count: true,
      _sum: { value: true },
      orderBy: { name: "asc" },
    });
    assert.strictEqual(result.length, 4);
  });
});
