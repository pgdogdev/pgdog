import { Sequelize, DataTypes } from "sequelize";

const sequelize = new Sequelize("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog", {
  logging: false,
  disableClsTransactions: true,
  dialectOptions: {
    autocommit: true,
  },
  pool: {
    max: 20,
    min: 20,
    acquire: 60000,
    idle: 10000,
  },
});

const Author = sequelize.define("Author", {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false,
  },
});

const Book = sequelize.define("Book", {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  title: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  authorId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    references: {
      model: Author,
      key: "id",
    },
  },
});

Author.hasMany(Book, { foreignKey: "authorId" });
Book.belongsTo(Author, { foreignKey: "authorId" });

async function insertAuthorAndBook(i) {
  const author = await Author.create({ name: `Author ${i}` });
  const book = await Book.create({ title: `Book ${i}`, authorId: author.id });
  return { author, book };
}

async function main() {
  await sequelize.sync({ force: true });

  const iterations = 500000;
  const batchSize = 1000;

  console.log(`Starting ${iterations} insert pairs in batches of ${batchSize} (pool: 20)`);

  let completed = 0;
  let errors = 0;
  const errorDetails = {};
  const start = Date.now();

  for (let batch = 0; batch < iterations; batch += batchSize) {
    const batchEnd = Math.min(batch + batchSize, iterations);
    const promises = [];

    for (let i = batch; i < batchEnd; i++) {
      promises.push(
        insertAuthorAndBook(i)
          .then(() => {
            completed++;
          })
          .catch((err) => {
            errors++;
            const key = err.message.substring(0, 60);
            errorDetails[key] = (errorDetails[key] || 0) + 1;
            if (errors <= 10) {
              console.error(`Error on iteration ${i}: ${err.message}`);
            }
          }),
      );
    }

    await Promise.allSettled(promises);

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    const rate = (completed / (Date.now() - start) * 1000).toFixed(0);
    console.log(`Batch ${batch / batchSize + 1}: ${completed}/${iterations} (${elapsed}s, ${rate}/s, errors: ${errors})`);
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\nFinished in ${elapsed}s`);
  console.log(`Results: Completed: ${completed}, Errors: ${errors}`);

  if (Object.keys(errorDetails).length > 0) {
    console.log("Error breakdown:");
    for (const [msg, count] of Object.entries(errorDetails)) {
      console.log(`  ${msg}...: ${count}`);
    }
  }

  const authorCount = await Author.count();
  const bookCount = await Book.count();
  console.log(`Authors: ${authorCount}, Books: ${bookCount}`);

  await sequelize.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
