package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

const createTables = `
CREATE TABLE IF NOT EXISTS companies (
	company_id BIGSERIAL PRIMARY KEY,
	name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
	user_id BIGSERIAL PRIMARY KEY,
	company_id BIGINT NOT NULL,
	username TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notes (
	note_id BIGSERIAL PRIMARY KEY,
	company_id BIGINT NOT NULL,
	user_id BIGINT NOT NULL,
	content TEXT NOT NULL
);
`

const dropTables = `
DROP TABLE IF EXISTS notes;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS companies;
`

func migrate(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), createTables)
	return err
}

func dropAll(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), dropTables)
	return err
}

var readQueries = []struct {
	q    string
	args []any
}{
	{"SELECT * FROM companies WHERE company_id = $1;", []any{1}},
	{"SELECT username FROM users WHERE user_id = $1;", []any{1}},
	{"SELECT COUNT(*) FROM notes WHERE company_id = $1;", []any{1}},
	{"SELECT note_id, content FROM notes WHERE company_id = $1;", []any{1}},
	{"SELECT u.username, n.content FROM users u JOIN notes n ON u.user_id = n.user_id WHERE u.company_id = $1", []any{1}},
	{"SELECT company_id FROM companies WHERE name LIKE $1", []any{"A%"}},
	{"SELECT EXISTS(SELECT 1 FROM users WHERE username = $1);", []any{"bob"}},
	{"SELECT n.content FROM notes n JOIN users u ON n.user_id = u.user_id WHERE u.username = $1;", []any{"bob"}},
	{"SELECT * FROM users WHERE company_id IN (SELECT company_id FROM companies WHERE name = $1);", []any{"TestCo"}},
	{"SELECT COUNT(*) FROM companies WHERE name = $1;", []any{"TestCo"}},
}

var writeQueries = []struct {
	q    string
	args []any
}{
	{"INSERT INTO companies(name) VALUES($1)", []any{"ACME Inc."}},
	{"INSERT INTO users(company_id, username) VALUES($1, $2);", []any{1, "bob"}},
	{"INSERT INTO notes(company_id, user_id, content) VALUES($1, $2, $3);", []any{1, 1, "Hello!"}},
	{"UPDATE users SET username = $1 WHERE user_id = $2;", []any{"alice", 1}},
	{"UPDATE notes SET content = $1 WHERE note_id = $2;", []any{"Updated", 1}},
	{"DELETE FROM notes WHERE note_id = $1", []any{1}},
	{"DELETE FROM users WHERE user_id = $1;", []any{1}},
	{"TRUNCATE notes;", nil},
	{"INSERT INTO companies(name) VALUES($1);", []any{"Globex"}},
	{"UPDATE companies SET name = $1 WHERE company_id = $2;", []any{"MegaCorp", 1}},
}

func runTest(t *testing.T) {
	ctx := context.Background()
	dsn := "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable"

	config, err := pgxpool.ParseConfig(dsn)
	assert.NoError(t, err)
	config.MaxConns = 32 // increase pool size for heavy test concurrency

	pool, err := pgxpool.NewWithConfig(ctx, config)
	assert.NoError(t, err)
	defer pool.Close()

	err = migrate(pool)
	assert.NoError(t, err)

	defer func() {
		_ = dropAll(pool)
	}()

	_, err = pool.Exec(ctx, "INSERT INTO companies(name) VALUES($1) ON CONFLICT DO NOTHING;", "TestCo")
	assert.NoError(t, err)
	_, err = pool.Exec(ctx, "INSERT INTO users(company_id, username) VALUES($1, $2) ON CONFLICT DO NOTHING", 1, "bob")
	assert.NoError(t, err)
	_, err = pool.Exec(ctx, "INSERT INTO notes(company_id, user_id, content) VALUES($1, $2, $3) ON CONFLICT DO NOTHING;", 1, 1, "Initial Note")
	assert.NoError(t, err)

	t.Run("Read queries are handled as reads", func(t *testing.T) {
		for i, q := range readQueries {
			t.Run(fmt.Sprintf("read_query_%d", i), func(t *testing.T) {
				t.Parallel()
				rows, err := pool.Query(ctx, q.q, q.args...)
				rows.Close()
				assert.NoError(t, err, "Query failed: %s", q.q)
			})
		}
	})

	t.Run("Write queries are handled as writes", func(t *testing.T) {
		for i, q := range writeQueries {
			t.Run(fmt.Sprintf("write_query_%d", i), func(t *testing.T) {
				// DO NOT parallelize writes! Leave out t.Parallel() here.
				if q.args == nil {
					_, err := pool.Exec(ctx, q.q)
					assert.NoError(t, err, "Query failed: %s", q.q)
				} else {
					_, err := pool.Exec(ctx, q.q, q.args...)
					assert.NoError(t, err, "Query failed: %s", q.q)
				}
			})
		}
	})
}

func TestRoundRobinWithPrimary(t *testing.T) {
	adminCommand(t, "RELOAD")
	adminCommand(t, "SET load_balancing_strategy TO 'round_robin'")
	runTest(t)
}

func adminCommand(t *testing.T, command string) {
	conn, err := pgx.Connect(context.Background(), "postgres://admin:pgdog@127.0.0.1:6432/admin")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), command, pgx.QueryExecModeSimpleProtocol)
	defer rows.Close()
}

func getTransactionsAndQueries(t *testing.T, role string) (pgtype.Numeric, pgtype.Numeric) {
	conn, err := pgx.Connect(context.Background(), "postgres://admin:pgdog@127.0.0.1:6432/admin")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SHOW STATS", pgx.QueryExecModeSimpleProtocol)
	defer rows.Close()

	assert.NoError(t, err)

	var totalQueryCount pgtype.Numeric
	var totalTransactionCount pgtype.Numeric

outer:
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			panic(err)
		}

		for i, description := range rows.FieldDescriptions() {
			if description.Name == "database" {
				db := values[i].(string)
				if db != "pgdog" {
					continue outer
				}
			}

			if description.Name == "user" {
				db := values[i].(string)
				if db != "pgdog" {
					continue outer
				}
			}

			if description.Name == "role" {
				db_role := values[i].(string)
				if db_role != role {
					continue outer
				}
			}

			if description.Name == "total_xact_count" {
				totalTransactionCount = values[i].(pgtype.Numeric)
			}

			if description.Name == "total_query_count" {
				totalQueryCount = values[i].(pgtype.Numeric)
			}
		}
	}

	return totalQueryCount, totalTransactionCount
}
