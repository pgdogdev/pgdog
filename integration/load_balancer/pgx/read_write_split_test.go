package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

type TestTable struct {
	id         int64
	email      string
	created_at pgtype.Timestamptz
}

func TestSelect(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	ResetStats()

	cmd, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pgx_test_select (
		id BIGINT,
		email VARCHAR,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), cmd.RowsAffected())

	calls := LoadStatsForPrimary("CREATE TABLE IF NOT EXISTS lb_pgx_test_select")
	assert.Equal(t, int64(1), calls.Calls)

	// Equalize round robin after connect.
	_, err = pool.Exec(context.Background(), "SELECT 1")
	assert.NoError(t, err)

	// Wait for replicas to catch up.
	time.Sleep(2 * time.Second)

	for i := range 50 {
		_, err = pool.Exec(context.Background(), "SELECT $1::bigint, now() FROM lb_pgx_test_select LIMIT 1", int64(i))
		assert.NoError(t, err)
		_, err = pool.Exec(context.Background(), `
			WITH t AS (SELECT $1::bigint AS val)
			SELECT * FROM lb_pgx_test_select
			WHERE id = (SELECT val FROM t) AND email = $2`, int64(i), fmt.Sprintf("test-%d@test.com", i))
		assert.NoError(t, err)
		_, err = pool.Exec(context.Background(), "SELECT * FROM lb_pgx_test_select LIMIT 1")
		assert.NoError(t, err)
	}

	replicaCalls := LoadStatsForReplicas("lb_pgx_test_select")
	assert.Equal(t, 2, len(replicaCalls))

	for _, call := range replicaCalls {
		assert.True(t, int64(math.Abs(float64(call.Calls-75))) <= 1)
	}

	_, err = pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pgx_test_select")
	assert.NoError(t, err)
}

func TestWrites(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	ResetStats()

	cmd, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pgx_test_writes (
		id BIGINT,
		email VARCHAR,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)

	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pgx_test_writes")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), cmd.RowsAffected())

	calls := LoadStatsForPrimary("CREATE TABLE IF NOT EXISTS lb_pgx_test_writes")
	assert.Equal(t, int64(1), calls.Calls)

	parallel := make(chan int)

	for i := range 50 {
		id := int64(i)
		email := fmt.Sprintf("test-%d@test.com", i)

		go func() {
			rows, err := pool.Query(context.Background(), `INSERT INTO lb_pgx_test_writes (id, email, created_at) VALUES ($1, $2, NOW()) RETURNING *`, i, email)
			assert.NoError(t, err)

			for rows.Next() {
				var result TestTable
				rows.Scan(&result.id, &result.email, &result.created_at)

				assert.Equal(t, id, result.id)
				assert.Equal(t, email, result.email)
			}

			parallel <- 1
		}()

		go func() {
			_, err := pool.Exec(context.Background(), `UPDATE lb_pgx_test_writes SET created_at = NOW() WHERE id = $1 RETURNING *`, i)
			assert.NoError(t, err)
			parallel <- 1
		}()

		go func() {
			_, err := pool.Exec(context.Background(), `DELETE FROM lb_pgx_test_writes WHERE id = $1 RETURNING *`, i)
			assert.NoError(t, err)
			parallel <- 1
		}()
	}

	for range 50 * 3 {
		<-parallel
	}

	calls = LoadStatsForPrimary("INSERT INTO lb_pgx_test_writes")
	assert.Equal(t, int64(50), calls.Calls)

	calls = LoadStatsForPrimary("UPDATE lb_pgx_test_writes")
	assert.Equal(t, int64(50), calls.Calls)

	calls = LoadStatsForPrimary("DELETE FROM lb_pgx_test_writes")
	assert.Equal(t, int64(50), calls.Calls)
}

func TestWriteFunctions(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	ResetStats()

	for i := range 25 {
		_, err := pool.Exec(context.Background(), "SELECT pg_advisory_lock($1), pg_advisory_unlock($1)", i)
		assert.NoError(t, err)
	}

	calls := LoadStatsForPrimary("SELECT pg_advisory_lock")
	assert.Equal(t, int64(25), calls.Calls)
}

func withTransaction(t *testing.T, pool *pgxpool.Pool, f func(t pgx.Tx) error) error {
	tx, err := pool.Begin(context.Background())
	assert.NoError(t, err)

	err = f(tx)

	if err != nil {
		return tx.Rollback(context.Background())
	} else {
		return tx.Commit(context.Background())
	}
}

func runTransactionWithError(tx pgx.Tx) error {
	_, err := tx.Exec(context.Background(), "SELECT ROW(t.*, 1, 2, 3) FROM test_transactions_with_func t")

	if err != nil {
		return err
	}

	return errors.New("error")
}

func runTransactionWithoutError(tx pgx.Tx) error {
	_, err := tx.Exec(context.Background(), "SELECT * FROM test_transactions_with_func")

	if err != nil {
		return err
	}

	_, err = tx.Exec(context.Background(), "SELECT pg_advisory_xact_lock(12345)")

	_, err = tx.Exec(context.Background(), "INSERT INTO test_transactions_with_func (id, email) VALUES ($1, $2)", 1, "apple@gmail.com")

	rows, err := tx.Query(context.Background(), "SELECT * FROM test_transactions_with_func")

	for rows.Next() {
	}

	rows.Close()

	if err != nil {
		return err
	}

	return nil
}

func TestTransactionsWithFunc(t *testing.T) {
	pool := GetPool()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS test_transactions_with_func (
		id BIGINT NOT NULL,
		email VARCHAR,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)

	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_transactions_with_func")

	c := make(chan int)

	for range 50 {
		go func() {
			err = withTransaction(t, pool, runTransactionWithError)
			assert.NoError(t, err)
			err = withTransaction(t, pool, runTransactionWithoutError)
			assert.NoError(t, err)
			err = withTransaction(t, pool, runTransactionWithError)
			assert.NoError(t, err)
			err = withTransaction(t, pool, runTransactionWithoutError)
			assert.NoError(t, err)
			c <- 1
		}()
	}

	for range 50 {
		<-c
	}

	assert.NoError(t, err)
}

func TestReadOnlyTransaction(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	ResetStats()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pgx_test_readonly (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pgx_test_readonly")

	// Wait for replicas to catch up
	time.Sleep(2 * time.Second)

	done := make(chan int)

	for i := range 20 {
		go func(clientId int) {
			tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{AccessMode: pgx.ReadOnly})
			assert.NoError(t, err)

			for j := range 5 {
				_, err = tx.Exec(context.Background(), "SELECT $1::bigint, $2::bigint FROM lb_pgx_test_readonly", int64(clientId), int64(j))
				assert.NoError(t, err)
			}

			err = tx.Commit(context.Background())
			assert.NoError(t, err)
			done <- 1
		}(i)
	}

	for range 20 {
		<-done
	}

	replicaCalls := LoadStatsForReplicas("lb_pgx_test_readonly")
	assert.Equal(t, 2, len(replicaCalls))

	totalCalls := int64(0)
	for _, call := range replicaCalls {
		totalCalls += call.Calls
	}
	assert.Equal(t, int64(100), totalCalls)
}

func TestRegularTransaction(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pgx_test_regular (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pgx_test_regular")

	// Wait for replicas to catch up
	time.Sleep(2 * time.Second)

	ResetStats()

	done := make(chan int)

	for i := range 20 {
		go func(clientId int) {
			tx, err := pool.Begin(context.Background())
			assert.NoError(t, err)

			for j := range 5 {
				_, err = tx.Exec(context.Background(), "SELECT $1::bigint, $2::bigint FROM lb_pgx_test_regular", int64(clientId), int64(j))
				assert.NoError(t, err)
			}

			err = tx.Commit(context.Background())
			assert.NoError(t, err)
			done <- 1
		}(i)
	}

	for range 20 {
		<-done
	}

	primaryCalls := LoadStatsForPrimary("lb_pgx_test_regular")
	assert.Equal(t, int64(100), primaryCalls.Calls)
}
