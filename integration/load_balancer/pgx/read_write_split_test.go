package main

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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
