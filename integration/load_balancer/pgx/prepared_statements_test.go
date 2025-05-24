package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestPrepared(t *testing.T) {
	done := make(chan int)
	iterations := 10

	for range iterations {
		// Creating separate pools in purpose.
		pool := GetPool()
		defer pool.Close()

		go func() {
			runPrepared(t, pool, 500)
			done <- 1
		}()
	}

	for range iterations {
		<-done
	}
}

func runPrepared(t *testing.T, pool *pgxpool.Pool, iterations int) {
	for range iterations {
		_, err := pool.Exec(context.Background(),
			"SELECT $1::bigint, $2::text, $3::real", int64(1), "hello world", float32(25.0))
		assert.NoError(t, err)

		tx, err := pool.Begin(context.Background())
		assert.NoError(t, err)

		_, err = tx.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS run_prepared (
				id BIGINT,
				value VARCHAR
			)`)

		assert.NoError(t, err)

		rows, err := tx.Query(context.Background(),
			"INSERT INTO run_prepared (id, value) VALUES ($1, $2), ($3, $4) RETURNING *",
			int64(1), "hello world", int64(2), "bye world")
		assert.NoError(t, err)
		rows.Close()

		rows, err = tx.Query(context.Background(), "SELECT * FROM run_prepared")
		assert.NoError(t, err)
		rows.Close()

		err = tx.Rollback(context.Background())
		assert.NoError(t, err)

		_, err = pool.Exec(context.Background(),
			"SELECT * FROM (SELECT $1::bigint, $2::text)", int64(1), "hello world")
		assert.NoError(t, err)

		_, err = pool.Exec(context.Background(),
			"SELECT *, NOW() FROM (SELECT $1::bigint, $2::text, current_user)", int64(1), "hello world")
		assert.NoError(t, err)

		// Generate 25 prepared statements.
		for i := range 25 {
			query := fmt.Sprintf(`SELECT
					*,
					NOW(),
					5 + %d
				FROM (
					SELECT $1::bigint, $2::text, current_user
				)`, i)

			_, err = pool.Exec(context.Background(), query, int64(1), "hello world")
			assert.NoError(t, err)
		}
	}
}
