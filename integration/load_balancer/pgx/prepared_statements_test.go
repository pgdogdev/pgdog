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
	concurrency := 100

	for range concurrency {
		// Creating separate pools in purpose.
		pool := GetPool()
		defer pool.Close()

		go func() {
			runPrepared(t, pool, 20)
			done <- 1
		}()
	}

	for range concurrency {
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

		// Generate 150 prepared statements.
		for i := range 150 {
			query := fmt.Sprintf(`SELECT
					*,
					NOW(),
					(5 + %d)::bigint
				FROM (
					SELECT $1::bigint, $2::text, current_user
				)`, i)

			rows, err := pool.Query(context.Background(), query, int64(i), fmt.Sprintf("hello world %d", i))
			var count int
			for rows.Next() {
				values, err := rows.Values()
				assert.NoError(t, err)
				assert.Equal(t, values[0].(int64), int64(i))
				assert.Equal(t, values[1].(string), fmt.Sprintf("hello world %d", i))
				assert.Equal(t, values[4].(int64), int64(5+i))
				count += 1
			}
			rows.Close()
			assert.Equal(t, count, 1)

			assert.NoError(t, err)
		}
	}
}
