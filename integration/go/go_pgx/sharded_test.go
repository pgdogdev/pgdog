package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestShardedVarchar(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	conn.Exec(context.Background(), "TRUNCATE TABLE sharded_varchar")

	for i := range 100 {
		str := fmt.Sprintf("%d_test_%d", i, i)
		rows, err := conn.Query(context.Background(), "INSERT INTO sharded_varchar (id_varchar) VALUES ($1) RETURNING *", str)
		assert.NoError(t, err)

		var len int
		for rows.Next() {
			len += 1
		}
		rows.Close()
		assert.Equal(t, 1, len)

		rows, err = conn.Query(context.Background(), "SELECT * FROM sharded_varchar WHERE id_varchar IN ($1) ", str)
		assert.NoError(t, err)

		len = 0
		for rows.Next() {
			values, err := rows.Values()
			assert.NoError(t, err)
			value := values[0].(string)
			assert.Equal(t, value, str)
			len += 1
		}
		rows.Close()
		assert.Equal(t, 1, len)
	}
}

func TestShardedVarcharArray(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	conn.Exec(context.Background(), "TRUNCATE TABLE sharded_varchar")
	values := [7]string{"one", "two", "three", "four", "five", "six", "seven"}

	for _, value := range values {
		conn.Exec(context.Background(), "INSERT INTO sharded_varchar (id_varchar) VALUES ($1)", value)
	}

	for range 100 {
		rows, err := conn.Query(context.Background(), "SELECT * FROM sharded_varchar WHERE id_varchar = ANY($1)", [5]string{"one", "two", "three", "four", "five"})
		assert.NoError(t, err)
		rows.Close()
	}
}

func TestShardedList(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "TRUNCATE TABLE sharded_list")
	assert.NoError(t, err)

	for i := range 20 {
		for _, query := range [4]string{
			"INSERT INTO sharded_list (id) VALUES ($1) RETURNING *",
			"SELECT * FROM sharded_list WHERE id = $1",
			"UPDATE sharded_list SET id = $1 WHERE id = $1 RETURNING *",
			"DELETE FROM sharded_list WHERE id = $1 RETURNING *",
		} {
			rows, err := conn.Query(context.Background(), query, int64(i))
			assert.NoError(t, err)
			count := 0

			for rows.Next() {
				count += 1
			}

			rows.Close()
			assert.Equal(t, 1, count)
		}
	}
}

func TestShardedRange(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "TRUNCATE TABLE sharded_range")
	assert.NoError(t, err)

	for i := range 200 {
		for _, query := range [4]string{
			"INSERT INTO sharded_range (id) VALUES ($1) RETURNING *",
			"SELECT * FROM sharded_range WHERE id = $1",
			"UPDATE sharded_range SET id = $1 WHERE id = $1 RETURNING *",
			"DELETE FROM sharded_range WHERE id = $1 RETURNING *",
		} {
			rows, err := conn.Query(context.Background(), query, int64(i))
			assert.NoError(t, err)
			count := 0

			for rows.Next() {
				count += 1
			}

			rows.Close()
			assert.Equal(t, 1, count)
		}
	}
}
