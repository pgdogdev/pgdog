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

		rows, err = conn.Query(context.Background(), "SELECT * FROM sharded_varchar WHERE id_varchar = $1 ", str)
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
