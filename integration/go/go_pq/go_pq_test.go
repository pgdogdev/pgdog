package main

import (
	"database/sql"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
)

func PqConnections() []*sql.DB {

	normal, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable")

	if err != nil {
		panic(err)
	}

	sharded, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?sslmode=disable")

	if err != nil {
		panic(err)
	}

	return []*sql.DB{normal, sharded}
}

func TestPqCrud(t *testing.T) {
	conns := PqConnections()

	for _, conn := range conns {
		defer conn.Close()
		for range 25 {
			tx, err := conn.Begin()

			assert.Nil(t, err)
			id := rand.Intn(1_000_000)
			rows, err := tx.Query("INSERT INTO sharded (id) VALUES ($1) RETURNING id", id)

			assert.Nil(t, err)

			var len int
			var id_val int64
			for rows.Next() {
				rows.Scan(&id_val)
				len += 1
				assert.Equal(t, id_val, int64(id))
			}
			assert.Equal(t, len, 1)

			rows, err = tx.Query("SELECT id FROM sharded WHERE id = $1", id)
			assert.Nil(t, err)

			len = 0
			id_val = 0
			for rows.Next() {
				rows.Scan(&id_val)
				len += 1
				assert.Equal(t, id_val, int64(id))
			}
			assert.Equal(t, len, 1)

			err = tx.Rollback()
			assert.Nil(t, err)
		}
	}
}
