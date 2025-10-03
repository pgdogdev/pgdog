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

func TestAuthenticationWithoutTLS(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.Nil(t, err)
	defer conn.Close()

	err = conn.Ping()
	assert.Nil(t, err)

	// Reset config
	adminConn, err := sql.Open("postgres", "postgres://admin:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	assert.Nil(t, err)
	defer adminConn.Close()

	_, err = adminConn.Exec("RELOAD")
	assert.Nil(t, err)
}

func TestAuthenticationWithTLS(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=require")
	assert.Nil(t, err)
	defer conn.Close()

	err = conn.Ping()
	assert.Nil(t, err)
}

func TestAuthenticationWithPassthrough(t *testing.T) {
	adminConn, err := sql.Open("postgres", "postgres://admin:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	assert.Nil(t, err)
	defer adminConn.Close()

	_, err = adminConn.Exec("SET passthrough_auth TO 'enabled'")
	assert.Nil(t, err)

	conn, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.Nil(t, err)
	defer conn.Close()

	err = conn.Ping()
	assert.Nil(t, err)

	badConn, err := sql.Open("postgres", "postgres://pgdog:wrong_password@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.Nil(t, err)
	defer badConn.Close()

	err = badConn.Ping()
	assert.NotNil(t, err)

	// Reset config
	_, err = adminConn.Exec("RELOAD")
	assert.Nil(t, err)
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
