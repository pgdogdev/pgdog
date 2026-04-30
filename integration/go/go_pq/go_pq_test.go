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
	adminConn, err := sql.Open("postgres", "postgres://admin:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	assert.Nil(t, err)
	defer adminConn.Close()

	conns := PqConnections()

	for _, conn := range conns {
		defer conn.Close()
		for i := range 25 {
			tx, err := conn.Begin()
			assert.Nil(t, err, "iteration %d: begin", i)

			id := rand.Intn(1_000_000)

			rows, err := tx.Query("INSERT INTO sharded (id) VALUES ($1) RETURNING id", id)
			assert.Nil(t, err, "iteration %d: insert id=%d", i, id)

			seen := 0
			var idVal int64
			for rows.Next() {
				assert.Nil(t, rows.Scan(&idVal), "iteration %d: insert scan", i)
				assert.Equal(t, int64(id), idVal, "iteration %d: insert id mismatch", i)
				seen++
			}
			assert.Nil(t, rows.Err(), "iteration %d: insert rows.Err", i)
			assert.Nil(t, rows.Close(), "iteration %d: insert rows.Close", i)
			assert.Equal(t, 1, seen, "iteration %d: insert row count", i)

			rows, err = tx.Query("SELECT id FROM sharded WHERE id = $1", id)
			assert.Nil(t, err, "iteration %d: select id=%d", i, id)

			seen = 0
			idVal = 0
			for rows.Next() {
				assert.Nil(t, rows.Scan(&idVal), "iteration %d: select scan", i)
				assert.Equal(t, int64(id), idVal, "iteration %d: select id mismatch", i)
				seen++
			}
			assert.Nil(t, rows.Err(), "iteration %d: select rows.Err", i)
			assert.Nil(t, rows.Close(), "iteration %d: select rows.Close", i)
			assert.Equal(t, 1, seen, "iteration %d: select row count", i)

			// Roll back so the row doesn't persist; the next iteration
			// uses a fresh random id but we want a clean slate either way.
			assert.Nil(t, tx.Rollback(), "iteration %d: rollback", i)

			// After rollback the row must be gone — verify outside the
			// transaction so we know the rollback actually took effect on
			// the shard the INSERT landed on.
			var count int
			assert.Nil(t,
				conn.QueryRow("SELECT COUNT(*) FROM sharded WHERE id = $1", id).Scan(&count),
				"iteration %d: post-rollback count", i)
			assert.Equal(t, 0, count, "iteration %d: row should not survive rollback", i)
		}
	}
}

func TestAdvisoryLockWithTransaction(t *testing.T) {
	adminConn, err := sql.Open("postgres", "postgres://admin:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	assert.Nil(t, err)
	defer adminConn.Close()

	_, err = adminConn.Exec("SET prepared_statements TO 'extended_anonymous'")
	assert.Nil(t, err)

	_, err = adminConn.Exec("SET query_parser TO 'on'")
	assert.Nil(t, err)

	conn, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.Nil(t, err)
	defer conn.Close()

	conn.SetMaxOpenConns(1)

	lockKey := rand.Int63()

	// 1. Take an advisory lock
	var lockResult bool
	err = conn.QueryRow("SELECT pg_advisory_lock($1) IS NOT NULL", lockKey).Scan(&lockResult)
	assert.Nil(t, err)
	assert.True(t, lockResult)

	// 2. Start a transaction
	tx, err := conn.Begin()
	assert.Nil(t, err)

	// 3. Run a couple queries inside the transaction
	var one int
	err = tx.QueryRow("SELECT 1").Scan(&one)
	assert.Nil(t, err)
	assert.Equal(t, 1, one)

	var two int
	err = tx.QueryRow("SELECT 2").Scan(&two)
	assert.Nil(t, err)
	assert.Equal(t, 2, two)

	// 4. Commit the transaction
	err = tx.Commit()
	assert.Nil(t, err)

	// 5. Run some queries outside of the transaction
	var three int
	err = conn.QueryRow("SELECT 3").Scan(&three)
	assert.Nil(t, err)
	assert.Equal(t, 3, three)

	var four int
	err = conn.QueryRow("SELECT 4").Scan(&four)
	assert.Nil(t, err)
	assert.Equal(t, 4, four)

	// 6. Release the advisory lock
	var unlockResult bool
	err = conn.QueryRow("SELECT pg_advisory_unlock($1)", lockKey).Scan(&unlockResult)
	assert.Nil(t, err)
	assert.True(t, unlockResult)
}
