package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func GetAutoPool() *pgxpool.Pool {
	config, err := pgxpool.ParseConfig("postgres://postgres:postgres@127.0.0.1:6432/postgres_auto?sslmode=disable")
	if err != nil {
		panic(err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic(err)
	}

	return pool
}

func adminExec(t *testing.T, command string) {
	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	if err != nil {
		t.Logf("admin connect error: %v", err)
		return
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), command, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Logf("%s error: %v", command, err)
		return
	}
	rows.Close()
}

// TestReloadWithAutoRole validates that config reloads don't cause write
// queries to be routed to replicas when using role = "auto".
func TestReloadWithAutoRole(t *testing.T) {
	pool := GetAutoPool()
	defer pool.Close()

	ctx := context.Background()

	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS lb_reload_auto_test (
		id BIGINT PRIMARY KEY,
		data VARCHAR NOT NULL DEFAULT ''
	)`)
	assert.NoError(t, err)
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS lb_reload_auto_test")

	// Wait for replicas to see the table.
	time.Sleep(2 * time.Second)

	duration := 10 * time.Second
	deadline := time.Now().Add(duration)

	var writeErrors atomic.Int64
	var readErrors atomic.Int64
	var reloads atomic.Int64

	done := make(chan struct{})

	// Writer goroutines: INSERT, UPDATE, DELETE
	for w := range 5 {
		go func(workerId int) {
			counter := 0
			for time.Now().Before(deadline) {
				id := int64(workerId*100000 + counter)
				data := fmt.Sprintf("w%d-%d", workerId, counter)

				_, err := pool.Exec(ctx,
					"INSERT INTO lb_reload_auto_test (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
					id, data)
				if err != nil {
					t.Logf("INSERT error (worker %d): %v", workerId, err)
					writeErrors.Add(1)
				}

				_, err = pool.Exec(ctx,
					"UPDATE lb_reload_auto_test SET data = $2 WHERE id = $1",
					id, data+"-updated")
				if err != nil {
					t.Logf("UPDATE error (worker %d): %v", workerId, err)
					writeErrors.Add(1)
				}

				_, err = pool.Exec(ctx,
					"DELETE FROM lb_reload_auto_test WHERE id = $1",
					id)
				if err != nil {
					t.Logf("DELETE error (worker %d): %v", workerId, err)
					writeErrors.Add(1)
				}

				counter++
			}
			done <- struct{}{}
		}(w)
	}

	// Reader goroutines
	for r := range 5 {
		go func(readerId int) {
			for time.Now().Before(deadline) {
				_, err := pool.Exec(ctx, "SELECT * FROM lb_reload_auto_test LIMIT 10")
				if err != nil {
					t.Logf("SELECT error (reader %d): %v", readerId, err)
					readErrors.Add(1)
				}
			}
			done <- struct{}{}
		}(r)
	}

	// Reloader goroutine: send RELOAD every 500ms
	go func() {
		for time.Now().Before(deadline) {
			adminExec(t, "RELOAD")
			reloads.Add(1)
			time.Sleep(500 * time.Millisecond)
		}
		done <- struct{}{}
	}()

	// Wait for all: 5 writers + 5 readers + 1 reloader
	for range 11 {
		<-done
	}

	t.Logf("reloads: %d, write errors: %d, read errors: %d",
		reloads.Load(), writeErrors.Load(), readErrors.Load())

	assert.Zero(t, writeErrors.Load(), "expected no write errors from reload with auto role detection")
	assert.Zero(t, readErrors.Load(), "expected no read errors from reload with auto role detection")
}

// TestReconnectWithAutoRole validates that RECONNECT doesn't break read/write
// routing when using role = "auto".
func TestReconnectWithAutoRole(t *testing.T) {
	pool := GetAutoPool()
	defer pool.Close()

	ctx := context.Background()

	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS lb_reconnect_auto_test (
		id BIGINT PRIMARY KEY,
		data VARCHAR NOT NULL DEFAULT ''
	)`)
	assert.NoError(t, err)
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS lb_reconnect_auto_test")

	// Wait for replicas to see the table.
	time.Sleep(2 * time.Second)

	duration := 10 * time.Second
	deadline := time.Now().Add(duration)

	var writeErrors atomic.Int64
	var readErrors atomic.Int64
	var reconnects atomic.Int64

	done := make(chan struct{})

	// Writer goroutines: INSERT, UPDATE, DELETE
	for w := range 5 {
		go func(workerId int) {
			counter := 0
			for time.Now().Before(deadline) {
				id := int64(workerId*100000 + counter)
				data := fmt.Sprintf("w%d-%d", workerId, counter)

				_, err := pool.Exec(ctx,
					"INSERT INTO lb_reconnect_auto_test (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
					id, data)
				if err != nil {
					t.Logf("INSERT error (worker %d): %v", workerId, err)
					writeErrors.Add(1)
				}

				_, err = pool.Exec(ctx,
					"UPDATE lb_reconnect_auto_test SET data = $2 WHERE id = $1",
					id, data+"-updated")
				if err != nil {
					t.Logf("UPDATE error (worker %d): %v", workerId, err)
					writeErrors.Add(1)
				}

				_, err = pool.Exec(ctx,
					"DELETE FROM lb_reconnect_auto_test WHERE id = $1",
					id)
				if err != nil {
					t.Logf("DELETE error (worker %d): %v", workerId, err)
					writeErrors.Add(1)
				}

				counter++
			}
			done <- struct{}{}
		}(w)
	}

	// Reader goroutines
	for r := range 5 {
		go func(readerId int) {
			for time.Now().Before(deadline) {
				_, err := pool.Exec(ctx, "SELECT * FROM lb_reconnect_auto_test LIMIT 10")
				if err != nil {
					t.Logf("SELECT error (reader %d): %v", readerId, err)
					readErrors.Add(1)
				}
			}
			done <- struct{}{}
		}(r)
	}

	// Reconnect goroutine: send RECONNECT every 2s
	go func() {
		for time.Now().Before(deadline) {
			adminExec(t, "RECONNECT")
			reconnects.Add(1)
			time.Sleep(2 * time.Second)
		}
		done <- struct{}{}
	}()

	// Wait for all: 5 writers + 5 readers + 1 reconnector
	for range 11 {
		<-done
	}

	t.Logf("reconnects: %d, write errors: %d, read errors: %d",
		reconnects.Load(), writeErrors.Load(), readErrors.Load())

	assert.Zero(t, writeErrors.Load(), "expected no write errors after RECONNECT with auto role detection")
	assert.Zero(t, readErrors.Load(), "expected no read errors after RECONNECT with auto role detection")
}
