package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func assertNoOutOfSync(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://admin:pgdog@127.0.0.1:6432/admin")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SHOW POOLS", pgx.QueryExecModeSimpleProtocol)
	defer rows.Close()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			panic(err)
		}

		out_of_sync := values[16].(int64)
		assert.Equal(t, out_of_sync, int64(0), "No connections should be out of sync")
	}
}

func connectNormal() (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't connect: %v\n", err)
		return nil, err
	}

	return conn, nil
}

func TestConnect(t *testing.T) {
	conn, err := connectNormal()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't connect: %v\n", err)
	}
	defer conn.Close(context.Background())

	assertNoOutOfSync(t)

}

func TestSelect(t *testing.T) {
	conn, err := connectNormal()
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	for i := range 25 {
		var one int64
		err = conn.QueryRow(context.Background(), "SELECT $1::bigint AS one", i).Scan(&one)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, one, int64(i))
	}

}

func TestTimeout(t *testing.T) {
	c := make(chan int, 1)

	// Using 9 because the pool size is 10
	// and we're executing a slow query that will block
	// the pool for a while.
	// Test pool size is 10.
	for _ = range 9 {
		go func() {
			executeTimeoutTest(t)
			c <- 1
		}()
	}

	for _ = range 9 {
		<-c
	}

	// Wait for the conn to be drained and checked in
	time.Sleep(2 * time.Second)

}

func executeTimeoutTest(t *testing.T) {
	conn, err := connectNormal()
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	c := make(chan int, 1)

	go func() {
		err = pgSleepOneSecond(conn)
		if err == nil {
			panic(err)
		}

		c <- 0
	}()

	select {
	case <-c:
		t.Error("Context should of been cancelled")
	case <-ctx.Done():
	}
}

// Sleep for 1 second.
func pgSleepOneSecond(conn *pgx.Conn) (err error) {
	_, err = conn.Exec(context.Background(), "SELECT pg_sleep(1)")
	return err
}
