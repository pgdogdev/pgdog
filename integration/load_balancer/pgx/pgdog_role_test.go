package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

const readOnlySqlState = "25006"

func connectWithRole(t *testing.T, role string) *pgx.Conn {
	config, err := pgx.ParseConfig("postgres://postgres:postgres@127.0.0.1:6432/postgres?sslmode=disable")
	assert.NoError(t, err)

	if role != "" {
		config.RuntimeParams["pgdog.role"] = role
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	assert.NoError(t, err)

	return conn
}

func setupRoleTable(t *testing.T, table string) {
	pool := GetPool()
	defer pool.Close()

	_, err := pool.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id BIGINT)", table))
	assert.NoError(t, err)

	t.Cleanup(func() {
		pool := GetPool()
		defer pool.Close()
		pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	time.Sleep(2 * time.Second)
	ResetStats()
}

func replicaCallsFor(query string) int64 {
	total := int64(0)
	for _, call := range LoadStatsForReplicas(query) {
		total += call.Calls
	}
	return total
}

func runSelects(t *testing.T, conn *pgx.Conn, table string, count int) {
	for i := range count {
		_, err := conn.Exec(context.Background(), fmt.Sprintf("SELECT $1::bigint FROM %s", table), int64(i))
		assert.NoError(t, err)
	}
}

func assertReadOnlyError(t *testing.T, err error) {
	var pgErr *pgconn.PgError
	assert.True(t, errors.As(err, &pgErr), "expected a Postgres error, got %v", err)
	if pgErr != nil {
		assert.Equal(t, readOnlySqlState, pgErr.Code)
	}
}

func TestPgdogRoleStartupPrimary(t *testing.T) {
	table := "lb_pgx_role_startup_primary"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "primary")
	defer conn.Close(context.Background())

	runSelects(t, conn, table, 20)

	assert.Equal(t, int64(20), LoadStatsForPrimary(table).Calls)
	assert.Equal(t, int64(0), replicaCallsFor(table))
}

func TestPgdogRoleStartupReplicaRejectsWrites(t *testing.T) {
	table := "lb_pgx_role_startup_replica"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "replica")
	defer conn.Close(context.Background())

	runSelects(t, conn, table, 10)

	_, err := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id) VALUES ($1)", table), int64(1))
	assertReadOnlyError(t, err)

	assert.Equal(t, int64(0), LoadStatsForPrimary(table).Calls)
	assert.Equal(t, int64(10), replicaCallsFor("SELECT $1::bigint FROM "+table))
}

func TestPgdogRoleSetAndReset(t *testing.T) {
	table := "lb_pgx_role_set_reset"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "")
	defer conn.Close(context.Background())

	_, err := conn.Exec(context.Background(), "SET pgdog.role TO 'primary'")
	assert.NoError(t, err)

	runSelects(t, conn, table, 10)
	assert.Equal(t, int64(10), LoadStatsForPrimary(table).Calls)
	assert.Equal(t, int64(0), replicaCallsFor(table))

	_, err = conn.Exec(context.Background(), "RESET pgdog.role")
	assert.NoError(t, err)

	runSelects(t, conn, table, 10)
	assert.Equal(t, int64(10), LoadStatsForPrimary(table).Calls)
	assert.Equal(t, int64(10), replicaCallsFor(table))
}

func TestPgdogRoleAutoClearsStartupRole(t *testing.T) {
	table := "lb_pgx_role_auto"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "replica")
	defer conn.Close(context.Background())

	_, err := conn.Exec(context.Background(), "SET pgdog.role TO 'auto'")
	assert.NoError(t, err)

	runSelects(t, conn, table, 10)

	_, err = conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id) VALUES ($1)", table), int64(1))
	assert.NoError(t, err)

	assert.Equal(t, int64(10), replicaCallsFor("SELECT $1::bigint FROM "+table))
	assert.Equal(t, int64(1), LoadStatsForPrimary("INSERT INTO "+table).Calls)
	assert.Equal(t, int64(0), LoadStatsForPrimary("SELECT $1::bigint FROM "+table).Calls)
}

func TestPgdogRoleReplicaTransaction(t *testing.T) {
	table := "lb_pgx_role_replica_tx"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "replica")
	defer conn.Close(context.Background())

	tx, err := conn.Begin(context.Background())
	assert.NoError(t, err)

	for i := range 5 {
		_, err = tx.Exec(context.Background(), fmt.Sprintf("SELECT $1::bigint FROM %s", table), int64(i))
		assert.NoError(t, err)
	}

	assert.NoError(t, tx.Commit(context.Background()))

	assert.Equal(t, int64(5), replicaCallsFor(table))
	assert.Equal(t, int64(0), LoadStatsForPrimary(table).Calls)
}

func TestPgdogRoleCommentOverridesSessionRole(t *testing.T) {
	table := "lb_pgx_role_comment"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "replica")
	defer conn.Close(context.Background())

	for i := range 10 {
		_, err := conn.Exec(context.Background(), fmt.Sprintf("/* pgdog_role: primary */ SELECT $1::bigint FROM %s", table), int64(i))
		assert.NoError(t, err)
	}

	assert.Equal(t, int64(10), LoadStatsForPrimary(table).Calls)
	assert.Equal(t, int64(0), replicaCallsFor(table))
}

func TestPgdogRoleSetAfterConnectInTransaction(t *testing.T) {
	table := "lb_pgx_role_set_in_tx"
	setupRoleTable(t, table)

	conn := connectWithRole(t, "")
	defer conn.Close(context.Background())

	tx, err := conn.Begin(context.Background())
	assert.NoError(t, err)

	_, err = tx.Exec(context.Background(), fmt.Sprintf("SELECT $1::bigint FROM %s", table), int64(1))
	assert.NoError(t, err)

	_, err = tx.Exec(context.Background(), "SET pgdog.role TO 'replica'")
	assert.Error(t, err)

	assert.NoError(t, tx.Rollback(context.Background()))

	assert.Equal(t, int64(1), LoadStatsForPrimary(table).Calls)
	assert.Equal(t, int64(0), replicaCallsFor(table))
}
