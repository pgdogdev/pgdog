package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func adminQuery(t *testing.T, command string) []map[string]any {
	t.Helper()

	conn, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	require.NoError(t, err)
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), command, pgx.QueryExecModeSimpleProtocol)
	require.NoError(t, err)

	result, err := pgx.CollectRows(rows, pgx.RowToMap)
	require.NoError(t, err)

	return result
}

func replicaPoolIds(t *testing.T) map[int64]int64 {
	t.Helper()

	ids := make(map[int64]int64)
	for _, pool := range adminQuery(t, "SHOW POOLS") {
		if pool["database"] == "postgres" && pool["user"] == "postgres" && pool["role"] == "replica" {
			ids[toInt64(t, pool["port"])] = toInt64(t, pool["id"])
		}
	}
	require.Len(t, ids, 2)

	return ids
}

func toInt64(t *testing.T, value any) int64 {
	t.Helper()

	switch v := value.(type) {
	case int64:
		return v
	case pgtype.Numeric:
		result, err := v.Int64Value()
		require.NoError(t, err)
		return result.Int64
	default:
		require.Failf(t, "unexpected column type", "%T", value)
		return 0
	}
}

func banPool(t *testing.T, id int64) {
	t.Helper()

	adminQuery(t, fmt.Sprintf("BAN %d", id))
	t.Cleanup(func() {
		adminQuery(t, fmt.Sprintf("UNBAN %d", id))
	})
}

func setupExcludePrimaryTable(t *testing.T, table string) {
	t.Helper()

	pool := GetPool()
	defer pool.Close()

	_, err := pool.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id BIGINT)", table))
	require.NoError(t, err)
	t.Cleanup(func() {
		pool := GetPool()
		defer pool.Close()
		pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	time.Sleep(2 * time.Second)
	ResetStats()
}

func TestExcludePrimaryOneReplicaBanned(t *testing.T) {
	table := "lb_pgx_exclude_primary_one_banned"
	setupExcludePrimaryTable(t, table)

	ids := replicaPoolIds(t)
	banPool(t, ids[45001])

	pool := GetPool()
	defer pool.Close()

	for range 20 {
		_, err := pool.Exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", table))
		assert.NoError(t, err)
	}

	replicaCalls := LoadStatsForReplicas(table)
	assert.Equal(t, int64(0), replicaCalls[0].Calls, "banned replica must not serve reads")
	assert.Equal(t, int64(20), replicaCalls[1].Calls, "healthy replica must serve every read")
	assert.Equal(t, int64(0), LoadStatsForPrimary(table).Calls, "primary must not serve reads")
}

func TestExcludePrimaryAllReplicasBanned(t *testing.T) {
	table := "lb_pgx_exclude_primary_all_banned"
	setupExcludePrimaryTable(t, table)

	for _, id := range replicaPoolIds(t) {
		banPool(t, id)
	}

	pool := GetPool()
	defer pool.Close()

	for range 5 {
		_, err := pool.Exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", table))
		assert.ErrorContains(t, err, "all replicas down")
	}

	assert.Equal(t, int64(0), LoadStatsForPrimary(table).Calls, "primary must not serve reads when all replicas are banned")
	for _, calls := range LoadStatsForReplicas(table) {
		assert.Equal(t, int64(0), calls.Calls, "banned replica must not serve reads")
	}
}
