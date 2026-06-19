package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

// drainCount consumes all rows and returns how many there were.
func drainCount(rows pgx.Rows) int {
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n
}

// connectDirect opens a connection straight to a physical shard database,
// bypassing pgdog, so tests can verify data placement.
func connectDirect(t *testing.T, shard int) *pgx.Conn {
	t.Helper()
	dsn := fmt.Sprintf("postgres://pgdog:pgdog@127.0.0.1:5432/shard_%d", shard)
	conn, err := pgx.Connect(context.Background(), dsn)
	assert.NoError(t, err)
	return conn
}

// countOnShard counts rows matching id on the given direct-shard connection.
func countOnShard(t *testing.T, conn *pgx.Conn, table string, id int64) int {
	t.Helper()
	var n int
	err := conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM "+table+" WHERE id = $1", id).Scan(&n)
	assert.NoError(t, err)
	return n
}

type shardCase struct {
	id        int64
	wantShard int
}

// runShardingTest is the shared verification body for all explicit-mapping tests.
// Phase 1: inserts each case through pgdog and asserts the row landed on the
// correct physical shard. Phase 2: verifies SELECT / UPDATE / DELETE route to
// that same shard.
func runShardingTest(t *testing.T, table string, cases []shardCase) {
	t.Helper()

	proxy, err := pgx.Connect(context.Background(), "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
	assert.NoError(t, err)
	defer proxy.Close(context.Background())

	shard0 := connectDirect(t, 0)
	defer shard0.Close(context.Background())
	shard1 := connectDirect(t, 1)
	defer shard1.Close(context.Background())

	ctx := context.Background()
	_, err = proxy.Exec(ctx, "TRUNCATE TABLE "+table)
	assert.NoError(t, err)

	for _, tc := range cases {
		rows, err := proxy.Query(ctx, "INSERT INTO "+table+" (id) VALUES ($1) RETURNING *", tc.id)
		assert.NoError(t, err)
		assert.Equal(t, 1, drainCount(rows), "INSERT id=%d must return 1 row", tc.id)

		if tc.wantShard == 0 {
			assert.Equal(t, 1, countOnShard(t, shard0, table, tc.id), "id=%d must be on shard 0", tc.id)
			assert.Equal(t, 0, countOnShard(t, shard1, table, tc.id), "id=%d must NOT be on shard 1", tc.id)
		} else {
			assert.Equal(t, 0, countOnShard(t, shard0, table, tc.id), "id=%d must NOT be on shard 0", tc.id)
			assert.Equal(t, 1, countOnShard(t, shard1, table, tc.id), "id=%d must be on shard 1", tc.id)
		}
	}

	for _, tc := range cases {
		for _, q := range []string{
			"SELECT * FROM " + table + " WHERE id = $1",
			"UPDATE " + table + " SET id = $1 WHERE id = $1 RETURNING *",
			"DELETE FROM " + table + " WHERE id = $1 RETURNING *",
		} {
			rows, err := proxy.Query(ctx, q, tc.id)
			assert.NoError(t, err)
			assert.Equal(t, 1, drainCount(rows), "id=%d: %s must return 1 row", tc.id, q)
		}
	}
}

func listCases() []shardCase {
	// 0–9 → shard 0, 10–19 → shard 1
	cases := make([]shardCase, 20)
	for i := range 20 {
		shard := 0
		if i >= 10 {
			shard = 1
		}
		cases[i] = shardCase{int64(i), shard}
	}
	return cases
}

func rangeCases() []shardCase {
	// [0, 100) → shard 0, [100, 200) → shard 1
	cases := make([]shardCase, 200)
	for i := range 200 {
		shard := 0
		if i >= 100 {
			shard = 1
		}
		cases[i] = shardCase{int64(i), shard}
	}
	return cases
}

// TestShardedListDeprecated tests list mapping via the legacy [[sharded_mappings]] config format.
func TestShardedListDeprecated(t *testing.T) { runShardingTest(t, "sharded_list_deprecated", listCases()) }

// TestShardedList tests list mapping via the inline mapping = [...] config format.
func TestShardedList(t *testing.T) { runShardingTest(t, "sharded_list", listCases()) }

// TestShardedRangeDeprecated tests range mapping via the legacy [[sharded_mappings]] config format.
func TestShardedRangeDeprecated(t *testing.T) {
	runShardingTest(t, "sharded_range_deprecated", rangeCases())
}

// TestShardedRange tests range mapping via the inline mapping = [...] config format.
func TestShardedRange(t *testing.T) { runShardingTest(t, "sharded_range", rangeCases()) }

// TestShardedMappingHierarchy verifies the list → range → default resolution order.
func TestShardedMappingHierarchy(t *testing.T) {
	runShardingTest(t, "sharded_mapping_hierarchy", []shardCase{
		// List entries take priority over everything.
		{1, 0}, {2, 0}, {4, 1}, {6, 1},
		// Range entries used when no list entry matches.
		{100, 0}, {150, 0}, {200, 1}, {299, 1},
		// Default used when neither list nor range matches.
		{50, 0}, {999, 0},
	})
}

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

func TestShardedTwoPc(t *testing.T) {
	conn, err := connectTwoPc()
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	conn.Exec(context.Background(), "TRUNCATE TABLE sharded")
	adminCommand(t, "RELOAD") // Clear stats
	adminCommand(t, "SET two_phase_commit TO true")

	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")

	for i := range 200 {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		assert.NoError(t, err)

		rows, err := tx.Query(
			context.Background(),
			"INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING *", int64(i), fmt.Sprintf("value_%d", i),
		)

		assert.NoError(t, err)
		assert.True(t, rows.Next())
		assert.False(t, rows.Next())
		rows.Close()

		err = tx.Commit(context.Background())
		assert.NoError(t, err)
	}

	// +5 is for schema sync
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 200, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 200, "pgdog_2pc", "pgdog_sharded", 1, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_count", 401+5, "pgdog_2pc", "pgdog_sharded", 0, "primary") // PREPARE, COMMIT for each transaction + TRUNCATE
	assertShowField(t, "SHOW STATS", "total_xact_count", 401+5, "pgdog_2pc", "pgdog_sharded", 1, "primary")

	for i := range 200 {
		rows, err := conn.Query(
			context.Background(),
			"SELECT * FROM sharded WHERE id = $1", int64(i),
		)
		assert.NoError(t, err)
		assert.True(t, rows.Next())
		assert.False(t, rows.Next())
		rows.Close()
	}

	conn.Exec(context.Background(), "TRUNCATE TABLE sharded")
}

func TestShardedTwoPcAuto(t *testing.T) {
	conn, err := connectTwoPc()
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	conn.Exec(context.Background(), "TRUNCATE TABLE sharded_omni")
	adminCommand(t, "RELOAD") // Clear stats
	adminCommand(t, "SET two_phase_commit TO true")
	adminCommand(t, "SET two_phase_commit_auto TO true")

	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")

	for i := range 200 {
		rows, err := conn.Query(context.Background(), "INSERT INTO sharded_omni (id, value) VALUES ($1, $2) RETURNING *", int64(i), fmt.Sprintf("value_%d", i))
		assert.NoError(t, err)

		// Returns 1 row
		assert.True(t, rows.Next())
		assert.False(t, rows.Next())
	}

	// We automatically used 2pc.
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 200, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 200, "pgdog_2pc", "pgdog_sharded", 1, "primary")
	conn.Exec(context.Background(), "TRUNCATE TABLE sharded_omni")
}

func TestShardedTwoPcAutoOff(t *testing.T) {
	conn, err := connectTwoPc()
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	conn.Exec(context.Background(), "TRUNCATE TABLE sharded_omni")
	adminCommand(t, "RELOAD")
	adminCommand(t, "SET two_phase_commit TO true")

	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")

	for i := range 200 {
		rows, err := conn.Query(context.Background(), "INSERT INTO sharded_omni (id, value) VALUES ($1, $2) RETURNING *", int64(i), fmt.Sprintf("value_%d", i))
		assert.NoError(t, err)

		// Returns 1 row
		assert.True(t, rows.Next())
		assert.False(t, rows.Next())
	}

	// We automatically used 2pc.
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")
	conn.Exec(context.Background(), "TRUNCATE TABLE sharded_omni")
}

func TestShardedTwoPcAutoError(t *testing.T) {
	conn, err := connectTwoPc()
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	adminCommand(t, "RELOAD") // Clear stats
	adminCommand(t, "SET two_phase_commit TO true")
	adminCommand(t, "SET two_phase_commit_auto TO true")

	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")

	// Attempt queries on non-existent table that would require 2PC (cross-shard)
	for i := range 50 {
		_, err := conn.Query(context.Background(), "INSERT INTO nonexistent_table_omni (id, value) VALUES ($1, $2) RETURNING *", int64(i), fmt.Sprintf("value_%d", i))
		assert.Error(t, err) // Should fail with table doesn't exist error
	}

	// 2PC count should remain 0 since transactions failed before preparation
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")
}

func TestShardedTwoPcAutoOnError(t *testing.T) {
	conn, err := connectTwoPc()
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	adminCommand(t, "RELOAD") // Clear stats
	adminCommand(t, "SET two_phase_commit TO true")
	adminCommand(t, "SET two_phase_commit_auto TO true") // Explicitly enable auto 2PC

	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")

	// Give the pool an extra second to warm up.
	time.Sleep(1 * time.Second)

	// Attempt explicit transaction with non-existent table that would require 2PC
	for i := range 25 {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		assert.NoError(t, err)

		_, err = tx.Query(context.Background(), "INSERT INTO nonexistent_cross_shard_table (id, value) VALUES ($1, $2) RETURNING *", int64(i), fmt.Sprintf("value_%d", i))
		assert.Error(t, err) // Should fail with table doesn't exist error

		// Transaction should be rolled back due to error
		err = tx.Rollback(context.Background())
		assert.NoError(t, err)
	}

	// 2PC count should remain 0 since transactions failed and were rolled back
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 0, "pgdog_2pc", "pgdog_sharded", 1, "primary")
}
