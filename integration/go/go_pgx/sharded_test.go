package main

import (
	"context"
	"fmt"
	"testing"
	"time"

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

	// +3 is for schema sync
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 200, "pgdog_2pc", "pgdog_sharded", 0, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_2pc_count", 200, "pgdog_2pc", "pgdog_sharded", 1, "primary")
	assertShowField(t, "SHOW STATS", "total_xact_count", 401+3, "pgdog_2pc", "pgdog_sharded", 0, "primary") // PREPARE, COMMIT for each transaction + TRUNCATE
	assertShowField(t, "SHOW STATS", "total_xact_count", 401+3, "pgdog_2pc", "pgdog_sharded", 1, "primary")

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
