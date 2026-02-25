package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func GetAdminConn() *pgx.Conn {
	config, err := pgx.ParseConfig("postgres://pgdog:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	if err != nil {
		panic(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		panic(err)
	}
	return conn
}

func TestShowReplicationLsnStats(t *testing.T) {
	conn := GetAdminConn()
	defer conn.Close(context.Background())

	// Wait for LSN monitor to populate stats (up to 10 seconds)
	maxAttempts := 20
	for attempt := 0; attempt < maxAttempts; attempt++ {
		rows, err := conn.Query(context.Background(), "SHOW REPLICATION")
		assert.NoError(t, err)

		type ReplicationRow struct {
			ID              int64
			Database        string
			User            string
			Addr            string
			Port            int64
			Shard           int64
			Role            string
			ReplicaLag      *string
			PgLsn           *string
			LsnAge          *string
			PgIsInRecovery  *string
		}

		var results []ReplicationRow
		for rows.Next() {
			var row ReplicationRow
			err := rows.Scan(
				&row.ID,
				&row.Database,
				&row.User,
				&row.Addr,
				&row.Port,
				&row.Shard,
				&row.Role,
				&row.ReplicaLag,
				&row.PgLsn,
				&row.LsnAge,
				&row.PgIsInRecovery,
			)
			assert.NoError(t, err)
			results = append(results, row)
		}
		rows.Close()

		assert.NotEmpty(t, results, "SHOW REPLICATION returned no rows")

		// Check if any row has valid pg_lsn (non-null)
		hasValidStats := false
		for _, row := range results {
			if row.PgLsn != nil && *row.PgLsn != "" {
				hasValidStats = true
				break
			}
		}

		if hasValidStats {
			// Count primaries (pg_is_in_recovery = 'f')
			primaryCount := 0

			// Verify all rows have the expected values
			for _, row := range results {
				// pg_lsn should be in format "X/XXXXXXXX" (LSN format)
				assert.NotNil(t, row.PgLsn, "pg_lsn should not be null for role %s", row.Role)
				assert.True(t, strings.Contains(*row.PgLsn, "/"),
					"pg_lsn '%s' should be in LSN format (containing '/')", *row.PgLsn)

				// lsn_age should be present and parseable
				assert.NotNil(t, row.LsnAge, "lsn_age should not be null for role %s", row.Role)

				// pg_is_in_recovery should be 't' for replica and 'f' for primary
				assert.NotNil(t, row.PgIsInRecovery, "pg_is_in_recovery should not be null for role %s", row.Role)

				if *row.PgIsInRecovery == "f" {
					primaryCount++
				}

				switch row.Role {
				case "primary":
					assert.Equal(t, "f", *row.PgIsInRecovery,
						"primary should have pg_is_in_recovery = 'f'")
				case "replica":
					assert.Equal(t, "t", *row.PgIsInRecovery,
						"replica should have pg_is_in_recovery = 't'")
				default:
					t.Fatalf("unexpected role: %s", row.Role)
				}
			}

			// Ensure exactly one primary
			assert.Equal(t, 1, primaryCount,
				"expected exactly one primary (pg_is_in_recovery = 'f'), got %d", primaryCount)

			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("LSN stats not populated after %d attempts (%dms)", maxAttempts, maxAttempts*500)
}
