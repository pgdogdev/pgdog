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
			ID             int64
			Database       string
			User           string
			Addr           string
			Port           int64
			Shard          int64
			Role           string
			ReplicaLag     *string
			PgLsn          *string
			LsnAge         *string
			PgIsInRecovery *string
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
			if row.Database == "postgres" {
				results = append(results, row)
			}
		}
		rows.Close()

		assert.NotEmpty(t, results, "SHOW REPLICATION returned no rows for database 'postgres'")

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

// TestShowReplicationAutoRoleDetection verifies that SHOW REPLICATION reports
// the auto-detected role (primary/replica) for the postgres_auto database,
// rather than leaving it as the configured "auto" value. The cluster has one
// real primary on port 45000 and two replicas on ports 45001 / 45002, so once
// the LSN monitor has populated stats, role detection must converge to exactly
// one primary (the row whose port is 45000) and two replicas, with each row's
// role matching its pg_is_in_recovery value.
func TestShowReplicationAutoRoleDetection(t *testing.T) {
	conn := GetAdminConn()
	defer conn.Close(context.Background())

	type ReplicationRow struct {
		ID             int64
		Database       string
		User           string
		Addr           string
		Port           int64
		Shard          int64
		Role           string
		ReplicaLag     *string
		PgLsn          *string
		LsnAge         *string
		PgIsInRecovery *string
	}

	const expectedPrimaryPort = int64(45000)

	// Wait up to 10 seconds for auto-detection to converge.
	maxAttempts := 20
	for attempt := 0; attempt < maxAttempts; attempt++ {
		rows, err := conn.Query(context.Background(), "SHOW REPLICATION")
		assert.NoError(t, err)

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
			if row.Database == "postgres_auto" {
				results = append(results, row)
			}
		}
		rows.Close()

		assert.NotEmpty(t, results,
			"SHOW REPLICATION returned no rows for database 'postgres_auto'")
		assert.Len(t, results, 3,
			"expected 3 backends for postgres_auto (1 primary + 2 replicas)")

		// Wait for every row to have valid LSN stats AND a resolved role
		// (i.e., never "auto" — auto means detection hasn't run yet).
		ready := true
		for _, row := range results {
			if row.PgLsn == nil || *row.PgLsn == "" {
				ready = false
				break
			}
			if row.Role == "auto" {
				ready = false
				break
			}
		}

		if !ready {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		primaryCount := 0
		replicaCount := 0
		for _, row := range results {
			assert.NotNil(t, row.PgLsn, "pg_lsn should not be null")
			assert.True(t, strings.Contains(*row.PgLsn, "/"),
				"pg_lsn '%s' should be in LSN format", *row.PgLsn)
			assert.NotNil(t, row.PgIsInRecovery,
				"pg_is_in_recovery should not be null")

			switch row.Role {
			case "primary":
				primaryCount++
				assert.Equal(t, "f", *row.PgIsInRecovery,
					"port %d marked primary but pg_is_in_recovery = '%s'",
					row.Port, *row.PgIsInRecovery)
				assert.Equal(t, expectedPrimaryPort, row.Port,
					"auto-detected primary should be on port %d, got %d",
					expectedPrimaryPort, row.Port)
			case "replica":
				replicaCount++
				assert.Equal(t, "t", *row.PgIsInRecovery,
					"port %d marked replica but pg_is_in_recovery = '%s'",
					row.Port, *row.PgIsInRecovery)
				assert.NotEqual(t, expectedPrimaryPort, row.Port,
					"port %d is the real primary but was detected as replica",
					row.Port)
			default:
				t.Fatalf("unexpected role %q for port %d", row.Role, row.Port)
			}
		}

		assert.Equal(t, 1, primaryCount,
			"expected exactly one auto-detected primary, got %d", primaryCount)
		assert.Equal(t, 2, replicaCount,
			"expected exactly two auto-detected replicas, got %d", replicaCount)

		return
	}

	t.Fatalf("auto role detection did not converge after %d attempts (%dms)",
		maxAttempts, maxAttempts*500)
}
