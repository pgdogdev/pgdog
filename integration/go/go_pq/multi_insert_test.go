package main

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
)

const shardedDSN = "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?sslmode=disable"
const adminDSN = "postgres://admin:pgdog@127.0.0.1:6432/admin?sslmode=disable"

// enableSplitInserts turns on the rewrite engine so multi-tuple INSERTs into
// sharded tables are split per-row and routed to the shard each row hashes to,
// instead of being broadcast to every shard. The settings are global, so this
// is applied via the admin connection and affects the data connection too.
func enableSplitInserts(t *testing.T, admin *sql.DB) {
	for _, stmt := range []string{
		"SET two_phase_commit TO false",
		"SET rewrite_enabled TO true",
		"SET rewrite_split_inserts TO rewrite",
	} {
		_, err := admin.Exec(stmt)
		assert.Nil(t, err, "enable split inserts: %s", stmt)
	}
}

// resetRewrite restores the defaults from integration/pgdog.toml so this test
// doesn't leak rewrite behavior into the rest of the suite.
func resetRewrite(t *testing.T, admin *sql.DB) {
	for _, stmt := range []string{
		"SET rewrite_split_inserts TO error",
		"SET rewrite_enabled TO false",
		"SET two_phase_commit TO false",
	} {
		_, err := admin.Exec(stmt)
		assert.Nil(t, err, "reset rewrite: %s", stmt)
	}
}

// countOnShard returns how many rows with the given id live on a specific
// shard. The `/* pgdog_shard: N */` comment pins the query to one shard so we
// can prove where a row actually landed rather than trusting pgdog's routing
// to read it back.
func countOnShard(t *testing.T, conn *sql.DB, shard int, id int64) int {
	var count int
	query := fmt.Sprintf("/* pgdog_shard: %d */ SELECT COUNT(*) FROM sharded WHERE id = %d", shard, id)
	assert.Nil(t, conn.QueryRow(query).Scan(&count), "count id=%d on shard %d", id, shard)
	return count
}

// deleteIds removes the given ids from the sharded table. pgdog routes each
// DELETE to the shard the id hashes to, which is exactly what we want for a
// clean slate before and after each test.
func deleteIds(t *testing.T, conn *sql.DB, ids []int64) {
	for _, id := range ids {
		_, err := conn.Exec("DELETE FROM sharded WHERE id = $1", id)
		assert.Nil(t, err, "delete id=%d", id)
	}
}

// TestMultiTupleInsertSplitAcrossShards inserts a single multi-tuple INSERT
// whose two rows hash to different shards and verifies each row is placed on
// its own shard (and only there). id=1 hashes to shard 0, id=11 to shard 1.
func TestMultiTupleInsertSplitAcrossShards(t *testing.T) {
	admin, err := sql.Open("postgres", adminDSN)
	assert.Nil(t, err)
	defer admin.Close()

	enableSplitInserts(t, admin)
	defer resetRewrite(t, admin)

	conn, err := sql.Open("postgres", shardedDSN)
	assert.Nil(t, err)
	defer conn.Close()

	ids := []int64{1, 11}
	deleteIds(t, conn, ids)
	defer deleteIds(t, conn, ids)

	_, err = conn.Exec("INSERT INTO sharded (id, value) VALUES (1, 'one'), (11, 'eleven')")
	assert.Nil(t, err, "multi-tuple insert split across shards")

	// id=1 lives on shard 0 only.
	assert.Equal(t, 1, countOnShard(t, conn, 0, 1), "id=1 should be on shard 0")
	assert.Equal(t, 0, countOnShard(t, conn, 1, 1), "id=1 should not be on shard 1")

	// id=11 lives on shard 1 only.
	assert.Equal(t, 1, countOnShard(t, conn, 1, 11), "id=11 should be on shard 1")
	assert.Equal(t, 0, countOnShard(t, conn, 0, 11), "id=11 should not be on shard 0")

	// The values came through intact; pgdog routes these SELECTs to the
	// correct shard automatically.
	var value string
	assert.Nil(t, conn.QueryRow("SELECT value FROM sharded WHERE id = $1", 1).Scan(&value))
	assert.Equal(t, "one", value)
	assert.Nil(t, conn.QueryRow("SELECT value FROM sharded WHERE id = $1", 11).Scan(&value))
	assert.Equal(t, "eleven", value)
}

// TestMultiTupleInsertSplitParameterized exercises the same split path with a
// parameterized (extended-protocol) multi-tuple INSERT, since splitting has to
// renumber the bind parameters for each row it pulls out.
func TestMultiTupleInsertSplitParameterized(t *testing.T) {
	admin, err := sql.Open("postgres", adminDSN)
	assert.Nil(t, err)
	defer admin.Close()

	enableSplitInserts(t, admin)
	defer resetRewrite(t, admin)

	conn, err := sql.Open("postgres", shardedDSN)
	assert.Nil(t, err)
	defer conn.Close()

	ids := []int64{1, 11}
	deleteIds(t, conn, ids)
	defer deleteIds(t, conn, ids)

	_, err = conn.Exec(
		"INSERT INTO sharded (id, value) VALUES ($1, $2), ($3, $4)",
		1, "one", 11, "eleven",
	)
	assert.Nil(t, err, "parameterized multi-tuple insert split across shards")

	assert.Equal(t, 1, countOnShard(t, conn, 0, 1), "id=1 should be on shard 0")
	assert.Equal(t, 1, countOnShard(t, conn, 1, 11), "id=11 should be on shard 1")

	var value string
	assert.Nil(t, conn.QueryRow("SELECT value FROM sharded WHERE id = $1", 1).Scan(&value))
	assert.Equal(t, "one", value)
	assert.Nil(t, conn.QueryRow("SELECT value FROM sharded WHERE id = $1", 11).Scan(&value))
	assert.Equal(t, "eleven", value)
}

// TestMultiTupleInsertSplitManyRows inserts many rows in a single statement and
// checks two things at once: every row is stored exactly once (no broadcast
// duplication), and the rows are actually distributed across both shards (not
// all funneled to one). The set includes id=1 (shard 0) and id=11 (shard 1) so
// the distribution check always has a row on each side.
func TestMultiTupleInsertSplitManyRows(t *testing.T) {
	admin, err := sql.Open("postgres", adminDSN)
	assert.Nil(t, err)
	defer admin.Close()

	enableSplitInserts(t, admin)
	defer resetRewrite(t, admin)

	conn, err := sql.Open("postgres", shardedDSN)
	assert.Nil(t, err)
	defer conn.Close()

	ids := []int64{1, 2, 3, 4, 5, 11, 12, 13, 14, 15}
	deleteIds(t, conn, ids)
	defer deleteIds(t, conn, ids)

	_, err = conn.Exec(`INSERT INTO sharded (id, value) VALUES
		(1, 'v1'), (2, 'v2'), (3, 'v3'), (4, 'v4'), (5, 'v5'),
		(11, 'v11'), (12, 'v12'), (13, 'v13'), (14, 'v14'), (15, 'v15')`)
	assert.Nil(t, err, "multi-tuple insert with many rows")

	shard0Rows, shard1Rows := 0, 0
	for _, id := range ids {
		onShard0 := countOnShard(t, conn, 0, id)
		onShard1 := countOnShard(t, conn, 1, id)

		// Each row must exist exactly once, on exactly one shard. A broadcast
		// would put it on both (total 2); a dropped row would put it on none.
		assert.Equal(t, 1, onShard0+onShard1, "id=%d should exist exactly once across shards", id)

		shard0Rows += onShard0
		shard1Rows += onShard1
	}

	// The split actually fanned out: both shards received at least one row.
	assert.Greater(t, shard0Rows, 0, "shard 0 should have received some rows")
	assert.Greater(t, shard1Rows, 0, "shard 1 should have received some rows")
	assert.Equal(t, len(ids), shard0Rows+shard1Rows, "every row should be accounted for")
}

// TestMultiTupleInsertSplitManyRowsParameterized is the parameterized variant
// of TestMultiTupleInsertSplitManyRows: the same many-row INSERT, but with
// every value sent as an extended-protocol bind parameter. This stresses the
// per-tuple parameter renumbering the splitter performs ($1..$20 in the
// original statement become $1, $2 in each single-tuple split).
func TestMultiTupleInsertSplitManyRowsParameterized(t *testing.T) {
	admin, err := sql.Open("postgres", adminDSN)
	assert.Nil(t, err)
	defer admin.Close()

	enableSplitInserts(t, admin)
	defer resetRewrite(t, admin)

	conn, err := sql.Open("postgres", shardedDSN)
	assert.Nil(t, err)
	defer conn.Close()

	ids := []int64{1, 2, 3, 4, 5, 11, 12, 13, 14, 15}
	deleteIds(t, conn, ids)
	defer deleteIds(t, conn, ids)

	// Build "VALUES ($1, $2), ($3, $4), ..." with one bound pair per row.
	tuples := make([]string, len(ids))
	args := make([]any, 0, len(ids)*2)
	for i, id := range ids {
		tuples[i] = fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2)
		args = append(args, id, fmt.Sprintf("v%d", id))
	}
	query := "INSERT INTO sharded (id, value) VALUES " + strings.Join(tuples, ", ")

	_, err = conn.Exec(query, args...)
	assert.Nil(t, err, "parameterized multi-tuple insert with many rows")

	shard0Rows, shard1Rows := 0, 0
	for _, id := range ids {
		onShard0 := countOnShard(t, conn, 0, id)
		onShard1 := countOnShard(t, conn, 1, id)

		// Each row must exist exactly once, on exactly one shard.
		assert.Equal(t, 1, onShard0+onShard1, "id=%d should exist exactly once across shards", id)

		shard0Rows += onShard0
		shard1Rows += onShard1
	}

	assert.Greater(t, shard0Rows, 0, "shard 0 should have received some rows")
	assert.Greater(t, shard1Rows, 0, "shard 1 should have received some rows")
	assert.Equal(t, len(ids), shard0Rows+shard1Rows, "every row should be accounted for")
}
