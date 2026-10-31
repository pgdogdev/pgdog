package main

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A range on the sharding key goes to every shard. lib/pq asks for integers
// in binary and numerics in text, so the rows come back with result formats (1, 0).
const districtSumsQuery = "SELECT d_id, SUM(amount) FROM pq_district_sums WHERE customer_id > $1 GROUP BY d_id ORDER BY d_id"

type districtSum struct {
	district int64
	sum      string
}

type query func(args ...any) (*sql.Rows, error)

// lib/pq sends the query with arguments as an unnamed statement.
func unnamed(conn *sql.DB) query {
	return func(args ...any) (*sql.Rows, error) {
		return conn.Query(districtSumsQuery, args...)
	}
}

func districtSums(query query) ([]districtSum, error) {
	rows, err := query(0)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sums []districtSum
	for rows.Next() {
		var sum districtSum
		if err := rows.Scan(&sum.district, &sum.sum); err != nil {
			return nil, err
		}
		sums = append(sums, sum)
	}
	return sums, rows.Err()
}

// Changing a column type (like resharding does when it widens integer keys
// to bigint) must not leave PgDog merging rows with the old column types.
func TestCrossShardAggregateAfterColumnTypeChange(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?sslmode=disable")
	require.NoError(t, err)
	defer conn.Close()

	admin, err := sql.Open("postgres", "postgres://admin:pgdog@127.0.0.1:6432/admin?sslmode=disable")
	require.NoError(t, err)
	defer admin.Close()

	// New server connections, like after a cutover: Postgres plans the statements
	// from scratch, while they stay in PgDog's cache.
	reconnect := func() {
		_, err := admin.Exec("RECONNECT")
		require.NoError(t, err)
	}

	for _, ddl := range []string{
		"DROP TABLE IF EXISTS pq_district_sums",
		"CREATE TABLE pq_district_sums (customer_id BIGINT, d_id INTEGER, amount NUMERIC)",
	} {
		_, err = conn.Exec(ddl)
		require.NoError(t, err)
	}
	defer conn.Exec("DROP TABLE IF EXISTS pq_district_sums")

	for customer := 1; customer <= 20; customer++ {
		_, err = conn.Exec(
			"INSERT INTO pq_district_sums (customer_id, d_id, amount) VALUES ($1, $2, $3)",
			customer, customer%2, customer,
		)
		require.NoError(t, err)
	}
	check := func(name string, query query) {
		sums, err := districtSums(query)
		if assert.NoError(t, err, name) {
			assert.Equal(t, []districtSum{{0, "110"}, {1, "100"}}, sums, name)
		}
	}
	reconnect()

	// Kept open so PgDog keeps this statement in its cache through the ALTER.
	integer, err := conn.Prepare(districtSumsQuery)
	require.NoError(t, err)
	defer integer.Close()

	check("named statement, integer column", integer.Query)
	check("unnamed statement, integer column", unnamed(conn))

	_, err = conn.Exec("ALTER TABLE pq_district_sums ALTER COLUMN d_id TYPE BIGINT")
	require.NoError(t, err)
	reconnect()

	bigint, err := conn.Prepare(districtSumsQuery)
	require.NoError(t, err)
	defer bigint.Close()

	check("named statement, bigint column", bigint.Query)
	check("unnamed statement, bigint column", unnamed(conn))
}
