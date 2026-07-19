package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrimaryOnlyTables(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_plugin_primary_only (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_plugin_primary_only")

	time.Sleep(2 * time.Second)

	ResetStats()

	for i := range 50 {
		_, err = pool.Exec(context.Background(), "SELECT $1::bigint FROM lb_plugin_primary_only", int64(i))
		assert.NoError(t, err)
	}

	primaryCalls := LoadStatsForPrimary("lb_plugin_primary_only")
	assert.Equal(t, int64(50), primaryCalls.Calls)
}

func TestPrimaryOnlyTablesExplain(t *testing.T) {
	pool := GetPool()
	defer pool.Close()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_plugin_primary_only (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_plugin_primary_only")

	time.Sleep(2 * time.Second)

	rows, err := pool.Query(context.Background(), "EXPLAIN SELECT * FROM lb_plugin_primary_only")
	assert.NoError(t, err)

	var explainLines []string
	foundPluginAnnotation := false
	for rows.Next() {
		var line string
		err = rows.Scan(&line)
		assert.NoError(t, err)
		explainLines = append(explainLines, line)

		if strings.Contains(line, "plugin pgdog_primary_only_tables adjusted routing role=primary") {
			foundPluginAnnotation = true
		}
	}
	rows.Close()

	t.Logf("EXPLAIN output:\n%s", strings.Join(explainLines, "\n"))
	assert.True(t, foundPluginAnnotation, "EXPLAIN output should contain plugin routing annotation")
}
