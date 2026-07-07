package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countOnShardByComment(t *testing.T, conn *pgx.Conn, shard int, id int64) int {
	t.Helper()

	var count int
	err := conn.QueryRow(context.Background(),
		fmt.Sprintf("/* pgdog_shard: %d */ SELECT COUNT(*) FROM sharded WHERE id = $1", shard), id).Scan(&count)
	require.NoError(t, err)
	return count
}

func TestBatchRoutesShardedTable(t *testing.T) {
	ctx := context.Background()

	proxy, err := pgx.Connect(ctx, testConnStr)
	require.NoError(t, err)
	defer proxy.Close(ctx)

	_, err = proxy.Exec(ctx, "TRUNCATE TABLE sharded")
	require.NoError(t, err)

	const shard0ID int64 = 1
	const shard1ID int64 = 11

	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO sharded (id, value) VALUES ($1, $2)", shard0ID, "shard_0")
	batch.Queue("INSERT INTO sharded (id, value) VALUES ($1, $2)", shard1ID, "shard_1")

	results := proxy.SendBatch(ctx, batch)
	for i := range 2 {
		tag, err := results.Exec()
		require.NoError(t, err, "batch insert %d", i)
		assert.EqualValues(t, 1, tag.RowsAffected(), "batch insert %d rows affected", i)
	}
	require.NoError(t, results.Close())

	assert.Equal(t, 1, countOnShardByComment(t, proxy, 0, shard0ID), "id=%d must be on shard 0", shard0ID)
	assert.Equal(t, 0, countOnShardByComment(t, proxy, 1, shard0ID), "id=%d must not be on shard 1", shard0ID)
	assert.Equal(t, 0, countOnShardByComment(t, proxy, 0, shard1ID), "id=%d must not be on shard 0", shard1ID)
	assert.Equal(t, 1, countOnShardByComment(t, proxy, 1, shard1ID), "id=%d must be on shard 1", shard1ID)
}
