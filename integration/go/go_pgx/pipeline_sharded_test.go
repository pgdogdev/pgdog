package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	textOID   = uint32(25)
	bigintOID = uint32(20)
)

func fieldNames(description *pgconn.StatementDescription) []string {
	names := []string{}
	for _, field := range description.Fields {
		names = append(names, field.Name)
	}
	return names
}

// pgx folds ParseComplete, ParameterDescription and RowDescription into one
// StatementDescription, so dropping any of the three desynchronises the read.
func nextStatement(t *testing.T, pipeline *pgconn.Pipeline) *pgconn.StatementDescription {
	t.Helper()

	result, err := pipeline.GetResults()
	require.NoError(t, err)

	description, ok := result.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected a statement description, got %T", result)
	return description
}

// A pipelined prepare phase describes several statements before one Sync, and
// pgx reads the replies positionally. The aggregate and the ORDER BY send the
// request to every shard, which is what puts it through the multi-shard path.
func TestPipelinedPrepareDescribesEveryStatement(t *testing.T) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, testConnStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	pipeline := conn.PgConn().StartPipeline(ctx)

	pipeline.SendPrepare("pipe1", "SELECT count(*) FROM sharded", nil)
	pipeline.SendPrepare("pipe2", "SELECT id, $1::text AS second FROM sharded ORDER BY id", nil)
	pipeline.SendPrepare(
		"pipe3",
		"SELECT id, $1::text AS second, $2::bigint AS third FROM sharded ORDER BY id",
		nil,
	)
	// Binary results, as every pgx query asks for.
	pipeline.SendQueryPrepared("pipe1", nil, nil, []int16{1})
	require.NoError(t, pipeline.Sync())

	first := nextStatement(t, pipeline)
	assert.Empty(t, first.ParamOIDs)
	assert.Equal(t, []string{"count"}, fieldNames(first))

	second := nextStatement(t, pipeline)
	assert.Equal(t, []uint32{textOID}, second.ParamOIDs)
	assert.Equal(t, []string{"id", "second"}, fieldNames(second))

	third := nextStatement(t, pipeline)
	assert.Equal(t, []uint32{textOID, bigintOID}, third.ParamOIDs)
	assert.Equal(t, []string{"id", "second", "third"}, fieldNames(third))

	result, err := pipeline.GetResults()
	require.NoError(t, err)
	reader, ok := result.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected a result reader, got %T", result)
	require.NoError(t, reader.Read().Err)

	result, err = pipeline.GetResults()
	require.NoError(t, err)
	require.IsType(t, &pgconn.PipelineSync{}, result)

	require.NoError(t, pipeline.Close())
}

// pgx asks for binary results, and pgdog decodes the rows itself to merge them.
// Read as text these ids sort with 10 before 2.
func TestShardedOrderByMergesBinaryRows(t *testing.T) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, testConnStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "TRUNCATE TABLE sharded")
	require.NoError(t, err)

	want := []int64{}
	for id := int64(1); id <= 20; id++ {
		_, err = conn.Exec(ctx, "INSERT INTO sharded (id, value) VALUES ($1, $2)", id, "row")
		require.NoError(t, err)
		want = append(want, id)
	}

	// Nothing is merged unless both shards hold rows.
	assert.NotZero(t, countOnShardByComment(t, conn, 0, 1), "shard 0 holds no rows")
	assert.NotZero(t, countOnShardByComment(t, conn, 1, 11), "shard 1 holds no rows")

	rows, err := conn.Query(ctx, "SELECT id FROM sharded ORDER BY id")
	require.NoError(t, err)

	got := []int64{}
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		got = append(got, id)
	}
	require.NoError(t, rows.Err())
	rows.Close()

	assert.Equal(t, want, got, "rows must arrive merge-sorted across shards")

	_, err = conn.Exec(ctx, "TRUNCATE TABLE sharded")
	require.NoError(t, err)
}
