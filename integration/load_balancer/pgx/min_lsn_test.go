package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func primaryConn(t *testing.T) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@127.0.0.1:45000/postgres?sslmode=disable")
	require.NoError(t, err)
	return conn
}

// `postgres` uses exclude_primary, so reads normally go to replicas. With
// pgdog.min_lsn set, reads must go only to a replica that has replayed past it,
// falling back to the primary when none has.
func pgdogConn(t *testing.T) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@127.0.0.1:6432/postgres?sslmode=disable")
	require.NoError(t, err)
	return conn
}

func replicaTotal(table string) int64 {
	var total int64
	for _, call := range LoadStatsForReplicas(table) {
		total += call.Calls
	}
	return total
}

func TestMinLsnRoutesReads(t *testing.T) {
	p := primaryConn(t)
	_, err := p.Exec(context.Background(),
		"CREATE TABLE IF NOT EXISTS lb_minlsn_ok (id BIGINT); CREATE TABLE IF NOT EXISTS lb_minlsn_far (id BIGINT)")
	require.NoError(t, err)
	p.Close(context.Background())
	defer func() {
		p := primaryConn(t)
		p.Exec(context.Background(), "DROP TABLE IF EXISTS lb_minlsn_ok; DROP TABLE IF EXISTS lb_minlsn_far")
		p.Close(context.Background())
	}()

	// Let the replicas catch up and the LSN monitor (lsn_check_interval=1s) populate
	// each pool's replay LSN, so lsn_stats.valid() is true.
	time.Sleep(3 * time.Second)

	const reads = 20

	// Case A: min_lsn already satisfied -> reads served by replicas, not primary.
	ResetStats()
	a := pgdogConn(t)
	_, err = a.Exec(context.Background(), "SET pgdog.min_lsn = '0/0'")
	require.NoError(t, err)
	for i := range reads {
		_, err := a.Exec(context.Background(), "SELECT * FROM lb_minlsn_ok WHERE id = $1", int64(i))
		assert.NoError(t, err)
	}
	a.Close(context.Background())

	okReplicas := replicaTotal("lb_minlsn_ok")
	okPrimary := LoadStatsForPrimary("lb_minlsn_ok").Calls
	t.Logf("min_lsn=0/0: replicas=%d primary=%d", okReplicas, okPrimary)
	assert.Equal(t, int64(reads), okReplicas, "satisfied min_lsn must be served by replicas")
	assert.Equal(t, int64(0), okPrimary, "satisfied min_lsn must not touch the primary")

	// Case B: min_lsn far in the future -> no replica caught up. Default behavior
	// (min_lsn_primary_fallback off) is to error, not serve stale or hit the primary.
	ResetStats()
	b := pgdogConn(t)
	_, err = b.Exec(context.Background(), "SET pgdog.min_lsn = '7FFFFFFF/FFFFFFFF'")
	require.NoError(t, err)
	_, readErr := b.Exec(context.Background(), "SELECT * FROM lb_minlsn_far WHERE id = $1", int64(1))
	b.Close(context.Background())

	farReplicas := replicaTotal("lb_minlsn_far")
	farPrimary := LoadStatsForPrimary("lb_minlsn_far").Calls
	t.Logf("min_lsn=max: err=%v replicas=%d primary=%d", readErr, farReplicas, farPrimary)
	assert.Error(t, readErr, "unmet min_lsn must error, not serve a stale read")
	// Pin the wire message: clients (and the glow relation_writer gate) match on
	// this exact text to recognize the rejection and defer, so it must not drift.
	if readErr != nil {
		assert.Contains(t, readErr.Error(), "no replica caught up to the requested min_lsn",
			"unmet min_lsn must surface PgDog's NoReplicaCaughtUp message to the client")
		// The message also carries a catch-up ETA (eta ~Ns) so the client can size
		// its retry/defer to the real deficit instead of a fixed backoff.
		assert.Regexp(t, `eta ~\d+s`, readErr.Error(),
			"NoReplicaCaughtUp must report the catch-up ETA")
	}
	assert.Equal(t, int64(0), farReplicas, "unmet min_lsn must not be served by a behind replica")
	assert.Equal(t, int64(0), farPrimary, "unmet min_lsn must not hit the primary when fallback is off")
}
