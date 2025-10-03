package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestConnectivityWithoutTLS(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.NoError(t, err)
	defer conn.Close(ctx)

	err = conn.Ping(ctx)
	assert.NoError(t, err)
}

func TestConnectivityWithTLS(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=require")
	assert.NoError(t, err)
	defer conn.Close(ctx)

	err = conn.Ping(ctx)
	assert.NoError(t, err)
}

func TestConnectivityWithPassthrough(t *testing.T) {
	ctx := context.Background()

	adminConn, err := pgx.Connect(ctx, "postgres://admin:pgdog@127.0.0.1:6432/admin")
	assert.NoError(t, err)
	defer adminConn.Close(ctx)

	_, err = adminConn.Exec(ctx, "SET passthrough_auth TO 'enabled'", pgx.QueryExecModeSimpleProtocol)
	assert.NoError(t, err)

	conn, err := pgx.Connect(ctx, "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.NoError(t, err)
	defer conn.Close(ctx)

	err = conn.Ping(ctx)
	assert.NoError(t, err)

	_, err = pgx.Connect(ctx, "postgres://pgdog:wrong_password@127.0.0.1:6432/pgdog?sslmode=disable")
	assert.Error(t, err)
}
