package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func GetPool() *pgxpool.Pool {
	config, err := pgxpool.ParseConfig("postgres://postgres:postgres@127.0.0.1:6432/postgres?sslmode=disable")

	if err != nil {
		panic(err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)

	if err != nil {
		panic(err)
	}

	return pool
}
