package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type PgStatStatement struct {
	Calls int64
}

func LoadStatsForQuery(conn *pgx.Conn, query string) PgStatStatement {
	var statement PgStatStatement
	q := fmt.Sprintf("SELECT SUM(calls) FROM pg_stat_statements WHERE query ILIKE '%%%s%%'", query)
	rows, err := conn.Query(context.Background(), q)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		rows.Scan(&statement.Calls)
		break
	}

	return statement
}

func LoadStatsForPrimary(query string) PgStatStatement {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@127.0.0.1:5433/postgres")
	defer conn.Close(context.Background())

	if err != nil {
		panic(err)
	}

	return LoadStatsForQuery(conn, query)
}

func LoadStatsForReplicas(query string) []PgStatStatement {
	stats := make([]PgStatStatement, 0)

	for i := range 2 {
		port := 5434 + i
		conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres", port))
		defer conn.Close(context.Background())
		if err != nil {
			panic(err)
		}

		stats = append(stats, LoadStatsForQuery(conn, query))
	}

	return stats
}

func ResetStats() {
	for i := range 3 {
		port := 5433 + i
		conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port))
		defer conn.Close(context.Background())

		if err != nil {
			panic(err)
		}

		_, err = conn.Exec(context.Background(), "SELECT pg_stat_statements_reset()")
		if err != nil {
			panic(err)
		}
	}
}
