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
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@127.0.0.1:45000/postgres")

	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	return LoadStatsForQuery(conn, query)
}

func LoadStatsForReplicas(query string) []PgStatStatement {
	stats := make([]PgStatStatement, 0)

	for i := range 2 {
		port := 45001 + i
		conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres", port))
		if err != nil {
			panic(err)
		}
		defer conn.Close(context.Background())

		stats = append(stats, LoadStatsForQuery(conn, query))
	}

	return stats
}

func ResetStats() {
	for i := range 3 {
		port := 45000 + i
		conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port))

		if err != nil {
			panic(err)
		}

		defer conn.Close(context.Background())

		_, err = conn.Exec(context.Background(), "SELECT pg_stat_statements_reset()")
		if err != nil {
			panic(err)
		}
	}
}
