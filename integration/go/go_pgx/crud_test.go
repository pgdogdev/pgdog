package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

const (
	testConnStr = "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?sslmode=disable"
)

func setupDB(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS customers (
	customer_id BIGSERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT UNIQUE NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS orders (
	order_id BIGSERIAL PRIMARY KEY,
	customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
	amount NUMERIC NOT NULL,
	order_date TIMESTAMP NOT NULL DEFAULT now()
);
`)
	return err
}

func teardownDB(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `DROP TABLE IF EXISTS orders; DROP TABLE IF EXISTS customers;`)
	return err
}

func TestCRUDAndJoin(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, testConnStr)
	assert.NoError(t, err, "Failed to connect")
	defer conn.Close(ctx)

	assert.NoError(t, setupDB(ctx, conn), "setupDB failed")
	defer teardownDB(ctx, conn)

	// CREATE customer
	var customerID int64
	insertCustomer := `INSERT INTO customers (customer_id, name, email) VALUES ($1, $2, $3) RETURNING customer_id`
	name, email := "Bob", "bob@example.com"
	err = conn.QueryRow(ctx, insertCustomer, 1, name, email).Scan(&customerID)
	assert.NoError(t, err, "Insert customer failed")
	assert.NotZero(t, customerID, "customerID should not be zero")

	// CREATE order
	var orderID int64
	insertOrder := `INSERT INTO orders (customer_id, amount) VALUES ($1, $2) RETURNING order_id`
	amount := 123.45
	err = conn.QueryRow(ctx, insertOrder, customerID, amount).Scan(&orderID)
	assert.NoError(t, err, "Insert order failed")
	assert.NotZero(t, orderID, "orderID should not be zero")

	// SELECT with JOIN by customer_id
	var gotName string
	var gotAmount float64
	joinQuery := `
SELECT c.name, o.amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = $1 AND o.customer_id = $1 AND o.order_id = $2`
	err = conn.QueryRow(ctx, joinQuery, customerID, orderID).Scan(&gotName, &gotAmount)
	assert.NoError(t, err, "Join select failed")
	assert.Equal(t, name, gotName, "Join select returned wrong name")
	assert.Equal(t, amount, gotAmount, "Join select returned wrong amount")

	// UPDATE order amount (by customer_id)
	newAmount := 200.00
	updateOrder := `UPDATE orders SET amount = $1 WHERE order_id = $2 AND customer_id = $3`
	cmdTag, err := conn.Exec(ctx, updateOrder, newAmount, orderID, customerID)
	assert.NoError(t, err, "Update order failed")
	assert.EqualValues(t, 1, cmdTag.RowsAffected(), "Update should affect one row")

	// Confirm update with join
	err = conn.QueryRow(ctx, joinQuery, customerID, orderID).Scan(&gotName, &gotAmount)
	assert.NoError(t, err, "Join select after update failed")
	assert.Equal(t, newAmount, gotAmount, "Join select after update returned wrong amount")

	// DELETE order (by customer_id)
	deleteOrder := `DELETE FROM orders WHERE order_id = $1 AND customer_id = $2`
	cmdTag, err = conn.Exec(ctx, deleteOrder, orderID, customerID)
	assert.NoError(t, err, "Delete order failed")
	assert.EqualValues(t, 1, cmdTag.RowsAffected(), "Delete should affect one row")

	// Confirm order is deleted (join returns no rows)
	err = conn.QueryRow(ctx, joinQuery, customerID, orderID).Scan(&gotName, &gotAmount)
	assert.Error(t, err, "Join select after delete should fail")
}
