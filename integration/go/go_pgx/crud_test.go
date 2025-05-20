package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Migration function to create tables
func migrate(conn *pgx.Conn) error {
	_, err := conn.Exec(context.Background(), `
	CREATE TABLE IF NOT EXISTS companies (
		id BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL
	);
	
	CREATE TABLE IF NOT EXISTS users (
		id BIGSERIAL PRIMARY KEY,
		company_id BIGINT NOT NULL REFERENCES companies(id),
		name TEXT NOT NULL
	);
	
	CREATE TABLE IF NOT EXISTS notes (
		id BIGSERIAL PRIMARY KEY,
		company_id BIGINT NOT NULL REFERENCES companies(id),
		user_id BIGINT NOT NULL REFERENCES users(id),
		content TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS goals (
		id BIGSERIAL PRIMARY KEY,
		company_id BIGINT NOT NULL REFERENCES companies(id),
		user_id BIGINT NOT NULL REFERENCES users(id),
		description TEXT NOT NULL
	);
	`)
	return err
}

// Models

type Company struct {
	ID   int64
	Name string
}

type User struct {
	ID        int64
	CompanyID int64
	Name      string
}

type Note struct {
	ID        int64
	CompanyID int64
	UserID    int64
	Content   string
}

type Goal struct {
	ID          int64
	CompanyID   int64
	UserID      int64
	Description string
}

// CRUD Functions
func createCompany(conn *pgx.Conn, name string) (int64, error) {
	var id int64
	err := conn.QueryRow(context.Background(), "INSERT INTO companies(name) VALUES($1) RETURNING id", name).Scan(&id)
	return id, err
}

func createUser(conn *pgx.Conn, companyID int64, name string) (int64, error) {
	var id int64
	err := conn.QueryRow(context.Background(), "INSERT INTO users(company_id, name) VALUES($1, $2) RETURNING id", companyID, name).Scan(&id)
	return id, err
}

func createNote(conn *pgx.Conn, companyID, userID int64, content string) (int64, error) {
	var id int64
	err := conn.QueryRow(context.Background(), "INSERT INTO notes(company_id, user_id, content) VALUES($1, $2, $3) RETURNING id", companyID, userID, content).Scan(&id)
	return id, err
}

func updateNote(conn *pgx.Conn, companyID, noteID int64, content string) error {
	_, err := conn.Exec(context.Background(), "UPDATE notes SET content = $1 WHERE id = $2 AND company_id = $3", content, noteID, companyID)
	return err
}

func deleteNote(conn *pgx.Conn, companyID, noteID int64) error {
	_, err := conn.Exec(context.Background(), "DELETE FROM notes WHERE id = $1 AND company_id = $2", noteID, companyID)
	return err
}

func createGoal(conn *pgx.Conn, companyID, userID int64, description string) (int64, error) {
	var id int64
	err := conn.QueryRow(context.Background(), "INSERT INTO goals(company_id, user_id, description) VALUES($1, $2, $3) RETURNING id", companyID, userID, description).Scan(&id)
	return id, err
}

func updateGoal(conn *pgx.Conn, companyID, goalID int64, description string) error {
	_, err := conn.Exec(context.Background(), "UPDATE goals SET description = $1 WHERE id = $2 AND company_id = $3", description, goalID, companyID)
	return err
}

func deleteGoal(conn *pgx.Conn, companyID, goalID int64) error {
	_, err := conn.Exec(context.Background(), "DELETE FROM goals WHERE id = $1 AND company_id = $2", goalID, companyID)
	return err
}

// Query with joins: Get all notes for a company
func getCompanyNotes(conn *pgx.Conn, companyID int64) ([]Note, error) {
	rows, err := conn.Query(context.Background(), `
		SELECT n.id, n.company_id, n.user_id, n.content FROM notes n
		INNER JOIN users u ON n.user_id = u.id
		WHERE n.company_id = $1
	`, companyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var notes []Note
	for rows.Next() {
		var n Note
		if err := rows.Scan(&n.ID, &n.CompanyID, &n.UserID, &n.Content); err != nil {
			return nil, err
		}
		notes = append(notes, n)
	}
	return notes, nil
}

// Get all goals for a company
func getCompanyGoals(conn *pgx.Conn, companyID int64) ([]Goal, error) {
	rows, err := conn.Query(context.Background(), `
		SELECT g.id, g.company_id, g.user_id, g.description FROM goals g
		INNER JOIN users u ON g.user_id = u.id
		WHERE g.company_id = $1
	`, companyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var goals []Goal
	for rows.Next() {
		var g Goal
		if err := rows.Scan(&g.ID, &g.CompanyID, &g.UserID, &g.Description); err != nil {
			return nil, err
		}
		goals = append(goals, g)
	}
	return goals, nil
}

func TestCRUDApp(t *testing.T) {
	// Use PGX connection string or env var
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatalf("Failed to connect to DB: %v", err)
	}
	defer conn.Close(context.Background())

	if err := migrate(conn); err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Start test sequence
	companyID, err := createCompany(conn, "Acme Inc")
	if err != nil {
		t.Fatalf("createCompany: %v", err)
	}
	userID, err := createUser(conn, companyID, "Alice")
	if err != nil {
		t.Fatalf("createUser: %v", err)
	}
	noteID, err := createNote(conn, companyID, userID, "Test note")
	if err != nil {
		t.Fatalf("createNote: %v", err)
	}
	goalID, err := createGoal(conn, companyID, userID, "First goal")
	if err != nil {
		t.Fatalf("createGoal: %v", err)
	}
	if err := updateNote(conn, companyID, noteID, "Updated note"); err != nil {
		t.Fatalf("updateNote: %v", err)
	}
	if err := updateGoal(conn, companyID, goalID, "Updated goal"); err != nil {
		t.Fatalf("updateGoal: %v", err)
	}
	if err := deleteNote(conn, companyID, noteID); err != nil {
		t.Fatalf("deleteNote: %v", err)
	}
	if err := deleteGoal(conn, companyID, goalID); err != nil {
		t.Fatalf("deleteGoal: %v", err)
	}
	// Insert another note and goal
	_, _ = createNote(conn, companyID, userID, "Another note")
	_, _ = createGoal(conn, companyID, userID, "Another goal")
	// Query notes and goals with join
	notes, err := getCompanyNotes(conn, companyID)
	if err != nil {
		t.Fatalf("getCompanyNotes: %v", err)
	}
	goals, err := getCompanyGoals(conn, companyID)
	if err != nil {
		t.Fatalf("getCompanyGoals: %v", err)
	}
	if len(notes) == 0 || len(goals) == 0 {
		t.Fatalf("Expected notes/goals after insert")
	}
	fmt.Println("CRUD test completed successfully")
}
