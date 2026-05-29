//go:build ignore

// seed creates demo/sample.db with three tables: users, orders, events.
// Run once before starting the demo:
//
//	go run demo/seed/main.go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := filepath.Join("demo", "sample.db")

	// Remove stale file so we start clean.
	os.Remove(dbPath)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	exec := func(q string) {
		if _, err := db.Exec(q); err != nil {
			log.Fatalf("exec %q: %v", q, err)
		}
	}

	// --- users ---
	exec(`CREATE TABLE users (
		id         INTEGER PRIMARY KEY,
		name       TEXT NOT NULL,
		email      TEXT NOT NULL,
		plan       TEXT NOT NULL DEFAULT 'free',
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL
	)`)

	// --- orders ---
	exec(`CREATE TABLE orders (
		id         INTEGER PRIMARY KEY,
		user_id    INTEGER NOT NULL,
		amount     REAL NOT NULL,
		status     TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL
	)`)

	// --- events ---
	exec(`CREATE TABLE events (
		id         INTEGER PRIMARY KEY,
		user_id    INTEGER NOT NULL,
		event_type TEXT NOT NULL,
		properties JSON,
		created_at TIMESTAMP NOT NULL
	)`)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Seed users.
	userStmt, err := db.Prepare(`INSERT INTO users (id, name, email, plan, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("prepare users: %v", err)
	}
	defer userStmt.Close()

	users := []struct {
		id   int
		name string
		plan string
	}{
		{1, "Priya Sharma", "pro"},
		{2, "Arjun Mehta", "free"},
		{3, "Sunita Reddy", "pro"},
		{4, "Karan Patel", "free"},
		{5, "Divya Nair", "enterprise"},
	}
	for i, u := range users {
		created := base.Add(time.Duration(i*24) * time.Hour)
		updated := created.Add(time.Duration(i*3) * time.Hour)
		email := fmt.Sprintf("%s@example.com", u.name[:5])
		if _, err := userStmt.Exec(u.id, u.name, email, u.plan, ts(created), ts(updated)); err != nil {
			log.Fatalf("insert user %d: %v", u.id, err)
		}
	}

	// Seed orders.
	orderStmt, err := db.Prepare(`INSERT INTO orders (id, user_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("prepare orders: %v", err)
	}
	defer orderStmt.Close()

	orders := []struct {
		id     int
		userID int
		amount float64
		status string
		dayOff int
	}{
		{1, 1, 4999.00, "paid", 2},
		{2, 1, 4999.00, "paid", 32},
		{3, 2, 999.00, "pending", 5},
		{4, 3, 14999.00, "paid", 10},
		{5, 3, 14999.00, "paid", 40},
		{6, 4, 999.00, "failed", 15},
		{7, 5, 49999.00, "paid", 20},
		{8, 2, 999.00, "paid", 25},
	}
	for _, o := range orders {
		created := base.Add(time.Duration(o.dayOff*24) * time.Hour)
		if _, err := orderStmt.Exec(o.id, o.userID, o.amount, o.status, ts(created)); err != nil {
			log.Fatalf("insert order %d: %v", o.id, err)
		}
	}

	// Seed events.
	eventStmt, err := db.Prepare(`INSERT INTO events (id, user_id, event_type, properties, created_at) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("prepare events: %v", err)
	}
	defer eventStmt.Close()

	events := []struct {
		id        int
		userID    int
		eventType string
		props     string
		dayOff    int
		hrOff     int
	}{
		{1, 1, "login", `{"device":"mobile"}`, 0, 9},
		{2, 1, "dashboard_view", `{"page":"overview"}`, 0, 9},
		{3, 2, "signup", `{"referrer":"twitter"}`, 1, 14},
		{4, 3, "login", `{"device":"desktop"}`, 2, 10},
		{5, 1, "export", `{"format":"csv","rows":1523}`, 3, 11},
		{6, 2, "upgrade_click", `{"plan":"pro"}`, 5, 16},
		{7, 4, "signup", `{"referrer":"direct"}`, 7, 8},
		{8, 5, "login", `{"device":"desktop"}`, 10, 9},
		{9, 3, "export", `{"format":"parquet","rows":84201}`, 12, 15},
		{10, 1, "login", `{"device":"mobile"}`, 14, 7},
	}
	for _, e := range events {
		created := base.Add(time.Duration(e.dayOff*24+e.hrOff) * time.Hour)
		if _, err := eventStmt.Exec(e.id, e.userID, e.eventType, e.props, ts(created)); err != nil {
			log.Fatalf("insert event %d: %v", e.id, err)
		}
	}

	fmt.Printf("Created %s with %d users, %d orders, %d events\n",
		dbPath, len(users), len(orders), len(events))
}

func ts(t time.Time) string {
	return t.Format("2006-01-02T15:04:05Z")
}
