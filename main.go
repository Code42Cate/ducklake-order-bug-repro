package main

// Minimal reproduction of DuckLake bug:
// ORDER BY ignores WHERE clause when data is inlined (stored in PostgreSQL, not yet flushed to S3)
//
// To run:
//
//	docker compose up repro

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

const (
	pgDSN      = "host=postgres port=5432 dbname=ducklake user=ducklake password=ducklake"
	s3Key      = "rustfsadmin"
	s3Secret   = "rustfsadmin"
	s3Endpoint = "s3:9000"
)

func main() {
	fmt.Println("connecting")
	db, connector := connect()
	defer db.Close()
	defer connector.Close()

	fmt.Println("creating table")
	mustExec(db, `
		CREATE TABLE IF NOT EXISTS lake.test_data (
			id VARCHAR NOT NULL,
			category VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
	`)

	fmt.Println("inserting 6 rows")
	now := time.Now()
	for i := range 3 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		mustExec(db, `INSERT INTO lake.test_data VALUES (?, ?, ?)`,
			fmt.Sprintf("a_%d", i), "A", ts)
		mustExec(db, `INSERT INTO lake.test_data VALUES (?, ?, ?)`,
			fmt.Sprintf("b_%d", i), "B", ts)
	}

	fmt.Println("query: WHERE category='A' ORDER BY created_at DESC LIMIT 3")
	fmt.Println("before flush:")
	runTest(db)

	fmt.Println("flushing to s3")
	if err := flush(db); err != nil {
		log.Fatal("flush failed: ", err)
	}

	fmt.Println("query: WHERE category='A' ORDER BY created_at DESC LIMIT 3")
	fmt.Println("after flush:")
	runTest(db)
}

func connect() (*sql.DB, *duckdb.Connector) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)

	fmt.Println("loading ducklake extension")
	mustExec(db, `INSTALL ducklake; LOAD ducklake;`)

	fmt.Println("configuring s3 secret")
	mustExec(db, fmt.Sprintf(`
		CREATE OR REPLACE SECRET s3_secret (
			TYPE s3, PROVIDER config,
			KEY_ID '%s', SECRET '%s', ENDPOINT '%s',
			USE_SSL false, URL_STYLE 'path'
		);
	`, s3Key, s3Secret, s3Endpoint))

	fmt.Println("attaching ducklake")
	mustExec(db, fmt.Sprintf(`
		ATTACH 'ducklake:postgres:%s' AS lake (
			DATA_PATH 's3://ducklake/data',
			DATA_INLINING_ROW_LIMIT 100
		);
	`, pgDSN))

	return db, connector
}

// flush uses explicit transaction like pkg/ducklake/client.go does
func flush(db *sql.DB) error {
	ctx := context.Background()

	// Get a fresh connection from the pool
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Begin explicit transaction for the flush operation
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	_, err = tx.ExecContext(ctx, "CALL ducklake_flush_inlined_data('lake');")
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func runTest(db *sql.DB) {
	rows, err := db.Query(`SELECT id, category FROM lake.test_data WHERE category = 'A' ORDER BY created_at DESC LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, cat string
		rows.Scan(&id, &cat)
		status := "ok"
		if cat != "A" {
			status = "BUG"
		}
		fmt.Printf("  %s %s [%s]\n", id, cat, status)
	}
}

func mustExec(db *sql.DB, query string, args ...any) {
	if _, err := db.Exec(query, args...); err != nil {
		log.Fatalf("Exec failed: %s\nError: %v", query, err)
	}
}
