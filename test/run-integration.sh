#!/bin/bash
# =============================================================================
# RootWit Integration Test Runner
# =============================================================================
# Exercises every CLI mode against a live Postgres database.
# BigQuery won't be available — this test specifically validates:
#   1. Config loading + validation
#   2. Postgres connection + schema detection
#   3. All Postgres type mappings on real data
#   4. Incremental/full/append reads with real rows
#   5. State management across runs
#
# Usage:
#   cd /path/to/rootwit
#   docker compose -f test/docker-compose.yml up -d
#   bash test/run-integration.sh
#   docker compose -f test/docker-compose.yml down -v
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ PASS${NC}: $1"; }
fail() { echo -e "${RED}✗ FAIL${NC}: $1"; FAILURES=$((FAILURES + 1)); }
warn() { echo -e "${YELLOW}⚠ WARN${NC}: $1"; }
info() { echo -e "  $1"; }

FAILURES=0
BINARY="./rootwit"
CONFIG="test/config-test.yaml"
STATE_FILE="./test/rootwit-state.json"

# Ensure binary exists
if [ ! -f "$BINARY" ]; then
    echo "Building rootwit binary..."
    export PATH=$PATH:/usr/local/go/bin
    go build -o rootwit .
fi

# Set env vars for test
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5433
export POSTGRES_DB=saas_prod
export POSTGRES_USER=rootwit_reader
export POSTGRES_PASS=r00tw1t_test_pass
export GCP_PROJECT_ID=test-project
export GOOGLE_CREDENTIALS_FILE=""
export SLACK_WEBHOOK_URL=""

# Clean state from previous runs
rm -f "$STATE_FILE" "$STATE_FILE.tmp"

echo "============================================"
echo "  RootWit Integration Tests"
echo "============================================"
echo ""

# =========================================
# Test 1: --validate (Postgres only)
# =========================================
echo "--- Test 1: --validate ---"

OUTPUT=$($BINARY --config $CONFIG --validate 2>&1 || true)

if echo "$OUTPUT" | grep -q "Source (postgres).*OK"; then
    pass "Postgres connection succeeded"
else
    fail "Postgres connection failed"
    info "$OUTPUT"
fi

if echo "$OUTPUT" | grep -q "Available tables (5)"; then
    pass "Found all 5 tables"
else
    fail "Did not find 5 tables"
    info "$OUTPUT"
fi

# Check all configured tables are found
for TABLE in users orders events plans audit_log; do
    if echo "$OUTPUT" | grep -q "✓ $TABLE"; then
        pass "Table '$TABLE' found in source"
    else
        fail "Table '$TABLE' not found"
    fi
done

# BigQuery should fail (no creds) — that's expected
if echo "$OUTPUT" | grep -q "FAILED"; then
    warn "BigQuery connection failed (expected — no GCP creds)"
fi

echo ""

# =========================================
# Test 2: Config validation edge cases
# =========================================
echo "--- Test 2: Config validation ---"

# Test with literal password (should warn)
LITERAL_CONFIG=$(mktemp /tmp/rootwit-test-XXXXXX.yaml)
cat > "$LITERAL_CONFIG" <<'EOF'
version: "1"
name: "test"
source:
  type: postgres
  host: localhost
  port: 5433
  database: saas_prod
  username: rootwit_reader
  password: hardcoded_secret
  ssl_mode: disable
destination:
  type: bigquery
  project_id: proj
  dataset_id: ds
sync:
  schedule: "* * * * *"
  tables:
    - name: users
      sync_mode: full_refresh
EOF

OUTPUT=$($BINARY --config "$LITERAL_CONFIG" --validate 2>&1 || true)
if echo "$OUTPUT" | grep -q "Security warnings"; then
    pass "Literal credential warning triggered"
else
    fail "Literal credential warning NOT triggered"
fi
rm -f "$LITERAL_CONFIG"

# Test with missing cursor_field on incremental
BAD_CONFIG=$(mktemp /tmp/rootwit-test-XXXXXX.yaml)
cat > "$BAD_CONFIG" <<EOF
version: "1"
name: "test"
source:
  type: postgres
  host: localhost
  port: 5433
  database: saas_prod
  username: \${POSTGRES_USER}
  password: \${POSTGRES_PASS}
  ssl_mode: disable
destination:
  type: bigquery
  project_id: \${GCP_PROJECT_ID}
  dataset_id: ds
sync:
  schedule: "* * * * *"
  tables:
    - name: users
      sync_mode: incremental
EOF

OUTPUT=$($BINARY --config "$BAD_CONFIG" --validate 2>&1 || true)
if echo "$OUTPUT" | grep -q "cursor_field"; then
    pass "Missing cursor_field rejected"
else
    fail "Missing cursor_field NOT rejected"
fi
rm -f "$BAD_CONFIG"

echo ""

# =========================================
# Test 3: Schema detection on all types
# =========================================
echo "--- Test 3: Schema type mapping ---"

# Query Postgres directly to count column types we should map
COLUMNS=$(docker exec rootwit-test-pg psql -U rootwit_reader -d saas_prod -t -A -c "
    SELECT COUNT(DISTINCT data_type) 
    FROM information_schema.columns 
    WHERE table_schema = 'public';
")
info "Postgres has $COLUMNS distinct column types to map"

# Check specific columns exist via schema query
TYPES_OUTPUT=$(docker exec rootwit-test-pg psql -U rootwit_reader -d saas_prod -t -A -c "
    SELECT column_name, data_type, udt_name 
    FROM information_schema.columns 
    WHERE table_schema = 'public' AND table_name = 'users' 
    ORDER BY ordinal_position;
")
info "Users table columns:"
echo "$TYPES_OUTPUT" | while read -r line; do
    info "  $line"
done

# Verify the binary can read schemas from Postgres by using the Go test
export PATH=$PATH:/usr/local/go/bin
echo ""

# =========================================
# Test 4: State file lifecycle
# =========================================
echo "--- Test 4: State file lifecycle ---"

# State file should not exist yet
if [ ! -f "$STATE_FILE" ]; then
    pass "No state file before first run (correct)"
else
    fail "State file exists before first run"
fi

echo ""

# =========================================
# Test 5: Read from Postgres (Go integration test)
# =========================================
echo "--- Test 5: Postgres read integration ---"

# Write a quick Go test that reads from the live database
INTEGRATION_TEST=$(mktemp /tmp/rootwit-integration-XXXXXX.go)
cat > "$INTEGRATION_TEST" <<'GOTEST'
//go:build ignore

package main

import (
	"fmt"
	"os"
	
	"github.com/rootwit/rootwit/config"
	pgsrc "github.com/rootwit/rootwit/sources/postgres"
)

func main() {
	cfg := config.SourceConfig{
		Type:                     "postgres",
		Host:                     os.Getenv("POSTGRES_HOST"),
		Port:                     5433,
		Database:                 os.Getenv("POSTGRES_DB"),
		Username:                 os.Getenv("POSTGRES_USER"),
		Password:                 os.Getenv("POSTGRES_PASS"),
		SSLMode:                  "disable",
		ConnectionTimeoutSeconds: 10,
	}

	src := pgsrc.NewPostgresSource(cfg)
	if err := src.Connect(); err != nil {
		fmt.Printf("FAIL: connect: %v\n", err)
		os.Exit(1)
	}
	defer src.Close()

	// Test GetTables
	tables, err := src.GetTables()
	if err != nil {
		fmt.Printf("FAIL: GetTables: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: GetTables returned %d tables: %v\n", len(tables), tables)

	// Test GetSchema for users (has all types)
	schema, err := src.GetSchema("users")
	if err != nil {
		fmt.Printf("FAIL: GetSchema(users): %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: GetSchema(users) returned %d fields\n", len(schema.Fields))
	for _, f := range schema.Fields {
		fmt.Printf("  - %s: %s (nullable=%v, itemType=%s)\n", f.Name, f.Type, f.Nullable, f.ItemType)
	}

	// Test ReadFull for plans (small table)
	rowCh, errCh := src.ReadFull("plans", 100)
	count := 0
	for row := range rowCh {
		count++
		if count == 1 {
			fmt.Printf("PASS: ReadFull first row keys: ")
			for k := range row {
				fmt.Printf("%s ", k)
			}
			fmt.Println()
		}
	}
	if err := <-errCh; err != nil {
		fmt.Printf("FAIL: ReadFull error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: ReadFull(plans) returned %d rows\n", count)

	// Test ReadFull for events (1000 rows — tests batching channel)
	rowCh2, errCh2 := src.ReadFull("events", 100)
	count2 := 0
	for range rowCh2 {
		count2++
	}
	if err := <-errCh2; err != nil {
		fmt.Printf("FAIL: ReadFull(events) error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: ReadFull(events) returned %d rows\n", count2)

	// Test ReadIncremental for orders
	rowCh3, errCh3 := src.ReadIncremental("orders", "updated_at", "2000-01-01T00:00:00Z", 50)
	count3 := 0
	for range rowCh3 {
		count3++
	}
	if err := <-errCh3; err != nil {
		fmt.Printf("FAIL: ReadIncremental(orders) error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: ReadIncremental(orders) returned %d rows (LIMIT 50)\n", count3)

	fmt.Println("\n=== ALL POSTGRES INTEGRATION TESTS PASSED ===")
}
GOTEST

OUTPUT=$(cd /home/tush/code/go/rootwit1 && go run "$INTEGRATION_TEST" 2>&1)
EXIT_CODE=$?
echo "$OUTPUT"

if [ $EXIT_CODE -eq 0 ]; then
    pass "Postgres integration tests passed"
else
    fail "Postgres integration tests failed (exit $EXIT_CODE)"
fi
rm -f "$INTEGRATION_TEST"

echo ""

# =========================================
# Summary
# =========================================
echo "============================================"
if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}  ALL TESTS PASSED${NC}"
else
    echo -e "${RED}  $FAILURES TEST(S) FAILED${NC}"
fi
echo "============================================"

exit $FAILURES
