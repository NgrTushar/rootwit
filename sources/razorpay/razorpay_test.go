package razorpay

import (
	"testing"
	"time"

	"github.com/rootwit/rootwit/types"
)

// --- toUnixTimestamp ---

func TestToUnixTimestamp(t *testing.T) {
	cases := []struct {
		name  string
		input any
		want  int64
	}{
		{"nil", nil, 0},
		{"float64", float64(1700000000), 1700000000},
		{"int64", int64(1700000000), 1700000000},
		{"RFC3339 string", "2023-11-14T22:13:20Z", 1700000000},
		{"RFC3339Nano string", "2023-11-14T22:13:20.000000000Z", 1700000000},
		{"numeric string", "1700000000", 1700000000},
		{"time.Time", time.Unix(1700000000, 0).UTC(), 1700000000},
		{"garbage string", "not-a-time", 0},
	}

	for _, tc := range cases {
		got := toUnixTimestamp(tc.input)
		if got != tc.want {
			t.Errorf("toUnixTimestamp(%q) = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// --- GetSchema ---

func TestGetSchema_SupportedEntities(t *testing.T) {
	src := &RazorpaySource{}
	for _, entity := range supportedEntities {
		schema, err := src.GetSchema(entity)
		if err != nil {
			t.Errorf("GetSchema(%q): unexpected error: %v", entity, err)
			continue
		}
		if schema.TableName != entity {
			t.Errorf("GetSchema(%q).TableName = %q, want %q", entity, schema.TableName, entity)
		}
		if len(schema.Fields) == 0 {
			t.Errorf("GetSchema(%q): empty fields", entity)
		}
		// Every schema must have a created_at TIMESTAMP field.
		hasCreatedAt := false
		for _, f := range schema.Fields {
			if f.Name == "created_at" && f.Type == types.FieldTypeTimestamp {
				hasCreatedAt = true
			}
		}
		if !hasCreatedAt {
			t.Errorf("GetSchema(%q): missing created_at TIMESTAMP field", entity)
		}
	}
}

func TestGetSchema_UnsupportedEntity(t *testing.T) {
	src := &RazorpaySource{}
	_, err := src.GetSchema("invoices")
	if err == nil {
		t.Fatal("expected error for unsupported entity, got nil")
	}
}

// --- GetTables ---

func TestGetTables(t *testing.T) {
	src := &RazorpaySource{}
	tables, err := src.GetTables()
	if err != nil {
		t.Fatalf("GetTables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("GetTables: empty list")
	}
	// Check all known entities are present.
	tableSet := make(map[string]bool)
	for _, t := range tables {
		tableSet[t] = true
	}
	for _, want := range []string{"payments", "orders", "refunds", "customers"} {
		if !tableSet[want] {
			t.Errorf("GetTables: missing %q", want)
		}
	}
}

// --- parseRow + coerceField ---

func TestParseRow_Payments(t *testing.T) {
	schema := entitySchemas["payments"]

	raw := map[string]interface{}{
		"id":               "pay_abc123",
		"entity":           "payment",
		"amount":           float64(50000),
		"currency":         "INR",
		"status":           "captured",
		"order_id":         "order_xyz",
		"invoice_id":       nil,
		"international":    false,
		"method":           "upi",
		"amount_refunded":  float64(0),
		"refund_status":    nil,
		"captured":         true,
		"description":      "Test payment",
		"card_id":          nil,
		"bank":             nil,
		"wallet":           nil,
		"vpa":              "user@upi",
		"email":            "user@example.com",
		"contact":          "+919876543210",
		"fee":              float64(1180),
		"tax":              float64(180),
		"error_code":       nil,
		"error_description": nil,
		"created_at":       float64(1700000000),
	}

	row := parseRow(raw, schema)

	// id stays as string.
	if row["id"] != "pay_abc123" {
		t.Errorf("id: got %v", row["id"])
	}
	// amount converted from float64 → int64.
	if row["amount"] != int64(50000) {
		t.Errorf("amount: got %T %v, want int64(50000)", row["amount"], row["amount"])
	}
	// captured stays bool.
	if row["captured"] != true {
		t.Errorf("captured: got %v", row["captured"])
	}
	// created_at converted from Unix float64 → time.Time.
	ts, ok := row["created_at"].(time.Time)
	if !ok {
		t.Fatalf("created_at: got %T, want time.Time", row["created_at"])
	}
	if ts.Unix() != 1700000000 {
		t.Errorf("created_at unix: got %d, want 1700000000", ts.Unix())
	}
	// nil fields stay nil.
	if row["invoice_id"] != nil {
		t.Errorf("invoice_id: expected nil, got %v", row["invoice_id"])
	}
}

func TestParseRow_Orders_NotesJSON(t *testing.T) {
	schema := entitySchemas["orders"]

	raw := map[string]interface{}{
		"id":          "order_abc",
		"entity":      "order",
		"amount":      float64(100000),
		"amount_paid": float64(100000),
		"amount_due":  float64(0),
		"currency":    "INR",
		"receipt":     "rcpt_001",
		"offer_id":    nil,
		"status":      "paid",
		"attempts":    float64(1),
		"notes":       map[string]interface{}{"key": "value", "ref": "abc"},
		"created_at":  float64(1700000000),
	}

	row := parseRow(raw, schema)

	// notes serialized to JSON string.
	notesStr, ok := row["notes"].(string)
	if !ok {
		t.Fatalf("notes: got %T, want string", row["notes"])
	}
	if len(notesStr) == 0 {
		t.Error("notes: empty JSON string")
	}
}

func TestCoerceField_NilAlwaysNil(t *testing.T) {
	fields := []types.Field{
		{Name: "f", Type: types.FieldTypeString},
		{Name: "f", Type: types.FieldTypeInt64},
		{Name: "f", Type: types.FieldTypeTimestamp},
		{Name: "f", Type: types.FieldTypeJSON},
		{Name: "f", Type: types.FieldTypeBool},
	}
	for _, f := range fields {
		if got := coerceField(f, nil); got != nil {
			t.Errorf("coerceField(%s, nil) = %v, want nil", f.Type, got)
		}
	}
}
