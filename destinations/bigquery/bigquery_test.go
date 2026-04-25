package bigquery

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/rootwit/rootwit/types"
)

// ==========================================================================
// Typemap tests — all 10 internal types to BigQuery
// ==========================================================================

func TestMapFieldTypeToBigQuery(t *testing.T) {
	tests := []struct {
		internal types.FieldType
		expected bq.FieldType
	}{
		{types.FieldTypeString, bq.StringFieldType},
		{types.FieldTypeInt64, bq.IntegerFieldType},
		{types.FieldTypeFloat64, bq.FloatFieldType},
		{types.FieldTypeNumeric, bq.NumericFieldType},
		{types.FieldTypeBool, bq.BooleanFieldType},
		{types.FieldTypeTimestamp, bq.TimestampFieldType},
		{types.FieldTypeDate, bq.DateFieldType},
		{types.FieldTypeJSON, bq.JSONFieldType},
		{types.FieldTypeBytes, bq.BytesFieldType},
	}

	for _, tt := range tests {
		t.Run(string(tt.internal), func(t *testing.T) {
			got := MapFieldTypeToBigQuery(tt.internal)
			if got != tt.expected {
				t.Errorf("MapFieldTypeToBigQuery(%s) = %v, want %v", tt.internal, got, tt.expected)
			}
		})
	}
}

func TestMapFieldTypeToBigQuery_UnknownFallsToString(t *testing.T) {
	got := MapFieldTypeToBigQuery("UNKNOWN_TYPE")
	if got != bq.StringFieldType {
		t.Errorf("unknown type should map to STRING, got %v", got)
	}
}

// ==========================================================================
// Schema conversion tests
// ==========================================================================

func TestSchemaToMetadata(t *testing.T) {
	schema := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64, Nullable: false},
			{Name: "name", Type: types.FieldTypeString, Nullable: true},
			{Name: "created_at", Type: types.FieldTypeTimestamp, Nullable: true},
			{Name: "tags", Type: types.FieldTypeRepeated, Nullable: false, ItemType: types.FieldTypeString},
		},
	}

	bqSchema := SchemaToMetadata(schema)
	if len(bqSchema) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(bqSchema))
	}

	// id: INT64, REQUIRED
	if bqSchema[0].Name != "id" || bqSchema[0].Type != bq.IntegerFieldType || !bqSchema[0].Required {
		t.Errorf("id field: got name=%s type=%v required=%v", bqSchema[0].Name, bqSchema[0].Type, bqSchema[0].Required)
	}

	// name: STRING, NULLABLE
	if bqSchema[1].Name != "name" || bqSchema[1].Type != bq.StringFieldType || bqSchema[1].Required {
		t.Errorf("name field: got name=%s type=%v required=%v", bqSchema[1].Name, bqSchema[1].Type, bqSchema[1].Required)
	}

	// created_at: TIMESTAMP, NULLABLE
	if bqSchema[2].Name != "created_at" || bqSchema[2].Type != bq.TimestampFieldType {
		t.Errorf("created_at field: got name=%s type=%v", bqSchema[2].Name, bqSchema[2].Type)
	}

	// tags: STRING, REPEATED
	if bqSchema[3].Name != "tags" || bqSchema[3].Type != bq.StringFieldType || !bqSchema[3].Repeated {
		t.Errorf("tags field: got name=%s type=%v repeated=%v", bqSchema[3].Name, bqSchema[3].Type, bqSchema[3].Repeated)
	}
}

func TestMetadataToSchema(t *testing.T) {
	bqSchema := bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType, Required: true},
		{Name: "email", Type: bq.StringFieldType, Required: false},
		{Name: "scores", Type: bq.FloatFieldType, Repeated: true},
	}

	schema := MetadataToSchema("users", bqSchema)
	if schema.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", schema.TableName)
	}
	if len(schema.Fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(schema.Fields))
	}

	// id: INT64, not nullable
	if schema.Fields[0].Name != "id" || schema.Fields[0].Type != types.FieldTypeInt64 || schema.Fields[0].Nullable {
		t.Errorf("id: got name=%s type=%s nullable=%v", schema.Fields[0].Name, schema.Fields[0].Type, schema.Fields[0].Nullable)
	}

	// email: STRING, nullable
	if schema.Fields[1].Name != "email" || schema.Fields[1].Type != types.FieldTypeString || !schema.Fields[1].Nullable {
		t.Errorf("email: got name=%s type=%s nullable=%v", schema.Fields[1].Name, schema.Fields[1].Type, schema.Fields[1].Nullable)
	}

	// scores: REPEATED FLOAT64
	if schema.Fields[2].Name != "scores" || schema.Fields[2].Type != types.FieldTypeRepeated || schema.Fields[2].ItemType != types.FieldTypeFloat64 {
		t.Errorf("scores: got name=%s type=%s itemType=%s", schema.Fields[2].Name, schema.Fields[2].Type, schema.Fields[2].ItemType)
	}
}

// ==========================================================================
// Round-trip test: internal → BQ → internal
// ==========================================================================

func TestSchemaRoundTrip(t *testing.T) {
	original := types.Schema{
		TableName: "orders",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64, Nullable: false},
			{Name: "total", Type: types.FieldTypeNumeric, Nullable: true},
			{Name: "data", Type: types.FieldTypeJSON, Nullable: true},
			{Name: "items", Type: types.FieldTypeRepeated, ItemType: types.FieldTypeString},
		},
	}

	// Convert to BQ schema and back.
	bqSchema := SchemaToMetadata(original)
	roundTripped := MetadataToSchema("orders", bqSchema)

	if len(roundTripped.Fields) != len(original.Fields) {
		t.Fatalf("field count mismatch: %d vs %d", len(roundTripped.Fields), len(original.Fields))
	}

	for i, orig := range original.Fields {
		rt := roundTripped.Fields[i]
		if orig.Name != rt.Name {
			t.Errorf("field %d: name mismatch %q vs %q", i, orig.Name, rt.Name)
		}
		if orig.Type != rt.Type {
			t.Errorf("field %d (%s): type mismatch %s vs %s", i, orig.Name, orig.Type, rt.Type)
		}
		if orig.ItemType != rt.ItemType {
			t.Errorf("field %d (%s): itemType mismatch %s vs %s", i, orig.Name, orig.ItemType, rt.ItemType)
		}
	}
}

// ==========================================================================
// Interface compliance check
// ==========================================================================

func TestBigQueryDestinationImplementsInterface(t *testing.T) {
	var _ interface {
		Connect() error
		GetSchema(string) (*types.Schema, error)
		CreateTable(string, types.Schema) error
		AlterTable(string, types.SchemaChanges) error
		WriteBatch(string, []types.Row) (types.WriteResult, error)
		TruncateTable(string) error
		Close() error
	} = &BigQueryDestination{}
}
