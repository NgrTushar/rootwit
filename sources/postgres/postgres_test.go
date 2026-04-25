package postgres

import (
	"context"
	"testing"

	"github.com/rootwit/rootwit/types"
)

// ==========================================================================
// Typemap unit tests — all 17 scalar OIDs + arrays + unknown fallback
// ==========================================================================

func TestMapOIDToFieldType_ScalarTypes(t *testing.T) {
	tests := []struct {
		name     string
		oid      uint32
		expected types.FieldType
	}{
		{"bool", OIDBool, types.FieldTypeBool},
		{"bytea", OIDBytea, types.FieldTypeBytes},
		{"int2/smallint", OIDInt2, types.FieldTypeInt64},
		{"int4/integer", OIDInt4, types.FieldTypeInt64},
		{"int8/bigint", OIDInt8, types.FieldTypeInt64},
		{"text", OIDText, types.FieldTypeString},
		{"varchar", OIDVarchar, types.FieldTypeString},
		{"bpchar/char", OIDBpchar, types.FieldTypeString},
		{"float4/real", OIDFloat4, types.FieldTypeFloat64},
		{"float8/double", OIDFloat8, types.FieldTypeFloat64},
		{"numeric", OIDNumeric, types.FieldTypeNumeric},
		{"date", OIDDate, types.FieldTypeDate},
		{"timestamp", OIDTimestamp, types.FieldTypeTimestamp},
		{"timestamptz", OIDTimestampTZ, types.FieldTypeTimestamp},
		{"uuid", OIDUUID, types.FieldTypeString},
		{"json", OIDJSON, types.FieldTypeJSON},
		{"jsonb", OIDJSONB, types.FieldTypeJSON},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft, itemType, known := MapOIDToFieldType(tt.oid)
			if !known {
				t.Errorf("OID %d should be known", tt.oid)
			}
			if ft != tt.expected {
				t.Errorf("OID %d: expected %s, got %s", tt.oid, tt.expected, ft)
			}
			if itemType != "" {
				t.Errorf("OID %d: expected empty itemType for scalar, got %s", tt.oid, itemType)
			}
		})
	}
}

func TestMapOIDToFieldType_ArrayTypes(t *testing.T) {
	tests := []struct {
		name         string
		oid          uint32
		expectedItem types.FieldType
	}{
		{"bool[]", OIDBoolArray, types.FieldTypeBool},
		{"int2[]", OIDInt2Array, types.FieldTypeInt64},
		{"int4[]", OIDInt4Array, types.FieldTypeInt64},
		{"int8[]", OIDInt8Array, types.FieldTypeInt64},
		{"text[]", OIDTextArray, types.FieldTypeString},
		{"varchar[]", OIDVarcharArray, types.FieldTypeString},
		{"float4[]", OIDFloat4Array, types.FieldTypeFloat64},
		{"float8[]", OIDFloat8Array, types.FieldTypeFloat64},
		{"numeric[]", OIDNumericArray, types.FieldTypeNumeric},
		{"date[]", OIDDateArray, types.FieldTypeDate},
		{"timestamp[]", OIDTimestampArray, types.FieldTypeTimestamp},
		{"timestamptz[]", OIDTimestampTZArray, types.FieldTypeTimestamp},
		{"uuid[]", OIDUUIDArray, types.FieldTypeString},
		{"jsonb[]", OIDJSONBArray, types.FieldTypeJSON},
		{"json[]", OIDJSONArray, types.FieldTypeJSON},
		{"bytea[]", OIDByteaArray, types.FieldTypeBytes},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft, itemType, known := MapOIDToFieldType(tt.oid)
			if !known {
				t.Errorf("array OID %d should be known", tt.oid)
			}
			if ft != types.FieldTypeRepeated {
				t.Errorf("array OID %d: expected REPEATED, got %s", tt.oid, ft)
			}
			if itemType != tt.expectedItem {
				t.Errorf("array OID %d: expected item type %s, got %s", tt.oid, tt.expectedItem, itemType)
			}
		})
	}
}

func TestMapOIDToFieldType_UnknownFallsToString(t *testing.T) {
	// Unknown OIDs (e.g., custom composite types) must NEVER panic.
	// They should map to STRING with known=false.
	unknownOIDs := []uint32{99999, 12345, 0, 42}

	for _, oid := range unknownOIDs {
		ft, _, known := MapOIDToFieldType(oid)
		if known {
			t.Errorf("OID %d should not be known", oid)
		}
		if ft != types.FieldTypeString {
			t.Errorf("OID %d: unknown should map to STRING, got %s", oid, ft)
		}
	}
}

// ==========================================================================
// Type name mapping tests
// ==========================================================================

func TestMapColumnNameToFieldType(t *testing.T) {
	tests := []struct {
		pgType   string
		expected types.FieldType
		known    bool
	}{
		{"boolean", types.FieldTypeBool, true},
		{"smallint", types.FieldTypeInt64, true},
		{"integer", types.FieldTypeInt64, true},
		{"bigint", types.FieldTypeInt64, true},
		{"real", types.FieldTypeFloat64, true},
		{"double precision", types.FieldTypeFloat64, true},
		{"numeric", types.FieldTypeNumeric, true},
		{"text", types.FieldTypeString, true},
		{"character varying", types.FieldTypeString, true},
		{"uuid", types.FieldTypeString, true},
		{"timestamp without time zone", types.FieldTypeTimestamp, true},
		{"timestamp with time zone", types.FieldTypeTimestamp, true},
		{"date", types.FieldTypeDate, true},
		{"json", types.FieldTypeJSON, true},
		{"jsonb", types.FieldTypeJSON, true},
		{"bytea", types.FieldTypeBytes, true},
		// Array notation.
		{"integer[]", types.FieldTypeRepeated, true},
		{"text[]", types.FieldTypeRepeated, true},
		// Unknown.
		{"citext", types.FieldTypeString, false},
		{"custom_enum", types.FieldTypeString, false},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			ft, _, known := MapColumnNameToFieldType(tt.pgType)
			if known != tt.known {
				t.Errorf("type %q: expected known=%v, got %v", tt.pgType, tt.known, known)
			}
			if ft != tt.expected {
				t.Errorf("type %q: expected %s, got %s", tt.pgType, tt.expected, ft)
			}
		})
	}
}

// ==========================================================================
// Verify PostgresSource implements SourceConnector interface
// ==========================================================================

func TestPostgresSourceImplementsInterface(t *testing.T) {
	// This is a compile-time check. If PostgresSource doesn't implement
	// SourceConnector, this file won't compile.
	var _ interface {
		Connect() error
		GetTables() ([]string, error)
		GetSchema(string) (types.Schema, error)
		ReadIncremental(context.Context, string, string, any, int) (<-chan types.Row, <-chan error)
		ReadFull(context.Context, string, int) (<-chan types.Row, <-chan error)
		Close() error
	} = &PostgresSource{}
}
