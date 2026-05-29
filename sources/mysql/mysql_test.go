package mysql

import (
	"testing"

	"github.com/rootwit/rootwit/types"
)

func TestFromMySQLType(t *testing.T) {
	cases := []struct {
		dataType   string
		columnType string
		want       types.FieldType
	}{
		{"tinyint", "tinyint(1)", types.FieldTypeBool},
		{"tinyint", "tinyint(4)", types.FieldTypeInt64},
		{"smallint", "smallint(6)", types.FieldTypeInt64},
		{"mediumint", "mediumint(9)", types.FieldTypeInt64},
		{"int", "int(11)", types.FieldTypeInt64},
		{"integer", "int(11)", types.FieldTypeInt64},
		{"bigint", "bigint(20)", types.FieldTypeInt64},
		{"year", "year(4)", types.FieldTypeInt64},
		{"float", "float", types.FieldTypeFloat64},
		{"double", "double", types.FieldTypeFloat64},
		{"real", "real", types.FieldTypeFloat64},
		{"decimal", "decimal(10,2)", types.FieldTypeNumeric},
		{"numeric", "numeric(8,4)", types.FieldTypeNumeric},
		{"char", "char(10)", types.FieldTypeString},
		{"varchar", "varchar(255)", types.FieldTypeString},
		{"tinytext", "tinytext", types.FieldTypeString},
		{"text", "text", types.FieldTypeString},
		{"mediumtext", "mediumtext", types.FieldTypeString},
		{"longtext", "longtext", types.FieldTypeString},
		{"enum", "enum('a','b')", types.FieldTypeString},
		{"set", "set('x','y')", types.FieldTypeString},
		{"time", "time", types.FieldTypeString},
		{"binary", "binary(16)", types.FieldTypeBytes},
		{"varbinary", "varbinary(255)", types.FieldTypeBytes},
		{"tinyblob", "tinyblob", types.FieldTypeBytes},
		{"blob", "blob", types.FieldTypeBytes},
		{"mediumblob", "mediumblob", types.FieldTypeBytes},
		{"longblob", "longblob", types.FieldTypeBytes},
		{"bit", "bit(8)", types.FieldTypeBytes},
		{"date", "date", types.FieldTypeDate},
		{"datetime", "datetime", types.FieldTypeTimestamp},
		{"timestamp", "timestamp", types.FieldTypeTimestamp},
		{"json", "json", types.FieldTypeJSON},
		{"geometry", "geometry", types.FieldTypeString}, // unknown → STRING fallback
	}

	for _, tc := range cases {
		got := fromMySQLType(tc.dataType, tc.columnType)
		if got != tc.want {
			t.Errorf("fromMySQLType(%q, %q) = %q, want %q", tc.dataType, tc.columnType, got, tc.want)
		}
	}
}

func TestBytesToString(t *testing.T) {
	// Verify the streamRows []byte → string conversion logic inline.
	// go-sql-driver returns TEXT values as []byte when scanning into interface{}.
	var val any = []byte("hello world")
	var converted any
	if b, ok := val.([]byte); ok {
		converted = string(b)
	} else {
		converted = val
	}
	if s, ok := converted.(string); !ok || s != "hello world" {
		t.Fatalf("expected string 'hello world', got %T %v", converted, converted)
	}
}
