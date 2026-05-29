package duckdb

import (
	"fmt"

	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/types"
)

// toDuckDBType maps an internal FieldType to a DuckDB SQL type string.
// Unknown types fall back to VARCHAR with a WARNING log — never panic.
func toDuckDBType(f types.Field) string {
	switch f.Type {
	case types.FieldTypeString:
		return "VARCHAR"
	case types.FieldTypeInt64:
		return "BIGINT"
	case types.FieldTypeFloat64:
		return "DOUBLE"
	case types.FieldTypeNumeric:
		return "DECIMAL"
	case types.FieldTypeBool:
		return "BOOLEAN"
	case types.FieldTypeTimestamp:
		return "TIMESTAMP"
	case types.FieldTypeDate:
		return "DATE"
	case types.FieldTypeJSON:
		return "JSON"
	case types.FieldTypeBytes:
		return "BLOB"
	case types.FieldTypeRepeated:
		inner := toDuckDBType(types.Field{Type: f.ItemType})
		return fmt.Sprintf("%s[]", inner)
	default:
		logger.L.Warnf("duckdb typemap: unknown FieldType %q for field %q — falling back to VARCHAR", f.Type, f.Name)
		return "VARCHAR"
	}
}

// fromDuckDBType maps a DuckDB column type string back to an internal FieldType.
// Used by GetSchema when reading information_schema.columns.
func fromDuckDBType(duckType string) types.FieldType {
	switch duckType {
	case "VARCHAR", "TEXT", "CHAR", "BPCHAR", "STRING":
		return types.FieldTypeString
	case "BIGINT", "INT8", "LONG", "HUGEINT", "INTEGER", "INT4", "INT", "INT2", "SMALLINT", "TINYINT", "INT1", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT":
		return types.FieldTypeInt64
	case "DOUBLE", "FLOAT8", "FLOAT", "FLOAT4", "REAL":
		return types.FieldTypeFloat64
	case "DECIMAL", "NUMERIC":
		return types.FieldTypeNumeric
	case "BOOLEAN", "BOOL", "LOGICAL":
		return types.FieldTypeBool
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ":
		return types.FieldTypeTimestamp
	case "DATE":
		return types.FieldTypeDate
	case "JSON":
		return types.FieldTypeJSON
	case "BLOB", "BINARY", "VARBINARY":
		return types.FieldTypeBytes
	default:
		logger.L.Warnf("duckdb typemap: unknown DuckDB type %q — falling back to STRING", duckType)
		return types.FieldTypeString
	}
}
