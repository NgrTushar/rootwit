package sqlite

import (
	"strings"

	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/types"
)

// fromSQLiteType maps a SQLite declared column type to an internal FieldType.
// SQLite's type system is flexible (type affinity), so we match on uppercase prefixes.
// Unknown types fall back to STRING with a WARNING log.
func fromSQLiteType(declaredType string) types.FieldType {
	t := strings.ToUpper(strings.TrimSpace(declaredType))

	switch {
	case t == "" || strings.Contains(t, "TEXT") || strings.Contains(t, "CHAR") || strings.Contains(t, "CLOB"):
		return types.FieldTypeString
	case strings.Contains(t, "INT"):
		return types.FieldTypeInt64
	case t == "REAL" || t == "FLOAT" || t == "DOUBLE" || strings.Contains(t, "FLOA") || strings.Contains(t, "DOUB"):
		return types.FieldTypeFloat64
	case strings.Contains(t, "NUMERIC") || strings.Contains(t, "DECIMAL") || t == "NUMBER":
		return types.FieldTypeNumeric
	case t == "BOOLEAN" || t == "BOOL":
		return types.FieldTypeBool
	case strings.Contains(t, "TIMESTAMP"):
		return types.FieldTypeTimestamp
	case t == "DATE":
		return types.FieldTypeDate
	case t == "JSON":
		return types.FieldTypeJSON
	case strings.Contains(t, "BLOB") || t == "BINARY" || t == "VARBINARY":
		return types.FieldTypeBytes
	default:
		logger.L.Warnf("sqlite typemap: unknown declared type %q — falling back to STRING", declaredType)
		return types.FieldTypeString
	}
}
