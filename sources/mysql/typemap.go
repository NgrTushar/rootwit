package mysql

import (
	"strings"

	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/types"
)

// fromMySQLType maps a MySQL DATA_TYPE + COLUMN_TYPE pair to an internal FieldType.
// dataType is the base type from information_schema (e.g. "int", "varchar").
// columnType is the full declaration (e.g. "tinyint(1)", "int unsigned") — used
// to detect the tinyint(1) → BOOL convention.
func fromMySQLType(dataType, columnType string) types.FieldType {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	ct := strings.ToLower(strings.TrimSpace(columnType))

	switch dt {
	case "TINYINT":
		// MySQL convention: TINYINT(1) is used as BOOLEAN by most ORMs.
		if strings.HasPrefix(ct, "tinyint(1)") {
			return types.FieldTypeBool
		}
		return types.FieldTypeInt64

	case "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
		"TINYINT UNSIGNED", "SMALLINT UNSIGNED", "MEDIUMINT UNSIGNED",
		"INT UNSIGNED", "BIGINT UNSIGNED", "YEAR":
		return types.FieldTypeInt64

	case "FLOAT":
		return types.FieldTypeFloat64

	case "DOUBLE", "REAL":
		return types.FieldTypeFloat64

	case "DECIMAL", "NUMERIC":
		return types.FieldTypeNumeric

	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"ENUM", "SET", "TIME":
		return types.FieldTypeString

	case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "BIT":
		return types.FieldTypeBytes

	case "DATE":
		return types.FieldTypeDate

	case "DATETIME", "TIMESTAMP":
		return types.FieldTypeTimestamp

	case "JSON":
		return types.FieldTypeJSON

	default:
		logger.L.Warnf("mysql typemap: unknown type %q — falling back to STRING", dataType)
		return types.FieldTypeString
	}
}
