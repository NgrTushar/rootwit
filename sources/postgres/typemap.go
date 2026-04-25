package postgres

import (
	"github.com/rootwit/rootwit/types"
)

// Postgres OID constants for type mapping.
// See: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
const (
	OIDBool        uint32 = 16
	OIDBytea       uint32 = 17
	OIDInt8        uint32 = 20 // bigint
	OIDInt2        uint32 = 21 // smallint
	OIDInt4        uint32 = 23 // integer
	OIDText        uint32 = 25
	OIDJSON        uint32 = 114
	OIDFloat4      uint32 = 700  // real
	OIDFloat8      uint32 = 701  // double precision
	OIDBpchar      uint32 = 1042 // char(n)
	OIDVarchar     uint32 = 1043
	OIDDate        uint32 = 1082
	OIDTimestamp   uint32 = 1114
	OIDTimestampTZ uint32 = 1184
	OIDNumeric     uint32 = 1700
	OIDUUID        uint32 = 2950
	OIDJSONB       uint32 = 3802

	// Array type OIDs (Postgres uses separate OIDs for array types).
	OIDBoolArray        uint32 = 1000
	OIDByteaArray       uint32 = 1001
	OIDInt2Array        uint32 = 1005
	OIDInt4Array        uint32 = 1007
	OIDInt8Array        uint32 = 1016
	OIDTextArray        uint32 = 1009
	OIDVarcharArray     uint32 = 1015
	OIDFloat4Array      uint32 = 1021
	OIDFloat8Array      uint32 = 1022
	OIDDateArray        uint32 = 1182
	OIDTimestampArray   uint32 = 1115
	OIDTimestampTZArray uint32 = 1185
	OIDNumericArray     uint32 = 1231
	OIDUUIDArray        uint32 = 2951
	OIDJSONBArray       uint32 = 3807
	OIDJSONArray        uint32 = 199
)

// oidToFieldType maps Postgres OIDs to the internal FieldType.
var oidToFieldType = map[uint32]types.FieldType{
	OIDBool:        types.FieldTypeBool,
	OIDBytea:       types.FieldTypeBytes,
	OIDInt8:        types.FieldTypeInt64,
	OIDInt2:        types.FieldTypeInt64,
	OIDInt4:        types.FieldTypeInt64,
	OIDText:        types.FieldTypeString,
	OIDJSON:        types.FieldTypeJSON,
	OIDFloat4:      types.FieldTypeFloat64,
	OIDFloat8:      types.FieldTypeFloat64,
	OIDBpchar:      types.FieldTypeString,
	OIDVarchar:     types.FieldTypeString,
	OIDDate:        types.FieldTypeDate,
	OIDTimestamp:   types.FieldTypeTimestamp,
	OIDTimestampTZ: types.FieldTypeTimestamp,
	OIDNumeric:     types.FieldTypeNumeric,
	OIDUUID:        types.FieldTypeString, // BQ has no UUID type
	OIDJSONB:       types.FieldTypeJSON,
}

// arrayOIDToElementType maps Postgres array OIDs to their element FieldType.
var arrayOIDToElementType = map[uint32]types.FieldType{
	OIDBoolArray:        types.FieldTypeBool,
	OIDByteaArray:       types.FieldTypeBytes,
	OIDInt2Array:        types.FieldTypeInt64,
	OIDInt4Array:        types.FieldTypeInt64,
	OIDInt8Array:        types.FieldTypeInt64,
	OIDTextArray:        types.FieldTypeString,
	OIDVarcharArray:     types.FieldTypeString,
	OIDFloat4Array:      types.FieldTypeFloat64,
	OIDFloat8Array:      types.FieldTypeFloat64,
	OIDDateArray:        types.FieldTypeDate,
	OIDTimestampArray:   types.FieldTypeTimestamp,
	OIDTimestampTZArray: types.FieldTypeTimestamp,
	OIDNumericArray:     types.FieldTypeNumeric,
	OIDUUIDArray:        types.FieldTypeString,
	OIDJSONBArray:       types.FieldTypeJSON,
	OIDJSONArray:        types.FieldTypeJSON,
}

// MapOIDToFieldType converts a Postgres OID to the internal FieldType.
// Unknown OIDs map to FieldTypeString — NEVER panic on unknown type.
// The caller should log a WARNING for unknown types.
//
// Returns:
//   - fieldType: the mapped FieldType
//   - itemType: for array types, the element FieldType; empty string otherwise
//   - known: false if the OID was not recognized (mapped to STRING as fallback)
func MapOIDToFieldType(oid uint32) (fieldType types.FieldType, itemType types.FieldType, known bool) {
	// Check scalar types first.
	if ft, ok := oidToFieldType[oid]; ok {
		return ft, "", true
	}

	// Check array types.
	if elemType, ok := arrayOIDToElementType[oid]; ok {
		return types.FieldTypeRepeated, elemType, true
	}

	// Unknown type → STRING + warn. NEVER crash.
	return types.FieldTypeString, "", false
}

// MapColumnNameToFieldType is a fallback for when we have the Postgres type name
// (from information_schema.columns) but not the OID. Uses string matching.
func MapColumnNameToFieldType(pgType string) (fieldType types.FieldType, itemType types.FieldType, known bool) {
	typeMap := map[string]types.FieldType{
		"boolean":                     types.FieldTypeBool,
		"bool":                        types.FieldTypeBool,
		"smallint":                    types.FieldTypeInt64,
		"integer":                     types.FieldTypeInt64,
		"int":                         types.FieldTypeInt64,
		"bigint":                      types.FieldTypeInt64,
		"bigserial":                   types.FieldTypeInt64,
		"real":                        types.FieldTypeFloat64,
		"double precision":            types.FieldTypeFloat64,
		"numeric":                     types.FieldTypeNumeric,
		"decimal":                     types.FieldTypeNumeric,
		"text":                        types.FieldTypeString,
		"character varying":           types.FieldTypeString,
		"character":                   types.FieldTypeString,
		"varchar":                     types.FieldTypeString,
		"char":                        types.FieldTypeString,
		"uuid":                        types.FieldTypeString,
		"timestamp without time zone": types.FieldTypeTimestamp,
		"timestamp with time zone":    types.FieldTypeTimestamp,
		"date":                        types.FieldTypeDate,
		"json":                        types.FieldTypeJSON,
		"jsonb":                       types.FieldTypeJSON,
		"bytea":                       types.FieldTypeBytes,
	}

	if ft, ok := typeMap[pgType]; ok {
		return ft, "", true
	}

	// Check for ARRAY suffix.
	if len(pgType) > 2 && pgType[len(pgType)-2:] == "[]" {
		baseType := pgType[:len(pgType)-2]
		if ft, _, ok := MapColumnNameToFieldType(baseType); ok {
			return types.FieldTypeRepeated, ft, true
		}
	}

	// Check for ARRAY prefix (e.g., "ARRAY").
	// Unknown type → STRING + warn. NEVER crash.
	return types.FieldTypeString, "", false
}
