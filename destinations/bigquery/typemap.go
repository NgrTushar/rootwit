package bigquery

import (
	bq "cloud.google.com/go/bigquery"
	"github.com/rootwit/rootwit/types"
)

// MapFieldTypeToBigQuery converts an internal FieldType to a BigQuery FieldType.
func MapFieldTypeToBigQuery(ft types.FieldType) bq.FieldType {
	switch ft {
	case types.FieldTypeString:
		return bq.StringFieldType
	case types.FieldTypeInt64:
		return bq.IntegerFieldType
	case types.FieldTypeFloat64:
		return bq.FloatFieldType
	case types.FieldTypeNumeric:
		return bq.NumericFieldType
	case types.FieldTypeBool:
		return bq.BooleanFieldType
	case types.FieldTypeTimestamp:
		return bq.TimestampFieldType
	case types.FieldTypeDate:
		return bq.DateFieldType
	case types.FieldTypeJSON:
		return bq.JSONFieldType
	case types.FieldTypeBytes:
		return bq.BytesFieldType
	default:
		// Fallback to STRING — never crash.
		return bq.StringFieldType
	}
}

// SchemaToMetadata converts an internal Schema to a BigQuery table schema.
func SchemaToMetadata(schema types.Schema) bq.Schema {
	var bqSchema bq.Schema
	for _, f := range schema.Fields {
		fs := FieldToFieldSchema(f)
		bqSchema = append(bqSchema, fs)
	}
	return bqSchema
}

// FieldToFieldSchema converts a single internal Field to a BigQuery FieldSchema.
func FieldToFieldSchema(f types.Field) *bq.FieldSchema {
	fs := &bq.FieldSchema{
		Name: f.Name,
	}

	if f.Type == types.FieldTypeRepeated {
		// Array type: use the inner type and set mode to REPEATED.
		fs.Type = MapFieldTypeToBigQuery(f.ItemType)
		fs.Repeated = true
	} else {
		fs.Type = MapFieldTypeToBigQuery(f.Type)
		if f.Nullable {
			fs.Required = false
		} else {
			fs.Required = true
		}
	}

	return fs
}

// MetadataToSchema converts BigQuery table metadata schema back to internal Schema.
func MetadataToSchema(tableName string, bqSchema bq.Schema) types.Schema {
	schema := types.Schema{TableName: tableName}
	for _, fs := range bqSchema {
		field := FieldSchemaToField(fs)
		schema.Fields = append(schema.Fields, field)
	}
	return schema
}

// FieldSchemaToField converts a BigQuery FieldSchema back to an internal Field.
func FieldSchemaToField(fs *bq.FieldSchema) types.Field {
	f := types.Field{
		Name:     fs.Name,
		Nullable: !fs.Required,
	}

	if fs.Repeated {
		f.Type = types.FieldTypeRepeated
		f.ItemType = mapBQFieldTypeToInternal(fs.Type)
	} else {
		f.Type = mapBQFieldTypeToInternal(fs.Type)
	}

	return f
}

// mapBQFieldTypeToInternal converts a BigQuery FieldType back to an internal FieldType.
func mapBQFieldTypeToInternal(bqType bq.FieldType) types.FieldType {
	switch bqType {
	case bq.StringFieldType:
		return types.FieldTypeString
	case bq.IntegerFieldType:
		return types.FieldTypeInt64
	case bq.FloatFieldType:
		return types.FieldTypeFloat64
	case bq.NumericFieldType:
		return types.FieldTypeNumeric
	case bq.BooleanFieldType:
		return types.FieldTypeBool
	case bq.TimestampFieldType:
		return types.FieldTypeTimestamp
	case bq.DateFieldType:
		return types.FieldTypeDate
	case bq.JSONFieldType:
		return types.FieldTypeJSON
	case bq.BytesFieldType:
		return types.FieldTypeBytes
	default:
		return types.FieldTypeString
	}
}
