package sync

import (
	"github.com/rootwit/rootwit/types"
)

// Type widening rules: these transitions are safe and can be applied automatically.
// All other type changes are incompatible and must halt the sync.
var safeWidenings = map[types.FieldType]map[types.FieldType]bool{
	types.FieldTypeInt64: {
		types.FieldTypeFloat64: true,
		types.FieldTypeNumeric: true,
		types.FieldTypeString:  true,
	},
	types.FieldTypeFloat64: {
		types.FieldTypeNumeric: true,
		types.FieldTypeString:  true,
	},
	types.FieldTypeNumeric: {
		types.FieldTypeString: true,
	},
	types.FieldTypeBool: {
		types.FieldTypeString: true,
	},
	types.FieldTypeDate: {
		types.FieldTypeTimestamp: true,
		types.FieldTypeString:    true,
	},
	types.FieldTypeTimestamp: {
		types.FieldTypeString: true,
	},
}

// DiffSchema compares a source schema against a destination schema and returns
// all detected schema changes. If dest is nil, the table doesn't exist yet in
// the destination — this is not a schema change, it's handled by CreateTable.
//
// Detects all 4 change types:
//   - add_column:        field exists in source but not in destination
//   - remove_column:     field exists in destination but not in source
//   - type_widen:        field type changed to a wider compatible type
//   - type_incompatible: field type changed to an incompatible type
func DiffSchema(source types.Schema, dest *types.Schema) types.SchemaChanges {
	changes := types.SchemaChanges{
		TableName: source.TableName,
	}

	// If destination doesn't exist, no schema changes to detect.
	if dest == nil {
		return changes
	}

	// Build lookup maps for O(1) access.
	srcFields := make(map[string]types.Field)
	for _, f := range source.Fields {
		srcFields[f.Name] = f
	}

	destFields := make(map[string]types.Field)
	for _, f := range dest.Fields {
		destFields[f.Name] = f
	}

	// Check for added and changed columns (in source but not in dest, or type changed).
	for _, srcField := range source.Fields {
		destField, exists := destFields[srcField.Name]
		if !exists {
			// Column exists in source but not in destination → add_column.
			f := srcField
			changes.Changes = append(changes.Changes, types.SchemaChange{
				ChangeType: types.SchemaChangeAddColumn,
				FieldName:  srcField.Name,
				NewField:   &f,
			})
			continue
		}

		// Column exists in both — check if type changed.
		if srcField.Type != destField.Type {
			oldField := destField
			newField := srcField

			if isTypeWiden(destField.Type, srcField.Type) {
				changes.Changes = append(changes.Changes, types.SchemaChange{
					ChangeType: types.SchemaChangeTypeWiden,
					FieldName:  srcField.Name,
					OldField:   &oldField,
					NewField:   &newField,
				})
			} else {
				changes.Changes = append(changes.Changes, types.SchemaChange{
					ChangeType: types.SchemaChangeTypeIncompatible,
					FieldName:  srcField.Name,
					OldField:   &oldField,
					NewField:   &newField,
				})
				changes.HasIncompatible = true
			}
		}
	}

	// Check for removed columns (in destination but not in source).
	for _, destField := range dest.Fields {
		if _, exists := srcFields[destField.Name]; !exists {
			f := destField
			changes.Changes = append(changes.Changes, types.SchemaChange{
				ChangeType: types.SchemaChangeRemoveColumn,
				FieldName:  destField.Name,
				OldField:   &f,
			})
		}
	}

	return changes
}

// isTypeWiden returns true if changing from oldType to newType is a safe widening.
func isTypeWiden(oldType, newType types.FieldType) bool {
	if widened, ok := safeWidenings[oldType]; ok {
		return widened[newType]
	}
	return false
}
