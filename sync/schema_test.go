package sync

import (
	"testing"

	"github.com/rootwit/rootwit/types"
)

func TestDiffSchema_AddColumn(t *testing.T) {
	src := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "name", Type: types.FieldTypeString},
			{Name: "email", Type: types.FieldTypeString}, // new column
		},
	}
	dst := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "name", Type: types.FieldTypeString},
		},
	}

	changes := DiffSchema(src, &dst)

	if len(changes.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes.Changes))
	}
	if changes.Changes[0].ChangeType != types.SchemaChangeAddColumn {
		t.Errorf("expected add_column, got %s", changes.Changes[0].ChangeType)
	}
	if changes.Changes[0].FieldName != "email" {
		t.Errorf("expected field 'email', got %s", changes.Changes[0].FieldName)
	}
	if changes.Changes[0].OldField != nil {
		t.Error("add_column should have nil OldField")
	}
	if changes.Changes[0].NewField == nil {
		t.Error("add_column should have non-nil NewField")
	}
	if changes.HasIncompatible {
		t.Error("add_column should not set HasIncompatible")
	}
}

func TestDiffSchema_RemoveColumn(t *testing.T) {
	src := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
		},
	}
	dst := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "old_col", Type: types.FieldTypeString}, // removed from source
		},
	}

	changes := DiffSchema(src, &dst)

	if len(changes.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes.Changes))
	}
	if changes.Changes[0].ChangeType != types.SchemaChangeRemoveColumn {
		t.Errorf("expected remove_column, got %s", changes.Changes[0].ChangeType)
	}
	if changes.Changes[0].FieldName != "old_col" {
		t.Errorf("expected field 'old_col', got %s", changes.Changes[0].FieldName)
	}
	if changes.Changes[0].NewField != nil {
		t.Error("remove_column should have nil NewField")
	}
	if changes.HasIncompatible {
		t.Error("remove_column should not set HasIncompatible")
	}
}

func TestDiffSchema_TypeWiden(t *testing.T) {
	src := types.Schema{
		TableName: "orders",
		Fields: []types.Field{
			{Name: "amount", Type: types.FieldTypeFloat64}, // widened from INT64
		},
	}
	dst := types.Schema{
		TableName: "orders",
		Fields: []types.Field{
			{Name: "amount", Type: types.FieldTypeInt64},
		},
	}

	changes := DiffSchema(src, &dst)

	if len(changes.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes.Changes))
	}
	if changes.Changes[0].ChangeType != types.SchemaChangeTypeWiden {
		t.Errorf("expected type_widen, got %s", changes.Changes[0].ChangeType)
	}
	if changes.HasIncompatible {
		t.Error("type_widen should not set HasIncompatible")
	}
}

func TestDiffSchema_TypeIncompatible(t *testing.T) {
	src := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "age", Type: types.FieldTypeBool}, // was INT64, now BOOL — incompatible
		},
	}
	dst := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "age", Type: types.FieldTypeInt64},
		},
	}

	changes := DiffSchema(src, &dst)

	if len(changes.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes.Changes))
	}
	if changes.Changes[0].ChangeType != types.SchemaChangeTypeIncompatible {
		t.Errorf("expected type_incompatible, got %s", changes.Changes[0].ChangeType)
	}
	if !changes.HasIncompatible {
		t.Error("HasIncompatible should be true for incompatible type change")
	}
}

func TestDiffSchema_HasIncompatible_OnlyWhenIncompatible(t *testing.T) {
	// Mix of add, widen, and remove — none incompatible.
	src := types.Schema{
		TableName: "t",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "amount", Type: types.FieldTypeFloat64}, // widen from INT64
			{Name: "new_col", Type: types.FieldTypeString}, // added
		},
	}
	dst := types.Schema{
		TableName: "t",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "amount", Type: types.FieldTypeInt64},
			{Name: "old_col", Type: types.FieldTypeString}, // removed
		},
	}

	changes := DiffSchema(src, &dst)

	if changes.HasIncompatible {
		t.Error("should not have HasIncompatible when only add/widen/remove changes")
	}
	if len(changes.Changes) != 3 {
		t.Errorf("expected 3 changes, got %d", len(changes.Changes))
	}
}

func TestDiffSchema_NilDest_NoChanges(t *testing.T) {
	src := types.Schema{
		TableName: "new_table",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
		},
	}

	changes := DiffSchema(src, nil)

	if len(changes.Changes) != 0 {
		t.Errorf("expected 0 changes for nil dest (new table), got %d", len(changes.Changes))
	}
}

func TestDiffSchema_NoChanges(t *testing.T) {
	schema := types.Schema{
		TableName: "users",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "name", Type: types.FieldTypeString},
		},
	}

	changes := DiffSchema(schema, &schema)

	if len(changes.Changes) != 0 {
		t.Errorf("expected 0 changes for identical schemas, got %d", len(changes.Changes))
	}
}

func TestDiffSchema_RemoveAndReAdd(t *testing.T) {
	// Column removed from source, different column added.
	src := types.Schema{
		TableName: "t",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "new_field", Type: types.FieldTypeString},
		},
	}
	dst := types.Schema{
		TableName: "t",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeInt64},
			{Name: "old_field", Type: types.FieldTypeString},
		},
	}

	changes := DiffSchema(src, &dst)

	if len(changes.Changes) != 2 {
		t.Fatalf("expected 2 changes (add + remove), got %d", len(changes.Changes))
	}

	hasAdd := false
	hasRemove := false
	for _, c := range changes.Changes {
		if c.ChangeType == types.SchemaChangeAddColumn && c.FieldName == "new_field" {
			hasAdd = true
		}
		if c.ChangeType == types.SchemaChangeRemoveColumn && c.FieldName == "old_field" {
			hasRemove = true
		}
	}

	if !hasAdd {
		t.Error("expected add_column for new_field")
	}
	if !hasRemove {
		t.Error("expected remove_column for old_field")
	}
}

func TestTypeWidening_AllSafeCases(t *testing.T) {
	safeCases := []struct {
		from types.FieldType
		to   types.FieldType
	}{
		{types.FieldTypeInt64, types.FieldTypeFloat64},
		{types.FieldTypeInt64, types.FieldTypeNumeric},
		{types.FieldTypeInt64, types.FieldTypeString},
		{types.FieldTypeFloat64, types.FieldTypeNumeric},
		{types.FieldTypeFloat64, types.FieldTypeString},
		{types.FieldTypeNumeric, types.FieldTypeString},
		{types.FieldTypeBool, types.FieldTypeString},
		{types.FieldTypeDate, types.FieldTypeTimestamp},
		{types.FieldTypeDate, types.FieldTypeString},
		{types.FieldTypeTimestamp, types.FieldTypeString},
	}

	for _, tc := range safeCases {
		t.Run(string(tc.from)+"→"+string(tc.to), func(t *testing.T) {
			if !isTypeWiden(tc.from, tc.to) {
				t.Errorf("%s → %s should be a safe widening", tc.from, tc.to)
			}
		})
	}
}

func TestTypeWidening_UnsafeCases(t *testing.T) {
	unsafeCases := []struct {
		from types.FieldType
		to   types.FieldType
	}{
		{types.FieldTypeString, types.FieldTypeInt64},   // narrowing
		{types.FieldTypeFloat64, types.FieldTypeInt64},  // narrowing
		{types.FieldTypeTimestamp, types.FieldTypeBool}, // incompatible
		{types.FieldTypeJSON, types.FieldTypeInt64},     // incompatible
		{types.FieldTypeBytes, types.FieldTypeFloat64},  // incompatible
	}

	for _, tc := range unsafeCases {
		t.Run(string(tc.from)+"→"+string(tc.to), func(t *testing.T) {
			if isTypeWiden(tc.from, tc.to) {
				t.Errorf("%s → %s should NOT be a safe widening", tc.from, tc.to)
			}
		})
	}
}
