package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	bq "cloud.google.com/go/bigquery"
	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/types"
	"google.golang.org/api/option"
)

// BigQueryDestination implements destinations.DestinationConnector for BigQuery.
type BigQueryDestination struct {
	cfg       config.DestConfig
	client    *bq.Client
	datasetID string
}

// NewBigQueryDestination creates a new BigQueryDestination from the given config.
func NewBigQueryDestination(cfg config.DestConfig) *BigQueryDestination {
	return &BigQueryDestination{
		cfg:       cfg,
		datasetID: cfg.DatasetID,
	}
}

// Connect establishes a connection to BigQuery.
func (b *BigQueryDestination) Connect() error {
	ctx := context.Background()

	var opts []option.ClientOption
	if b.cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(b.cfg.CredentialsFile))
	}

	client, err := bq.NewClient(ctx, b.cfg.ProjectID, opts...)
	if err != nil {
		return fmt.Errorf("bigquery: failed to create client: %w", err)
	}

	b.client = client

	// Auto-create dataset if configured.
	if b.cfg.AutoCreateDataset {
		dataset := b.client.Dataset(b.datasetID)
		_, err := dataset.Metadata(ctx)
		if err != nil {
			// Dataset doesn't exist — create it.
			meta := &bq.DatasetMetadata{
				Location: b.cfg.Location,
			}
			if err := dataset.Create(ctx, meta); err != nil {
				return fmt.Errorf("bigquery: failed to create dataset %s: %w", b.datasetID, err)
			}
		}
	}

	return nil
}

// GetSchema returns the current schema of a table in BigQuery.
// Returns nil (not error) when the table does not yet exist.
func (b *BigQueryDestination) GetSchema(table string) (*types.Schema, error) {
	ctx := context.Background()
	tableRef := b.client.Dataset(b.datasetID).Table(table)

	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		// Check if the error is "table not found" — return nil, not error.
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("bigquery: failed to get schema for %s: %w", table, err)
	}

	schema := MetadataToSchema(table, meta.Schema)
	return &schema, nil
}

// CreateTable creates a new BigQuery table with the given schema.
func (b *BigQueryDestination) CreateTable(table string, schema types.Schema) error {
	ctx := context.Background()
	tableRef := b.client.Dataset(b.datasetID).Table(table)

	bqSchema := SchemaToMetadata(schema)
	meta := &bq.TableMetadata{
		Schema: bqSchema,
	}

	if err := tableRef.Create(ctx, meta); err != nil {
		return fmt.Errorf("bigquery: failed to create table %s: %w", table, err)
	}
	return nil
}

// AlterTable applies schema changes to an existing BigQuery table.
// Caller MUST check SchemaChanges.HasIncompatible before calling.
func (b *BigQueryDestination) AlterTable(table string, changes types.SchemaChanges) error {
	if changes.HasIncompatible {
		return fmt.Errorf("bigquery: cannot apply incompatible schema changes to %s", table)
	}

	ctx := context.Background()
	tableRef := b.client.Dataset(b.datasetID).Table(table)

	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("bigquery: failed to get metadata for %s: %w", table, err)
	}

	updatedSchema := meta.Schema

	for _, change := range changes.Changes {
		switch change.ChangeType {
		case types.SchemaChangeAddColumn:
			// Add new column as NULLABLE.
			fs := FieldToFieldSchema(*change.NewField)
			fs.Required = false // New columns in BigQuery must be NULLABLE.
			updatedSchema = append(updatedSchema, fs)

		case types.SchemaChangeRemoveColumn:
			// BigQuery doesn't support dropping columns directly via schema update.
			// Keep the column — it will be filled with NULLs for new rows.
			// This is the correct behavior per spec: "Removed col → keep + NULL."
			continue

		case types.SchemaChangeTypeWiden:
			// Update the field type to the wider type.
			for i, fs := range updatedSchema {
				if fs.Name == change.FieldName {
					updatedSchema[i].Type = MapFieldTypeToBigQuery(change.NewField.Type)
					break
				}
			}

		case types.SchemaChangeTypeIncompatible:
			// Should never reach here — caller must check HasIncompatible.
			return fmt.Errorf("bigquery: incompatible type change for column %s", change.FieldName)
		}
	}

	update := bq.TableMetadataToUpdate{
		Schema: updatedSchema,
	}
	if _, err := tableRef.Update(ctx, update, meta.ETag); err != nil {
		return fmt.Errorf("bigquery: failed to alter table %s: %w", table, err)
	}

	return nil
}

// WriteBatch writes a batch of rows to BigQuery using a load job (not streaming
// inserts). Load jobs are free within BigQuery's free tier. Streaming inserts
// cost $0.01 per 200MB.
func (b *BigQueryDestination) WriteBatch(table string, rows []types.Row) (types.WriteResult, error) {
	if len(rows) == 0 {
		return types.WriteResult{}, nil
	}

	ctx := context.Background()
	tableRef := b.client.Dataset(b.datasetID).Table(table)

	// Serialize rows as newline-delimited JSON for load job.
	var buf bytes.Buffer
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return types.WriteResult{}, fmt.Errorf("bigquery: failed to marshal row: %w", err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	// Create load job from the JSON data.
	source := bq.NewReaderSource(&buf)
	source.SourceFormat = bq.JSON
	source.AutoDetect = false

	loader := tableRef.LoaderFrom(source)
	loader.WriteDisposition = bq.WriteAppend

	job, err := loader.Run(ctx)
	if err != nil {
		return types.WriteResult{}, fmt.Errorf("bigquery: failed to start load job for %s: %w", table, err)
	}

	// Wait for the load job to complete.
	status, err := job.Wait(ctx)
	if err != nil {
		return types.WriteResult{}, fmt.Errorf("bigquery: load job failed for %s: %w", table, err)
	}
	if err := status.Err(); err != nil {
		return types.WriteResult{}, fmt.Errorf("bigquery: load job error for %s: %w", table, err)
	}

	return types.WriteResult{
		RowsWritten:  int64(len(rows)),
		BytesWritten: int64(buf.Len()),
	}, nil
}

// TruncateTable removes all rows from a BigQuery table.
// Uses a DELETE query since BigQuery doesn't have a native TRUNCATE.
func (b *BigQueryDestination) TruncateTable(table string) error {
	ctx := context.Background()

	query := fmt.Sprintf("DELETE FROM `%s.%s.%s` WHERE TRUE",
		b.cfg.ProjectID, b.datasetID, table)

	q := b.client.Query(query)
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("bigquery: failed to truncate %s: %w", table, err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("bigquery: truncate job failed for %s: %w", table, err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("bigquery: truncate error for %s: %w", table, err)
	}

	return nil
}

// SwapTables copies all rows from staging into dest (truncating dest first),
// then drops the staging table. Uses a BQ copy job with WriteTruncate so dest
// is never visible as empty — readers see old data until the copy completes,
// then immediately see the new data.
func (b *BigQueryDestination) SwapTables(staging, dest string) error {
	ctx := context.Background()
	ds := b.client.Dataset(b.datasetID)

	copier := ds.Table(dest).CopierFrom(ds.Table(staging))
	copier.WriteDisposition = bq.WriteTruncate

	job, err := copier.Run(ctx)
	if err != nil {
		return fmt.Errorf("bigquery: swap copy job failed (%s → %s): %w", staging, dest, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("bigquery: swap copy wait failed: %w", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("bigquery: swap copy error: %w", err)
	}

	if err := ds.Table(staging).Delete(ctx); err != nil {
		return fmt.Errorf("bigquery: failed to drop staging table %s: %w", staging, err)
	}

	return nil
}

// Close closes the BigQuery client.
func (b *BigQueryDestination) Close() error {
	if b.client != nil {
		return b.client.Close()
	}
	return nil
}

// isNotFoundError checks if a BigQuery error indicates a resource was not found.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	// The BigQuery client returns errors with "notFound" in the message.
	return containsString(err.Error(), "notFound") || containsString(err.Error(), "Not found")
}

func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
