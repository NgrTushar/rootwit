// Package razorpay implements a RootWit source connector for the Razorpay API.
// It syncs payments, orders, refunds, and customers into any RootWit destination.
// No external SDK — uses net/http with Basic auth (key_id:key_secret).
package razorpay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/types"
)

const (
	baseURL  = "https://api.razorpay.com"
	pageSize = 100 // Razorpay API maximum items per request
)

// RazorpaySource implements sources.SourceConnector for the Razorpay API.
type RazorpaySource struct {
	cfg    config.SourceConfig
	client *http.Client
}

// NewRazorpaySource creates a new RazorpaySource from the given config.
func NewRazorpaySource(cfg config.SourceConfig) *RazorpaySource {
	return &RazorpaySource{cfg: cfg}
}

// Connect verifies that the Razorpay credentials work by calling a lightweight endpoint.
func (r *RazorpaySource) Connect() error {
	r.client = &http.Client{Timeout: 30 * time.Second}

	// Verify credentials with a minimal request (fetch 1 payment).
	params := url.Values{"count": {"1"}}
	_, err := r.fetchPage(context.Background(), "/v1/payments", params)
	if err != nil {
		return fmt.Errorf("razorpay: connect failed (check key_id and key_secret): %w", err)
	}
	return nil
}

// GetTables returns the list of supported Razorpay entities.
func (r *RazorpaySource) GetTables() ([]string, error) {
	return supportedEntities, nil
}

// GetSchema returns the hardcoded schema for a Razorpay entity.
// Returns an error if the entity is not supported.
func (r *RazorpaySource) GetSchema(table string) (types.Schema, error) {
	schema, ok := entitySchemas[table]
	if !ok {
		return types.Schema{}, fmt.Errorf("razorpay: unsupported entity %q (supported: payments, orders, refunds, customers)", table)
	}
	return schema, nil
}

// ReadIncremental streams records created after cursorValue (a Unix timestamp stored as time.Time).
// cursorField is always "created_at" for all Razorpay entities.
func (r *RazorpaySource) ReadIncremental(ctx context.Context, table string, cursorField string, cursorValue any, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, pageSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		schema, ok := entitySchemas[table]
		if !ok {
			errCh <- fmt.Errorf("razorpay: unsupported entity %q", table)
			return
		}
		apiPath, ok := entityAPIPath[table]
		if !ok {
			errCh <- fmt.Errorf("razorpay: no API path for entity %q", table)
			return
		}

		fromUnix := toUnixTimestamp(cursorValue)
		// Add 1 second to avoid re-fetching the last synced record (cursor is inclusive).
		if fromUnix > 0 {
			fromUnix++
		}

		if err := r.streamPages(ctx, apiPath, fromUnix, 0, schema, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// ReadFull streams all records for the entity from the beginning of time.
func (r *RazorpaySource) ReadFull(ctx context.Context, table string, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, pageSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		schema, ok := entitySchemas[table]
		if !ok {
			errCh <- fmt.Errorf("razorpay: unsupported entity %q", table)
			return
		}
		apiPath, ok := entityAPIPath[table]
		if !ok {
			errCh <- fmt.Errorf("razorpay: no API path for entity %q", table)
			return
		}

		if err := r.streamPages(ctx, apiPath, 0, 0, schema, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// Close is a no-op for the HTTP-based Razorpay source.
func (r *RazorpaySource) Close() error {
	return nil
}

// streamPages fetches all pages of results from apiPath starting at fromUnix,
// sending each parsed row to rowCh.
func (r *RazorpaySource) streamPages(ctx context.Context, apiPath string, fromUnix, toUnix int64, schema types.Schema, rowCh chan<- types.Row) error {
	skip := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		params := url.Values{
			"count": {strconv.Itoa(pageSize)},
			"skip":  {strconv.Itoa(skip)},
		}
		if fromUnix > 0 {
			params.Set("from", strconv.FormatInt(fromUnix, 10))
		}
		if toUnix > 0 {
			params.Set("to", strconv.FormatInt(toUnix, 10))
		}

		items, err := r.fetchPage(ctx, apiPath, params)
		if err != nil {
			return err
		}

		for _, raw := range items {
			row := parseRow(raw, schema)
			select {
			case rowCh <- row:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if len(items) < pageSize {
			break // Last page — done.
		}
		skip += pageSize
	}
	return nil
}

// collectionResponse is the envelope Razorpay wraps list results in.
type collectionResponse struct {
	Count int                      `json:"count"`
	Items []map[string]interface{} `json:"items"`
}

// fetchPage calls the Razorpay API and returns the raw items slice.
// Retries once on 429 (rate limit) with a 1-second pause.
func (r *RazorpaySource) fetchPage(ctx context.Context, apiPath string, params url.Values) ([]map[string]interface{}, error) {
	u := baseURL + apiPath
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, fmt.Errorf("razorpay: build request: %w", err)
		}
		req.SetBasicAuth(r.cfg.Razorpay.KeyID, r.cfg.Razorpay.KeySecret)
		req.Header.Set("Accept", "application/json")

		resp, err := r.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("razorpay: HTTP request failed: %w", err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("razorpay: read body failed: %w", err)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			var col collectionResponse
			if err := json.Unmarshal(body, &col); err != nil {
				return nil, fmt.Errorf("razorpay: parse response failed: %w", err)
			}
			return col.Items, nil

		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, fmt.Errorf("razorpay: authentication failed (status %d) — check key_id and key_secret", resp.StatusCode)

		case http.StatusTooManyRequests:
			logger.L.Warnf("razorpay: rate limited (attempt %d/3), retrying...", attempt+1)
			lastErr = fmt.Errorf("razorpay: rate limited")
			continue

		default:
			return nil, fmt.Errorf("razorpay: unexpected status %d: %s", resp.StatusCode, string(body))
		}
	}
	return nil, lastErr
}

// parseRow converts a raw Razorpay API map to a types.Row, applying type coercions
// based on the entity's hardcoded schema.
func parseRow(raw map[string]interface{}, schema types.Schema) types.Row {
	row := make(types.Row, len(schema.Fields))
	for _, field := range schema.Fields {
		v := raw[field.Name]
		row[field.Name] = coerceField(field, v)
	}
	return row
}

// coerceField converts a raw JSON value to the appropriate Go type for a schema field.
func coerceField(field types.Field, v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch field.Type {
	case types.FieldTypeTimestamp:
		// Razorpay returns created_at as Unix seconds (JSON number → float64).
		switch val := v.(type) {
		case float64:
			return time.Unix(int64(val), 0).UTC()
		case int64:
			return time.Unix(val, 0).UTC()
		}
		return nil

	case types.FieldTypeInt64:
		switch val := v.(type) {
		case float64:
			return int64(val)
		case int64:
			return val
		}
		return v

	case types.FieldTypeBool:
		if b, ok := v.(bool); ok {
			return b
		}
		return v

	case types.FieldTypeJSON:
		// Serialize nested objects/arrays back to JSON string for the destination.
		if v == nil {
			return nil
		}
		b, err := json.Marshal(v)
		if err != nil {
			logger.L.Warnf("razorpay: coerceField JSON marshal failed for field %s: %v", field.Name, err)
			return fmt.Sprintf("%v", v)
		}
		return string(b)

	default:
		// STRING and others — Razorpay returns them as strings already.
		return v
	}
}

// toUnixTimestamp converts a cursorValue (from state.json) to a Unix timestamp.
// state.json stores time.Time as RFC3339 string via JSON; JSON numbers come as float64.
func toUnixTimestamp(v any) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case string:
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t.Unix()
		}
		if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return t.Unix()
		}
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n
		}
	case time.Time:
		return val.Unix()
	}
	return 0
}
