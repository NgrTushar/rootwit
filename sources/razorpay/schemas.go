package razorpay

import "github.com/rootwit/rootwit/types"

// supportedEntities lists the Razorpay entities RootWit can sync.
// Each name maps to a fixed schema and a Razorpay API path.
var supportedEntities = []string{
	"payments",
	"orders",
	"refunds",
	"customers",
}

// entityAPIPath maps entity names to their Razorpay v1 API paths.
var entityAPIPath = map[string]string{
	"payments":  "/v1/payments",
	"orders":    "/v1/orders",
	"refunds":   "/v1/refunds",
	"customers": "/v1/customers",
}

// entitySchemas holds the hardcoded schema for each supported entity.
// Razorpay's API shape is fixed — no schema introspection available.
var entitySchemas = map[string]types.Schema{
	"payments": {
		TableName: "payments",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeString},
			{Name: "entity", Type: types.FieldTypeString},
			{Name: "amount", Type: types.FieldTypeInt64},
			{Name: "currency", Type: types.FieldTypeString},
			{Name: "status", Type: types.FieldTypeString},
			{Name: "order_id", Type: types.FieldTypeString, Nullable: true},
			{Name: "invoice_id", Type: types.FieldTypeString, Nullable: true},
			{Name: "international", Type: types.FieldTypeBool, Nullable: true},
			{Name: "method", Type: types.FieldTypeString, Nullable: true},
			{Name: "amount_refunded", Type: types.FieldTypeInt64},
			{Name: "refund_status", Type: types.FieldTypeString, Nullable: true},
			{Name: "captured", Type: types.FieldTypeBool},
			{Name: "description", Type: types.FieldTypeString, Nullable: true},
			{Name: "card_id", Type: types.FieldTypeString, Nullable: true},
			{Name: "bank", Type: types.FieldTypeString, Nullable: true},
			{Name: "wallet", Type: types.FieldTypeString, Nullable: true},
			{Name: "vpa", Type: types.FieldTypeString, Nullable: true},
			{Name: "email", Type: types.FieldTypeString, Nullable: true},
			{Name: "contact", Type: types.FieldTypeString, Nullable: true},
			{Name: "fee", Type: types.FieldTypeInt64, Nullable: true},
			{Name: "tax", Type: types.FieldTypeInt64, Nullable: true},
			{Name: "error_code", Type: types.FieldTypeString, Nullable: true},
			{Name: "error_description", Type: types.FieldTypeString, Nullable: true},
			{Name: "created_at", Type: types.FieldTypeTimestamp},
		},
	},

	"orders": {
		TableName: "orders",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeString},
			{Name: "entity", Type: types.FieldTypeString},
			{Name: "amount", Type: types.FieldTypeInt64},
			{Name: "amount_paid", Type: types.FieldTypeInt64},
			{Name: "amount_due", Type: types.FieldTypeInt64},
			{Name: "currency", Type: types.FieldTypeString},
			{Name: "receipt", Type: types.FieldTypeString, Nullable: true},
			{Name: "offer_id", Type: types.FieldTypeString, Nullable: true},
			{Name: "status", Type: types.FieldTypeString},
			{Name: "attempts", Type: types.FieldTypeInt64},
			{Name: "notes", Type: types.FieldTypeJSON, Nullable: true},
			{Name: "created_at", Type: types.FieldTypeTimestamp},
		},
	},

	"refunds": {
		TableName: "refunds",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeString},
			{Name: "entity", Type: types.FieldTypeString},
			{Name: "amount", Type: types.FieldTypeInt64},
			{Name: "currency", Type: types.FieldTypeString},
			{Name: "payment_id", Type: types.FieldTypeString},
			{Name: "notes", Type: types.FieldTypeJSON, Nullable: true},
			{Name: "receipt", Type: types.FieldTypeString, Nullable: true},
			{Name: "acquirer_data", Type: types.FieldTypeJSON, Nullable: true},
			{Name: "created_at", Type: types.FieldTypeTimestamp},
			{Name: "batch_id", Type: types.FieldTypeString, Nullable: true},
			{Name: "status", Type: types.FieldTypeString},
			{Name: "speed_processed", Type: types.FieldTypeString, Nullable: true},
			{Name: "speed_requested", Type: types.FieldTypeString, Nullable: true},
		},
	},

	"customers": {
		TableName: "customers",
		Fields: []types.Field{
			{Name: "id", Type: types.FieldTypeString},
			{Name: "entity", Type: types.FieldTypeString},
			{Name: "name", Type: types.FieldTypeString, Nullable: true},
			{Name: "email", Type: types.FieldTypeString, Nullable: true},
			{Name: "contact", Type: types.FieldTypeString, Nullable: true},
			{Name: "gstin", Type: types.FieldTypeString, Nullable: true},
			{Name: "notes", Type: types.FieldTypeJSON, Nullable: true},
			{Name: "created_at", Type: types.FieldTypeTimestamp},
		},
	},
}
