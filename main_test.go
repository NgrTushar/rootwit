package main

import (
	"testing"
)

// ==========================================================================
// redact() — always returns ***
// ==========================================================================

func TestRedact_AlwaysReturnsStars(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", "***"},
		{"a", "***"},
		{"ab", "***"},
		{"abc", "***"},
		{"localhost", "***"},
		{"analytics-prod-master.acme.internal", "***"},
		{"x", "***"},
	}

	for _, tt := range tests {
		got := redact(tt.input)
		if got != tt.want {
			t.Errorf("redact(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
