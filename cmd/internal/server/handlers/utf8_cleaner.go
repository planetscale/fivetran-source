package handlers

import (
	"log"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// CleanStringValue ensures all string values are valid UTF-8.
// This is required because protobuf string fields must contain valid UTF-8
// per the proto3 specification: https://protobuf.dev/programming-guides/proto3/
//
// MySQL databases can contain invalid UTF-8 data from:
// - Latin-1 (ISO-8859-1) encoded columns
// - Truncated multi-byte UTF-8 sequences (e.g., from VARCHAR length limits)
// - Legacy data migrations
//
// This function attempts to preserve data fidelity by:
// 1. Passing through valid UTF-8 unchanged
// 2. Converting Latin-1 encoded data to UTF-8
// 3. Truncating incomplete UTF-8 sequences
// 4. Replacing unrecoverable bytes with the replacement character (�)
func CleanStringValue(s string) string {
	if utf8.ValidString(s) {
		return s
	}

	// Try to interpret as Latin-1 (ISO-8859-1)
	// This is common for Western European text in legacy MySQL databases
	decoder := charmap.ISO8859_1.NewDecoder()
	if result, _, err := transform.String(decoder, s); err == nil {
		if utf8.ValidString(result) {
			log.Printf("Converted non-UTF8 string from Latin-1: %q -> %q", s, result)
			return result
		}
	}

	// If Latin-1 didn't work, check if it's truncated UTF-8
	bytes := []byte(s)
	lastValidIdx := 0

	for i := 0; i < len(bytes); {
		r, size := utf8.DecodeRune(bytes[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid sequence found - truncate here
			log.Printf("Truncated invalid UTF-8 sequence at position %d: %q", i, s)
			return string(bytes[:lastValidIdx])
		}
		lastValidIdx = i + size
		i += size
	}

	// Last resort: replace with replacement character
	log.Printf("Could not recover string, using replacement character: %q", s)
	return strings.ToValidUTF8(s, "�")
}
