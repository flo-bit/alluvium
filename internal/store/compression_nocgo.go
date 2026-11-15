//go:build !cgo

package store

import "github.com/cockroachdb/pebble"

const blockCompression = pebble.ZstdCompression
