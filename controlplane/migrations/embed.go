// Package migrations embeds the versioned SQL so the binary that runs a
// schema is the binary that was tested against it.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
