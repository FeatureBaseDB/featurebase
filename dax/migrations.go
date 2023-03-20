package dax

import "embed"

// MigrationsFS will hold the contents of the migrations directory as
// a filesystem object embedded in the binary. Pretty neat!
//
//go:embed migrations/*
var MigrationsFS embed.FS
