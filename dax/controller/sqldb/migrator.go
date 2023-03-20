package sqldb

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gobuffalo/pop/v6"
)

// EmbedMigrator is a migrator for SQL and Fizz files which are
// embedded in any fs.FS. This is lifted directly from pop's
// FileMigrator and tweaked slightly to take an fs.FS instead of a
// file path.
type EmbedMigrator struct {
	pop.Migrator
	FS  fs.FS
	log logger.Logger
}

// NewEmbedMigrator for a path and a Connection
func NewEmbedMigrator(fs fs.FS, c *pop.Connection, log logger.Logger) (*EmbedMigrator, error) {
	fm := &EmbedMigrator{
		Migrator: pop.NewMigrator(c),
		FS:       fs,
		log:      log,
	}
	fm.SchemaPath = ""

	runner := func(mf pop.Migration, tx *pop.Connection) error {
		f, err := fm.FS.Open(mf.Path)
		if err != nil {
			return err
		}
		defer f.Close()
		content, err := pop.MigrationContent(mf, tx, f, true)
		if err != nil {
			return fmt.Errorf("error processing %s: %w", mf.Path, err)
		}
		if content == "" {
			return nil
		}
		err = tx.RawQuery(content).Exec()
		if err != nil {
			return fmt.Errorf("error executing %s, sql: %s: %w", mf.Path, content, err)
		}
		return nil
	}

	err := fm.findMigrations(runner)
	if err != nil {
		return fm, err
	}

	return fm, nil
}

func (fm *EmbedMigrator) findMigrations(runner func(mf pop.Migration, tx *pop.Connection) error) error {
	return fs.WalkDir(fm.FS, "migrations", func(path string, d fs.DirEntry, err error) error {
		fmt.Printf("walking path: %s, d: %v, err: %v\n", path, d, err)
		if !d.IsDir() {
			match, err := pop.ParseMigrationFilename(d.Name())
			if err != nil {
				if strings.HasPrefix(err.Error(), "unsupported dialect") {
					fm.log.Warnf("ignoring migration file with %s", err.Error())
					return nil
				}
				return err
			}
			if match == nil {
				fm.log.Warnf("ignoring file %s because it does not match the migration file pattern", d.Name())
				return nil
			}
			mf := pop.Migration{
				Path:      path,
				Version:   match.Version,
				Name:      match.Name,
				DBType:    match.DBType,
				Direction: match.Direction,
				Type:      match.Type,
				Runner:    runner,
			}
			switch mf.Direction {
			case "up":
				fm.UpMigrations.Migrations = append(fm.UpMigrations.Migrations, mf)
			case "down":
				fm.DownMigrations.Migrations = append(fm.DownMigrations.Migrations, mf)
			default:
				// the regex only matches `(up|down)` for direction, so a panic here is appropriate
				panic("got unknown migration direction " + mf.Direction)
			}
		}
		return nil
	})
}
