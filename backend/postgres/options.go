package postgresbackend

import (
	"database/sql"

	"github.com/cschleiden/go-workflows/backend"
)

type options struct {
	backend.Options

	PostgreSQLOptions func(db *sql.DB)

	// ApplyMigrations automatically applies database migrations on startup.
	ApplyMigrations bool
}

type option func(*options)

// WithApplyMigrations automatically applies database migrations on startup.
func WithApplyMigrations(applyMigrations bool) option {
	return func(o *options) {
		o.ApplyMigrations = applyMigrations
	}
}

func WithPostgreSQLOptions(f func(db *sql.DB)) option {
	return func(o *options) {
		o.PostgreSQLOptions = f
	}
}

// WithBackendOptions allows to pass generic backend options.
func WithBackendOptions(opts ...backend.BackendOption) option {
	return func(o *options) {
		for _, opt := range opts {
			opt(&o.Options)
		}
	}
}
