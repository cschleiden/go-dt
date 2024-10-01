package turso

import (
	"github.com/cschleiden/go-workflows/backend"
)

type options struct {
	backend.Options

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

// WithBackendOptions allows to pass generic backend options.
func WithBackendOptions(opts ...backend.BackendOption) option {
	return func(o *options) {
		for _, opt := range opts {
			opt(&o.Options)
		}
	}
}
