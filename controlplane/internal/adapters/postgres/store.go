// Package postgres implements the application ports on PostgreSQL 16.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/migrations"
)

// Store is the transaction runner. RetryHook, when set, is called on every
// retried transaction; the metrics layer counts serialization conflicts with it.
type Store struct {
	Pool      *pgxpool.Pool
	MaxRetry  int
	RetryHook func(code string)
}

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: ping database: %v", application.ErrUnavailable, err)
	}
	return &Store{Pool: pool, MaxRetry: 8}, nil
}

func (s *Store) Close() { s.Pool.Close() }

// retryable are the SQLSTATEs where re-running the whole transaction is the
// correct response: serialization failure and deadlock. Anything else is a
// real error and is returned to the caller.
func retryable(err error) (string, bool) {
	var pg *pgconn.PgError
	if errors.As(err, &pg) && (pg.Code == "40001" || pg.Code == "40P01") {
		return pg.Code, true
	}
	return "", false
}

func (s *Store) InTx(ctx context.Context, fn func(tx application.Tx) error) error {
	var err error
	for attempt := 0; attempt <= s.MaxRetry; attempt++ {
		err = s.once(ctx, fn)
		code, ok := retryable(err)
		if !ok {
			return err
		}
		if s.RetryHook != nil {
			s.RetryHook(code)
		}
		// Short jittered backoff; contention here is between a handful of
		// processes, so a few milliseconds is enough to reorder them.
		d := time.Duration(1+attempt*attempt) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	return fmt.Errorf("transaction retries exhausted: %w", err)
}

func (s *Store) once(ctx context.Context, fn func(tx application.Tx) error) (err error) {
	ptx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin: %v", application.ErrUnavailable, err)
	}
	defer func() {
		if err != nil {
			_ = ptx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = fn(&tx{tx: ptx}); err != nil {
		return err
	}
	return ptx.Commit(ctx)
}

// Migrate applies embedded migrations in order under an advisory lock, so two
// processes starting at once cannot both apply 0003. A migration whose
// checksum differs from the recorded one is an error: editing an applied
// migration is how schemas silently diverge between environments.
func (s *Store) Migrate(ctx context.Context) ([]string, error) {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	const lockKey = 7_400_101
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return nil, err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockKey) //nolint:errcheck
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var applied []string
	for _, name := range names {
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])
		version := strings.TrimSuffix(name, ".sql")
		var have string
		err = conn.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, version).Scan(&have)
		switch {
		case err == nil:
			if have != checksum {
				return applied, fmt.Errorf("migration %s was edited after it was applied (checksum %s, recorded %s)", version, short(checksum), short(have))
			}
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return applied, err
		}
		t, err := conn.Begin(ctx)
		if err != nil {
			return applied, err
		}
		if _, err := t.Exec(ctx, string(body)); err != nil {
			_ = t.Rollback(ctx)
			return applied, fmt.Errorf("apply %s: %w", version, err)
		}
		if _, err := t.Exec(ctx, `INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`, version, checksum); err != nil {
			_ = t.Rollback(ctx)
			return applied, err
		}
		if err := t.Commit(ctx); err != nil {
			return applied, err
		}
		applied = append(applied, version)
	}
	return applied, nil
}

// SchemaReady reports whether every embedded migration has been applied. The
// API's readiness probe uses it so a pod never serves against an old schema.
func (s *Store) SchemaReady(ctx context.Context) error {
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
		return fmt.Errorf("%w: %v", application.ErrUnavailable, err)
	}
	if n < len(names) {
		return fmt.Errorf("schema has %d of %d migrations applied", n, len(names))
	}
	return nil
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
