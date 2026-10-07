package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const leaderLockKey = 7_400_201

// Leadership is a held scheduler leadership. The advisory lock lives on a
// dedicated connection taken out of the pool: if this process dies or that
// connection drops, Postgres releases the lock and another replica can take
// it. The epoch is the fencing token every reservation checks (ADR 0004).
type Leadership struct {
	conn  *pgx.Conn
	Epoch int64
}

// TryLead attempts to become the scheduler leader without blocking. It
// returns nil, nil when another process holds the lock.
func (s *Store) TryLead(ctx context.Context, holder string) (*Leadership, error) {
	pc, err := s.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn := pc.Hijack()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&got); err != nil {
		conn.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	if !got {
		conn.Close(context.WithoutCancel(ctx))
		return nil, nil
	}
	// The bump takes a row lock, so a deposed leader's in-flight reservation
	// (which holds FOR SHARE on this row) either commits first or sees the
	// new epoch and aborts.
	var epoch int64
	err = conn.QueryRow(ctx, `UPDATE scheduler_leader SET epoch = epoch + 1, holder = $1, acquired_at = $2
		WHERE id = 1 RETURNING epoch`, holder, time.Now().UTC()).Scan(&epoch)
	if err != nil {
		conn.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return &Leadership{conn: conn, Epoch: epoch}, nil
}

// Alive checks the lock's connection. An error means leadership may have
// been lost and the caller must stop scheduling immediately.
func (l *Leadership) Alive(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := l.conn.Exec(ctx, `SELECT 1`)
	return err
}

// Release gives up leadership. Closing the connection releases the lock even
// if the unlock statement fails.
func (l *Leadership) Release(ctx context.Context) {
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, leaderLockKey)
	_ = l.conn.Close(ctx)
}

// CurrentEpoch reads the epoch without taking leadership (for diagnostics).
func (s *Store) CurrentEpoch(ctx context.Context) (int64, string, error) {
	var e int64
	var h string
	err := s.Pool.QueryRow(ctx, `SELECT epoch, holder FROM scheduler_leader WHERE id = 1`).Scan(&e, &h)
	return e, h, err
}
