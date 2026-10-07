package application

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// Principal is an authenticated caller. Tenant-owned reads use TenantID from
// here and never from the request.
type Principal struct {
	Kind     TokenKind
	TokenID  string
	UserID   domain.UserID
	TenantID domain.TenantID
	Role     domain.Role
	WorkerID domain.WorkerID
	PoolID   domain.PoolID
	Evidence domain.EvidenceClass
}

func (p Principal) Key() string { return string(p.Kind) + ":" + p.TokenID }

func (p Principal) Actor() domain.Actor {
	switch p.Kind {
	case TokenWorker:
		return domain.Actor{Kind: domain.ActorWorker, ID: string(p.WorkerID)}
	case TokenBootstrap:
		return domain.Actor{Kind: domain.ActorSystem, ID: "bootstrap:" + string(p.PoolID)}
	}
	if p.Role == domain.RoleOperator {
		return domain.Actor{Kind: domain.ActorOperator, ID: string(p.UserID)}
	}
	return domain.Actor{Kind: domain.ActorUser, ID: string(p.UserID)}
}

func hashSecret(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

// newToken returns "gpub_<kind>_<id>_<secret>" and the stored record.
func (s *Service) newToken(kind TokenKind) (string, Token) {
	id := strings.TrimPrefix(s.IDs.New("tok"), "tok_")
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	secret := base64.RawURLEncoding.EncodeToString(b[:])
	return fmt.Sprintf("gpub_%s_%s_%s", kind, id, secret), Token{ID: id, Kind: kind, SecretHash: hashSecret(secret)}
}

// Authenticate resolves a bearer token. Every failure is the same
// ErrUnauthorized so the response does not say which part was wrong.
func (s *Service) Authenticate(ctx context.Context, bearer string) (Principal, error) {
	parts := strings.SplitN(bearer, "_", 4)
	if len(parts) != 4 || parts[0] != "gpub" {
		return Principal{}, ErrUnauthorized
	}
	kind, id, secret := TokenKind(parts[1]), parts[2], parts[3]
	var p Principal
	err := s.Store.InTx(ctx, func(tx Tx) error {
		tok, err := tx.GetToken(ctx, id)
		if err != nil {
			return err
		}
		if tok.Revoked || tok.Kind != kind || !hmac.Equal(tok.SecretHash, hashSecret(secret)) {
			return ErrUnauthorized
		}
		p = Principal{Kind: tok.Kind, TokenID: tok.ID, WorkerID: tok.WorkerID, PoolID: tok.PoolID}
		if tok.Kind == TokenUser {
			u, err := tx.GetUser(ctx, tok.UserID)
			if err != nil {
				return err
			}
			t, err := tx.GetTenant(ctx, u.TenantID)
			if err != nil {
				return err
			}
			p.UserID, p.TenantID, p.Role, p.Evidence = u.ID, u.TenantID, u.Role, t.EvidenceClass
		}
		return nil
	})
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrUnauthorized) {
		return Principal{}, ErrUnauthorized
	}
	return p, err
}

// LeaseSigner MACs dispatches so a worker can refuse one that was altered in
// the queue or addressed to someone else (ADR 0007). The key is shared by the
// control plane and agents through a Secret.
type LeaseSigner struct{ key []byte }

func NewLeaseSigner(key []byte) (*LeaseSigner, error) {
	if len(key) < 32 {
		return nil, fmt.Errorf("dispatch key must be at least 32 bytes, got %d", len(key))
	}
	return &LeaseSigner{key: append([]byte(nil), key...)}, nil
}

func (l *LeaseSigner) Sign(attempt domain.AttemptID, worker domain.WorkerID, expires time.Time, specDigest string) string {
	m := hmac.New(sha256.New, l.key)
	fmt.Fprintf(m, "%s|%s|%d|%s", attempt, worker, expires.Unix(), specDigest)
	return hex.EncodeToString(m.Sum(nil))
}

func (l *LeaseSigner) Verify(attempt domain.AttemptID, worker domain.WorkerID, expires time.Time, specDigest, mac string) bool {
	want := l.Sign(attempt, worker, expires, specDigest)
	return hmac.Equal([]byte(want), []byte(mac))
}
