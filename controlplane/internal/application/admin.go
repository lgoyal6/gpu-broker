package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// TenantSetup is what an operator creates in one step: a tenant, its first
// user, a default project with a budget, and a quota.
type TenantSetup struct {
	Name          string
	EvidenceClass domain.EvidenceClass
	Handle        string
	Role          domain.Role
	BudgetUSD     float64
	MaxQueuedJobs int
	MaxActiveGPUs int
}

type TenantCreated struct {
	TenantID  domain.TenantID  `json:"tenant_id"`
	UserID    domain.UserID    `json:"user_id"`
	ProjectID domain.ProjectID `json:"project_id"`
	Token     string           `json:"token"`
}

func (s *Service) CreateTenant(ctx context.Context, in TenantSetup) (TenantCreated, error) {
	if in.Name == "" || in.Handle == "" || !in.EvidenceClass.Valid() {
		return TenantCreated{}, fmt.Errorf("%w: tenant needs a name, a handle and a valid evidence class", ErrInvalid)
	}
	if in.Role == "" {
		in.Role = domain.RoleOperator
	}
	if in.MaxQueuedJobs <= 0 {
		in.MaxQueuedJobs = 200
	}
	if in.MaxActiveGPUs <= 0 {
		in.MaxActiveGPUs = 16
	}
	out := TenantCreated{
		TenantID:  domain.TenantID(s.IDs.New("ten")),
		UserID:    domain.UserID(s.IDs.New("usr")),
		ProjectID: domain.ProjectID(s.IDs.New("prj")),
	}
	err := s.Store.InTx(ctx, func(tx Tx) error {
		now := s.Clock.Now()
		if err := tx.InsertTenant(ctx, domain.Tenant{ID: out.TenantID, Name: in.Name, EvidenceClass: in.EvidenceClass, CreatedAt: now}); err != nil {
			return err
		}
		if err := tx.InsertUser(ctx, domain.User{ID: out.UserID, TenantID: out.TenantID, Handle: in.Handle, Role: in.Role}); err != nil {
			return err
		}
		if err := tx.InsertProject(ctx, domain.Project{ID: out.ProjectID, TenantID: out.TenantID, Name: "default",
			BudgetMicroUSD: domain.MicroUSD(in.BudgetUSD * 1e6)}); err != nil {
			return err
		}
		if err := tx.UpsertQuota(ctx, domain.Quota{TenantID: out.TenantID, MaxQueuedJobs: in.MaxQueuedJobs, MaxActiveGPUs: in.MaxActiveGPUs}); err != nil {
			return err
		}
		raw, tok := s.newToken(TokenUser)
		tok.UserID = out.UserID
		out.Token = raw
		if err := tx.InsertToken(ctx, tok); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, domain.AuditEvent{TenantID: out.TenantID, Actor: domain.Actor{Kind: domain.ActorSystem, ID: "admin"},
			Action: "tenant.create", Target: string(out.TenantID), At: now, CorrelationID: string(out.TenantID)})
	})
	return out, err
}

// AddUser creates a user in the caller's tenant. Operators only.
func (s *Service) AddUser(ctx context.Context, p Principal, handle string, role domain.Role) (domain.UserID, string, error) {
	if p.Kind != TokenUser || p.Role != domain.RoleOperator {
		return "", "", ErrForbidden
	}
	if role != domain.RoleMember && role != domain.RoleOperator {
		return "", "", fmt.Errorf("%w: role must be member or operator", ErrInvalid)
	}
	id := domain.UserID(s.IDs.New("usr"))
	var raw string
	err := s.Store.InTx(ctx, func(tx Tx) error {
		if err := tx.InsertUser(ctx, domain.User{ID: id, TenantID: p.TenantID, Handle: handle, Role: role}); err != nil {
			return err
		}
		r, tok := s.newToken(TokenUser)
		tok.UserID, raw = id, r
		if err := tx.InsertToken(ctx, tok); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, domain.AuditEvent{TenantID: p.TenantID, Actor: p.Actor(), Action: "user.create",
			Target: string(id), At: s.Clock.Now(), CorrelationID: string(id)})
	})
	return id, raw, err
}

// UpsertPool creates or updates a pool and returns a bootstrap token workers
// in that pool register with.
func (s *Service) UpsertPool(ctx context.Context, p domain.Pool) (string, error) {
	if p.ID == "" || p.Name == "" || p.Region == "" {
		return "", fmt.Errorf("%w: pool needs id, name and region", ErrInvalid)
	}
	if p.Kind != domain.PoolKubernetes && p.Kind != domain.PoolSimulated {
		return "", fmt.Errorf("%w: pool kind must be kubernetes or simulated", ErrInvalid)
	}
	var raw string
	err := s.Store.InTx(ctx, func(tx Tx) error {
		if err := tx.UpsertPool(ctx, p); err != nil {
			return err
		}
		r, tok := s.newToken(TokenBootstrap)
		tok.PoolID, raw = p.ID, r
		return tx.InsertToken(ctx, tok)
	})
	return raw, err
}

// UpsertPoolWithToken creates or updates a pool and registers a bootstrap
// token chosen by the operator (from a Kubernetes Secret), so the worker
// DaemonSet and the control plane can be installed in one step. The token
// must have the gpub_bootstrap_<id>_<secret> shape; only its hash is stored.
func (s *Service) UpsertPoolWithToken(ctx context.Context, p domain.Pool, raw string) error {
	parts := strings.SplitN(raw, "_", 4)
	if len(parts) != 4 || parts[0] != "gpub" || parts[1] != string(TokenBootstrap) || len(parts[3]) < 24 || len(parts[2]) < 4 {
		return fmt.Errorf("%w: bootstrap token must look like gpub_bootstrap_<id>_<secret of 24+ chars>", ErrInvalid)
	}
	if p.ID == "" || p.Name == "" || p.Region == "" {
		return fmt.Errorf("%w: pool needs id, name and region", ErrInvalid)
	}
	return s.Store.InTx(ctx, func(tx Tx) error {
		if err := tx.UpsertPool(ctx, p); err != nil {
			return err
		}
		existing, err := tx.GetToken(ctx, parts[2])
		switch {
		case err == nil:
			if existing.Kind != TokenBootstrap || existing.PoolID != p.ID {
				return fmt.Errorf("%w: token id %s belongs to another principal", ErrConflict, parts[2])
			}
			return tx.ReplaceTokenSecret(ctx, parts[2], hashSecret(parts[3]))
		case errors.Is(err, ErrNotFound):
			return tx.InsertToken(ctx, Token{ID: parts[2], Kind: TokenBootstrap, SecretHash: hashSecret(parts[3]), PoolID: p.ID})
		default:
			return err
		}
	})
}

// RegisterPolicies stores the builtin policies so every decision row can
// reference the exact spec that made it. A changed spec under an existing
// version fails here, at startup, rather than mislabelling decisions.
func (s *Service) RegisterPolicies(ctx context.Context) error {
	return s.Store.InTx(ctx, func(tx Tx) error {
		for _, name := range sortedKeys(s.Policies) {
			if err := tx.UpsertPolicy(ctx, s.Policies[name]); err != nil {
				return err
			}
		}
		return nil
	})
}
