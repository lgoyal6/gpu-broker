package postgres_test

import "github.com/lgoyal6/gpu-broker/controlplane/internal/domain"

func domainWorker(id string) domain.WorkerID { return domain.WorkerID(id) }
