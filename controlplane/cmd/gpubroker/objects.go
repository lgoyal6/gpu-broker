package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/objectstore"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

type artifactStoreOptions struct {
	kind, directory, bucket, prefix, region, endpoint string
	allowHTTP                                         bool
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func (o *artifactStoreOptions) bind(flags *flag.FlagSet) {
	flags.StringVar(&o.kind, "object-store", envDefault("GPUB_OBJECT_STORE", "fs"), "artifact adapter: fs or s3")
	flags.StringVar(&o.directory, "object-dir", "/var/lib/gpubroker/objects", "filesystem artifact directory")
	flags.StringVar(&o.bucket, "s3-bucket", os.Getenv("GPUB_S3_BUCKET"), "existing private S3 bucket")
	flags.StringVar(&o.prefix, "s3-prefix", envDefault("GPUB_S3_PREFIX", "gpubroker"), "S3 object prefix")
	flags.StringVar(&o.region, "s3-region", envDefault("AWS_REGION", "us-east-1"), "S3 signing region")
	flags.StringVar(&o.endpoint, "s3-endpoint", os.Getenv("GPUB_S3_ENDPOINT"), "optional S3-compatible endpoint; uses path-style addressing")
	flags.BoolVar(&o.allowHTTP, "s3-allow-http", false, "allow plaintext endpoint for isolated local tests only")
}

func (o artifactStoreOptions) build(ctx context.Context) (application.ObjectStore, error) {
	switch o.kind {
	case "fs":
		if o.directory == "" {
			return nil, fmt.Errorf("%w: filesystem directory required", application.ErrInvalid)
		}
		return objectstore.NewFS(o.directory)
	case "s3":
		if o.bucket == "" || o.region == "" {
			return nil, fmt.Errorf("%w: S3 bucket and region required", application.ErrInvalid)
		}
		if o.endpoint != "" {
			u, err := url.Parse(o.endpoint)
			if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
				(u.Path != "" && u.Path != "/") || (u.Scheme != "https" && !(o.allowHTTP && u.Scheme == "http")) {
				return nil, fmt.Errorf("%w: S3 endpoint must be an HTTPS origin; plaintext requires --s3-allow-http", application.ErrInvalid)
			}
		}
		// Standard AWS credentials support workload roles and refreshing session
		// credentials. No credential material is accepted through public APIs.
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(o.region),
			config.WithRetryMaxAttempts(3), config.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))
		if err != nil {
			return nil, err
		}
		client := s3.NewFromConfig(cfg, func(options *s3.Options) {
			if o.endpoint != "" {
				options.BaseEndpoint = aws.String(o.endpoint)
				options.UsePathStyle = true
			}
		})
		return objectstore.NewS3(client, o.bucket, o.prefix)
	default:
		return nil, fmt.Errorf("%w: object-store must be fs or s3", application.ErrInvalid)
	}
}
