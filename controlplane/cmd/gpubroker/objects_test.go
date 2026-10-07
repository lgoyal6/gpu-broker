package main

import (
	"context"
	"errors"
	"flag"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/objectstore"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

func TestArtifactStoreOptions(t *testing.T) {
	t.Setenv("GPUB_OBJECT_STORE", "")
	var options artifactStoreOptions
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	options.bind(flags)
	if err := flags.Parse([]string{"--object-dir=" + t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	store, err := options.build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*objectstore.FS); !ok {
		t.Fatalf("default adapter: %T", store)
	}
}

func TestS3ConfigurationFailsClosed(t *testing.T) {
	for _, options := range []artifactStoreOptions{
		{kind: "typo"}, {kind: "s3", region: "us-east-1"},
		{kind: "s3", bucket: "private", region: "us-east-1", endpoint: "http://example.com"},
		{kind: "s3", bucket: "private", region: "us-east-1", endpoint: "https://user:secret@example.com"},
		{kind: "s3", bucket: "private", region: "us-east-1", endpoint: "https://example.com?secret=x"},
	} {
		if _, err := options.build(context.Background()); !errors.Is(err, application.ErrInvalid) {
			t.Fatalf("%+v: %v", options, err)
		}
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	if store, err := (artifactStoreOptions{kind: "s3", bucket: "private", region: "us-east-1", prefix: "gpub",
		endpoint: "http://127.0.0.1:9000", allowHTTP: true}).build(context.Background()); err != nil {
		t.Fatal(err)
	} else if _, ok := store.(*objectstore.S3); !ok {
		t.Fatalf("S3 adapter: %T", store)
	}
}
