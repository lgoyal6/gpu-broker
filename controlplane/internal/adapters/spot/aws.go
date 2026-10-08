// Package spot translates provider interruption notices into an agent signal.
package spot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type Metadata interface {
	GetMetadata(context.Context, *imds.GetMetadataInput, ...func(*imds.Options)) (*imds.GetMetadataOutput, error)
}

type AWS struct{ Client Metadata }

func (p AWS) Interrupted(ctx context.Context, now time.Time) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("AWS IMDS client is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := p.Client.GetMetadata(ctx, &imds.GetMetadataInput{Path: "spot/instance-action"})
	if err != nil {
		var httpErr *smithyhttp.ResponseError
		if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == 404 {
			return false, nil
		}
		return false, fmt.Errorf("AWS interruption notice: %w", err)
	}
	defer resp.Content.Close()
	const limit = 4096
	b, err := io.ReadAll(io.LimitReader(resp.Content, limit+1))
	if err != nil {
		return false, err
	}
	if len(b) > limit {
		return false, fmt.Errorf("AWS interruption notice too large")
	}
	var notice struct {
		Action string    `json:"action"`
		Time   time.Time `json:"time"`
	}
	if err := json.Unmarshal(b, &notice); err != nil {
		return false, err
	}
	if notice.Action != "terminate" && notice.Action != "stop" && notice.Action != "hibernate" {
		return false, fmt.Errorf("unknown AWS interruption action")
	}
	// Do not treat an old cached response or an implausible distant event as
	// a current notice. Hibernation may have no advance warning.
	if notice.Time.IsZero() || notice.Time.Before(now.Add(-5*time.Minute)) || notice.Time.After(now.Add(5*time.Minute)) {
		return false, fmt.Errorf("AWS interruption notice outside current window")
	}
	return true, nil
}
