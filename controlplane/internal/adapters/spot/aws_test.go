package spot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type metadataStub struct {
	body string
	err  error
}

func (s metadataStub) GetMetadata(context.Context, *imds.GetMetadataInput, ...func(*imds.Options)) (*imds.GetMetadataOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &imds.GetMetadataOutput{Content: io.NopCloser(strings.NewReader(s.body))}, nil
}

func TestAWSNoticeTreatsMissingMetadataAsNoInterruption(t *testing.T) {
	err := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}}, Err: fmt.Errorf("missing")}
	ok, got := (AWS{Client: metadataStub{err: err}}).Interrupted(context.Background(), time.Now())
	if got != nil || ok {
		t.Fatalf("404 should mean no notice, got interrupted=%v err=%v", ok, got)
	}
}

func TestAWSNoticeRejectsOversizedBody(t *testing.T) {
	body := `{"action":"terminate","time":"2026-10-08T09:09:00Z"}` + strings.Repeat("x", 4096)
	now := time.Date(2026, 10, 8, 9, 10, 0, 0, time.UTC)
	if ok, err := (AWS{Client: metadataStub{body: body}}).Interrupted(context.Background(), now); err == nil || ok {
		t.Fatalf("accepted oversized notice: %v %v", ok, err)
	}
}

func TestAWSNoticeActions(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 10, 0, 0, time.UTC)
	for _, action := range []string{"terminate", "stop", "hibernate"} {
		ok, err := (AWS{Client: metadataStub{body: `{"action":"` + action + `","time":"2026-10-08T09:09:00Z"}`}}).Interrupted(context.Background(), now)
		if err != nil || !ok {
			t.Fatalf("%s: %v %v", action, ok, err)
		}
	}
}

func TestAWSNoticeRejectsUnknownOrStale(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 10, 0, 0, time.UTC)
	for _, body := range []string{
		`{"action":"reboot","time":"2026-10-08T09:09:00Z"}`,
		`{"action":"terminate","time":"2026-10-08T08:00:00Z"}`,
		`{"action":"terminate","time":"2026-10-08T12:00:00Z"}`,
		`{`,
	} {
		if ok, err := (AWS{Client: metadataStub{body: body}}).Interrupted(context.Background(), now); err == nil || ok {
			t.Fatalf("accepted %s: %v %v", body, ok, err)
		}
	}
}

func TestAWSNoticeRequiresIMDSClient(t *testing.T) {
	if _, err := (AWS{}).Interrupted(context.Background(), time.Now()); err == nil {
		t.Fatal("nil client accepted")
	}
}
