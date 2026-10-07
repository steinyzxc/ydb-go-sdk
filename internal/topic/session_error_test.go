package topic

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestClassifySessionError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode string
		errorType  string
	}{
		{
			name:       "transport",
			err:        grpcStatus.Error(grpcCodes.Unavailable, "connection lost"),
			statusCode: "Unavailable",
			errorType:  "transport_error",
		},
		{
			name:       "wrapped transport",
			err:        xerrors.WithStackTrace(grpcStatus.Error(grpcCodes.DeadlineExceeded, "timeout")),
			statusCode: "DeadlineExceeded",
			errorType:  "transport_error",
		},
		{
			name:       "unknown grpc status",
			err:        grpcStatus.Error(grpcCodes.Code(99), "future transport code"),
			statusCode: "Code(99)",
			errorType:  "transport_error",
		},
		{
			name:       "ydb",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			statusCode: "BAD_SESSION",
			errorType:  "ydb_error",
		},
		{
			name:       "unknown ydb status",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_StatusCode(499999))),
			statusCode: "499999",
			errorType:  "ydb_error",
		},
		{
			name:       "unspecified ydb status",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)),
			statusCode: "unknown",
			errorType:  "ydb_error",
		},
		{
			name:       "generic",
			err:        errors.New("unexpected failure"),
			statusCode: "unknown",
			errorType:  "unknown",
		},
		{
			name:       "nil",
			statusCode: "unknown",
			errorType:  "unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifySessionError(test.err)
			require.Equal(t, test.statusCode, got.StatusCode)
			require.Equal(t, test.errorType, got.ErrorType)
		})
	}
}
