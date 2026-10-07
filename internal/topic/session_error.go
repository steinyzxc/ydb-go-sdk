package topic

import (
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	unknownSessionErrorStatusCode = "unknown"
	transportSessionErrorType     = "transport_error"
	ydbSessionErrorType           = "ydb_error"
	unknownSessionErrorType       = "unknown"
)

// SessionErrorClassification contains labels derived from a topic session
// error. Reader and writer session traces use the same classification.
type SessionErrorClassification struct {
	StatusCode string
	ErrorType  string
}

// ClassifySessionError maps a transport or YDB operation error to the labels
// used by topic session error trace events.
func ClassifySessionError(err error) SessionErrorClassification {
	classification := SessionErrorClassification{
		StatusCode: unknownSessionErrorStatusCode,
		ErrorType:  unknownSessionErrorType,
	}
	if err == nil {
		return classification
	}

	if xerrors.IsTransportError(err) {
		classification.ErrorType = transportSessionErrorType
		classification.StatusCode = grpcStatus.Code(err).String()

		return classification
	}

	if operationErr := xerrors.OperationError(err); operationErr != nil {
		classification.ErrorType = ydbSessionErrorType
		classification.StatusCode = ydbStatusCodeName(operationErr.Code())

		return classification
	}

	return classification
}

func ydbStatusCodeName(code int32) string {
	name, ok := Ydb.StatusIds_StatusCode_name[code]
	if ok && name != "STATUS_CODE_UNSPECIFIED" {
		return name
	}
	if code == int32(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED) {
		return unknownSessionErrorStatusCode
	}

	return strconv.FormatInt(int64(code), 10)
}
