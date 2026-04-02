package systemtest

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

func (h *runtimeQueryHandler) PrepareQuery(_ context.Context, _ *pgwire.Session, query string, parameterTypeOIDs []uint32) (pgwire.PreparedQueryDescription, error) {
	prepared, err := h.planning.DescribePreparedQuery(query)
	if err != nil {
		return pgwire.PreparedQueryDescription{}, err
	}
	if len(parameterTypeOIDs) > 0 && len(parameterTypeOIDs) != len(prepared.ParameterTypeOIDs) {
		return pgwire.PreparedQueryDescription{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08P01",
			Message:  "prepared parameter type count does not match query placeholders",
		}
	}
	for i, oid := range parameterTypeOIDs {
		if oid == 0 {
			continue
		}
		if prepared.ParameterTypeOIDs[i] != 0 && prepared.ParameterTypeOIDs[i] != oid {
			return pgwire.PreparedQueryDescription{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "42P08",
				Message:  fmt.Sprintf("parameter $%d has incompatible type hint", i+1),
			}
		}
		prepared.ParameterTypeOIDs[i] = oid
	}
	return prepared, nil
}

func (h *runtimeQueryHandler) ExecutePreparedQuery(ctx context.Context, session *pgwire.Session, prepared pgwire.PreparedQueryDescription, params []pgwire.BoundParameter) (pgwire.QueryResult, error) {
	values, err := decodePreparedParameters(prepared.ParameterTypes, params)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	query, err := chronossql.RenderPreparedQuery(prepared.Query, values)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	return h.HandleSimpleQuery(ctx, session, query)
}

func decodePreparedParameters(types []chronossql.ColumnType, params []pgwire.BoundParameter) ([]chronossql.Value, error) {
	if len(types) != len(params) {
		return nil, pgwire.Error{
			Severity: "ERROR",
			Code:     "08P01",
			Message:  "bound parameter count does not match prepared statement",
		}
	}
	values := make([]chronossql.Value, 0, len(params))
	for i, param := range params {
		if param.IsNull {
			return nil, pgwire.Error{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  fmt.Sprintf("NULL prepared parameter $%d is unsupported", i+1),
			}
		}
		value, err := decodePreparedParameter(types[i], param)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func decodePreparedParameter(columnType chronossql.ColumnType, param pgwire.BoundParameter) (chronossql.Value, error) {
	switch param.FormatCode {
	case 0:
		return decodeTextPreparedParameter(columnType, param.Value)
	case 1:
		return decodeBinaryPreparedParameter(columnType, param.Value)
	default:
		return chronossql.Value{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "0A000",
			Message:  fmt.Sprintf("unsupported parameter format code %d", param.FormatCode),
		}
	}
}

func decodeTextPreparedParameter(columnType chronossql.ColumnType, payload []byte) (chronossql.Value, error) {
	switch columnType {
	case chronossql.ColumnTypeInt:
		value, err := strconv.ParseInt(string(payload), 10, 64)
		if err != nil {
			return chronossql.Value{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "22P02",
				Message:  fmt.Sprintf("invalid integer parameter: %v", err),
			}
		}
		return chronossql.Value{Type: chronossql.ColumnTypeInt, Int64: value}, nil
	case chronossql.ColumnTypeBytes:
		return chronossql.Value{Type: chronossql.ColumnTypeBytes, Bytes: append([]byte(nil), payload...)}, nil
	case chronossql.ColumnTypeString:
		fallthrough
	default:
		return chronossql.Value{Type: chronossql.ColumnTypeString, String: string(payload)}, nil
	}
}

func decodeBinaryPreparedParameter(columnType chronossql.ColumnType, payload []byte) (chronossql.Value, error) {
	switch columnType {
	case chronossql.ColumnTypeInt:
		var value int64
		switch len(payload) {
		case 2:
			value = int64(int16(binary.BigEndian.Uint16(payload)))
		case 4:
			value = int64(int32(binary.BigEndian.Uint32(payload)))
		case 8:
			value = int64(binary.BigEndian.Uint64(payload))
		default:
			return chronossql.Value{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "22P03",
				Message:  "invalid binary integer parameter width",
			}
		}
		return chronossql.Value{Type: chronossql.ColumnTypeInt, Int64: value}, nil
	case chronossql.ColumnTypeBytes:
		return chronossql.Value{Type: chronossql.ColumnTypeBytes, Bytes: append([]byte(nil), payload...)}, nil
	case chronossql.ColumnTypeString:
		fallthrough
	default:
		return chronossql.Value{Type: chronossql.ColumnTypeString, String: string(payload)}, nil
	}
}
