package graylog

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const StackTraceKey = "_stacktrace"

type marshallableError struct {
	err error
}

func newMarshallableError(err error) *marshallableError {
	return &marshallableError{err}
}

// MarshalJSON 实现error类型序列化接口
func (m *marshallableError) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.err.Error())
}

type causer interface {
	Cause() error
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func extractStackTrace(err error) errors.StackTrace {
	var tracer stackTracer
	for {
		if st, ok := err.(stackTracer); ok {
			tracer = st
		}
		if cause, ok := err.(causer); ok {
			err = cause.Cause()
			continue
		}
		break
	}
	if tracer == nil {
		return nil
	}
	return tracer.StackTrace()
}

const (
	LogEmerg   = 0 /* system is unusable */
	LogAlert   = 1 /* action must be taken immediately */
	LogCrit    = 2 /* critical conditions */
	LogErr     = 3 /* error conditions */
	LogWarning = 4 /* warning conditions */
	LogNotice  = 5 /* normal but significant condition */
	LogInfo    = 6 /* informational */
	LogDebug   = 7 /* debug-level messages */
)

func logrusLevelToSyslog(level logrus.Level) int32 {
	// logrus has no equivalent of syslog LOG_NOTICE
	switch level {
	case logrus.PanicLevel:
		return LogAlert
	case logrus.FatalLevel:
		return LogCrit
	case logrus.ErrorLevel:
		return LogErr
	case logrus.WarnLevel:
		return LogWarning
	case logrus.InfoLevel:
		return LogInfo
	case logrus.DebugLevel, logrus.TraceLevel:
		return LogDebug
	default:
		return LogDebug
	}
}

// GELFMessage A GELF message is a JSON string with the following fields:
// https://go2docs.graylog.org/5-0/getting_in_log_data/gelf.html#GELFPayloadSpecification
type GELFMessage struct {
	Version  string  `json:"version"`
	Host     string  `json:"host"`
	Short    string  `json:"short_message"`
	Full     string  `json:"full_message"`
	TimeUnix float64 `json:"timestamp"`
	Level    int32   `json:"level"`
	// Facility @Deprecated send as additional field instead
	Facility string `json:"facility"`
	// Line @Deprecated send as additional field instead
	Line int `json:"line"`
	// File @Deprecated send as additional field instead
	File  string                 `json:"file"`
	Extra map[string]interface{} `json:"-"`
}

type innerMessage GELFMessage // against circular (Un)MarshalJSON

func (m *GELFMessage) MarshalJSON() ([]byte, error) {
	var err error
	var b, eb []byte

	extra := m.Extra
	b, err = json.Marshal((*innerMessage)(m))
	m.Extra = extra
	if err != nil {
		return nil, err
	}

	if len(extra) == 0 {
		return b, nil
	}

	if eb, err = json.Marshal(extra); err != nil {
		return nil, err
	}

	// merge serialized message + serialized extra map
	b[len(b)-1] = ','
	return append(b, eb[1:]...), nil
}

func (m *GELFMessage) UnmarshalJSON(data []byte) error {
	i := make(map[string]interface{}, 16)
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}
	for k, v := range i {
		if k[0] == '_' {
			if m.Extra == nil {
				m.Extra = make(map[string]interface{}, 1)
			}
			m.Extra[k] = v
			continue
		}
		switch k {
		case "version":
			m.Version = v.(string)
		case "host":
			m.Host = v.(string)
		case "short_message":
			m.Short = v.(string)
		case "full_message":
			m.Full = v.(string)
		case "timestamp":
			m.TimeUnix = v.(float64)
		case "level":
			m.Level = int32(v.(float64))
		case "facility":
			m.Facility = v.(string)
		case "file":
			m.File = v.(string)
		case "line":
			m.Line = int(v.(float64))
		}
	}
	return nil
}
