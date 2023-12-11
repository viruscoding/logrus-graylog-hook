package graylog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type Hook struct {
	extra       map[string]interface{}
	host        string
	level       logrus.Level
	backend     Backend
	synchronous bool
	queue       *BlockingList
}

type gelfEntry struct {
	Level    logrus.Level
	Data     map[string]interface{}
	Message  string
	File     string
	Line     int
	Function string
	Time     time.Time
}

type HookOptions struct {
	Backend     Backend
	Extra       map[string]interface{}
	Synchronous bool
	// Concurrency is the number of goroutines to use when sending messages to the backend,default 100
	Concurrency int
}

func NewHook(opts HookOptions) *Hook {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 100
	}
	host, err := os.Hostname()
	if err != nil {
		host = "localhost"
	}
	var queue *BlockingList
	if !opts.Synchronous {
		queue = NewBlockingList()
	}

	hook := &Hook{
		extra:       opts.Extra,
		host:        host,
		level:       logrus.DebugLevel,
		backend:     opts.Backend,
		synchronous: opts.Synchronous,
		queue:       queue,
	}
	if !opts.Synchronous {
		for i := 0; i < opts.Concurrency; i++ {
			go func() {
				for {
					entry := hook.queue.FrontBlock()
					if err := hook.sendEntry(entry.(gelfEntry)); err != nil {
						fmt.Println(err)
					}
				}
			}()
		}
	}
	return hook
}

func (u *Hook) FlushAndClose() error {
	if !u.synchronous {
		for {
			if u.queue.Len() == 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}
	return u.backend.Close()
}

func (u *Hook) Levels() []logrus.Level {
	var levels []logrus.Level
	for _, level := range logrus.AllLevels {
		if level <= u.level {
			levels = append(levels, level)
		}
	}
	return levels
}

func (u *Hook) Fire(entry *logrus.Entry) error {
	var file, function string
	var line int

	if entry.Caller != nil {
		file = entry.Caller.File
		line = entry.Caller.Line
		function = entry.Caller.Function
	}

	newData := make(map[string]interface{})
	for k, v := range entry.Data {
		newData[k] = v
	}

	gEntry := gelfEntry{
		Level:    entry.Level,
		Data:     newData,
		Message:  entry.Message,
		File:     file,
		Line:     line,
		Function: function,
		Time:     time.Now(),
	}

	if u.synchronous {
		if err := u.sendEntry(gEntry); err != nil {
			return err
		}
	} else {
		u.queue.PushBack(gEntry)
	}

	return nil
}

func (u *Hook) sendEntry(entry gelfEntry) error {
	p := bytes.TrimSpace([]byte(entry.Message))

	// 多行则放到full字段，取第一行放到short字段
	short := p
	full := []byte("")
	if i := bytes.IndexRune(p, '\n'); i > 0 {
		short = p[:i]
		full = p
	}

	level := logrusLevelToSyslog(entry.Level)

	extra := map[string]interface{}{}
	for k, v := range u.extra {
		k = fmt.Sprintf("_%s", k)
		extra[k] = v
	}

	extra["_caller_file"] = entry.File
	extra["_caller_line"] = entry.Line
	extra["_caller_function"] = entry.Function

	for k, v := range entry.Data {
		extraK := fmt.Sprintf("_%s", k)
		if k == logrus.ErrorKey {
			asError, isError := v.(error)
			_, isMarshaler := v.(json.Marshaler)
			if isError && !isMarshaler {
				extra[extraK] = newMarshallableError(asError)
			} else {
				extra[extraK] = v
			}
			if stackTrace := extractStackTrace(asError); stackTrace != nil {
				extra[StackTraceKey] = fmt.Sprintf("%+v", stackTrace)
			}
		} else {
			extra[extraK] = v
		}
	}

	m := &GELFMessage{
		Version:  "1.1",
		Host:     u.host,
		Short:    string(short),
		Full:     string(full),
		TimeUnix: float64(entry.Time.UnixNano()/1000000) / 1000.,
		Level:    level,
		Extra:    extra,
	}
	return u.backend.SendMessage(m)
}
