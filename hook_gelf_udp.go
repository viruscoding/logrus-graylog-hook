package graylog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var bufMaxSize uint = 8192

type UdpHook struct {
	extra       map[string]interface{}
	host        string
	level       logrus.Level
	Backend     Backend
	synchronous bool
	buf         chan gelfEntry
	wg          *sync.WaitGroup
	mu          *sync.RWMutex
}

type gelfEntry struct {
	Level    logrus.Level
	Data     map[string]interface{}
	Message  string
	File     string
	Line     int
	Function string
}

func NewAsyncUdpHook(addr string, extra map[string]interface{}) (*UdpHook, error) {
	return newUdpHook(false, addr, extra)
}

func NewSyncUdpHook(addr string, extra map[string]interface{}) (*UdpHook, error) {
	return newUdpHook(true, addr, extra)
}

func newUdpHook(synchronous bool, addr string, extra map[string]interface{}) (*UdpHook, error) {
	backend, err := NewUdpBackend(addr)
	if err != nil {
		return nil, err
	}

	host, err := os.Hostname()
	if err != nil {
		host = "localhost"
	}
	var buf chan gelfEntry
	var wg *sync.WaitGroup
	var mu *sync.RWMutex
	if !synchronous {
		buf = make(chan gelfEntry, bufMaxSize)
		wg = &sync.WaitGroup{}
		mu = &sync.RWMutex{}
	}

	hook := &UdpHook{
		extra:       extra,
		host:        host,
		level:       logrus.DebugLevel,
		Backend:     backend,
		synchronous: synchronous,
		buf:         buf,
		wg:          wg,
		mu:          mu,
	}
	if !synchronous {
		go func() {
			for {
				entry := <-hook.buf
				if err := hook.sendEntry(entry); err != nil {
					fmt.Println(err)
				}
				hook.wg.Done()
			}
		}()
	}
	return hook, nil
}

func (u *UdpHook) Flush() {
	if !u.synchronous {
		u.mu.Lock() // claim the mutex as a Lock - we want exclusive access to it
		defer u.mu.Unlock()

		u.wg.Wait()
	}
}

func (u *UdpHook) Levels() []logrus.Level {
	var levels []logrus.Level
	for _, level := range logrus.AllLevels {
		if level <= u.level {
			levels = append(levels, level)
		}
	}
	return levels
}

func (u *UdpHook) Fire(entry *logrus.Entry) error {
	u.mu.RLock() // Claim the mutex as a RLock - allowing multiple go routines to log simultaneously
	defer u.mu.RUnlock()

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
	}

	if u.synchronous {
		if err := u.sendEntry(gEntry); err != nil {
			return err
		}
	} else {
		u.wg.Add(1)
		u.buf <- gEntry
	}

	return nil
}

func (u *UdpHook) sendEntry(entry gelfEntry) error {
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
		TimeUnix: float64(time.Now().UnixNano()/1000000) / 1000.,
		Level:    level,
		Extra:    extra,
	}
	return u.Backend.SendMessage(m)
}
