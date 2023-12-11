package graylog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hibiken/asynq"
)

var LogQueue = "graylog"

type RedisOptions struct {
	Addr     string
	Username string
	Password string
	DB       int
	// Workers  asynq maximum number of concurrent processing of tasks. default 100
	Workers int
}

type redisBackend struct {
	client *asynq.Client
	server *asynq.Server
}

func NewRedisBackend(opts RedisOptions) Backend {
	if opts.Workers <= 0 {
		opts.Workers = 100
	}
	redisClientOpt := asynq.RedisClientOpt{
		Addr:     opts.Addr,
		Username: opts.Username,
		Password: opts.Password,
		DB:       opts.DB,
	}
	client := asynq.NewClient(redisClientOpt)

	server := asynq.NewServer(redisClientOpt, asynq.Config{
		Concurrency: opts.Workers,
		Queues:      map[string]int{LogQueue: 10},
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
			fmt.Printf("Error: %v\n", err)
		}),
	})

	return &redisBackend{
		client: client,
		server: server,
	}
}

func (r *redisBackend) SendMessage(message *GELFMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// 压缩
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return err
	}

	if _, err = zw.Write(data); err != nil {
		return err
	}
	// ensure all data is written
	_ = zw.Close()

	for {
		if _, err := r.client.Enqueue(asynq.NewTask("gelf_message", buf.Bytes()), asynq.Queue(LogQueue)); err != nil {
			fmt.Printf("enqueue error: %v\n", err)
			time.Sleep(time.Second)
			continue
		}
		return nil
	}
}

func (r *redisBackend) Close() error {
	return r.client.Close()
}

func (r *redisBackend) LaunchConsume(f func(message *GELFMessage) error) error {
	mux := asynq.NewServeMux()
	mux.HandleFunc("gelf_message", func(ctx context.Context, task *asynq.Task) error {
		// 解压
		zr, err := gzip.NewReader(bytes.NewReader(task.Payload()))
		if err != nil {
			return err
		}
		data, err := io.ReadAll(zr)
		if err != nil {
			return err
		}

		var gelfMessage GELFMessage
		if err := json.Unmarshal(data, &gelfMessage); err != nil {
			return err
		}

		if err := f(&gelfMessage); err != nil {
			return err
		}
		return nil
	})

	return r.server.Run(mux)
}
