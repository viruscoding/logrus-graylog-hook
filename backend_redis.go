package graylog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hibiken/asynq"
)

var LogQueue = "graylog"

type RedisOptions struct {
	Addr     string
	Username string
	Password string
	DB       int
}

type redisBackend struct {
	client *asynq.Client
	server *asynq.Server
}

func NewRedisBackend(opts RedisOptions) (Backend, error) {
	redisClientOpt := asynq.RedisClientOpt{
		Addr:     opts.Addr,
		Username: opts.Username,
		Password: opts.Password,
		DB:       opts.DB,
	}
	client := asynq.NewClient(redisClientOpt)

	server := asynq.NewServer(redisClientOpt, asynq.Config{
		Concurrency: 100,
		Queues:      map[string]int{LogQueue: 10},
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
			fmt.Printf("Error: %v\n", err)
		}),
	})

	return &redisBackend{
		client: client,
		server: server,
	}, nil
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

	_, err = r.client.Enqueue(asynq.NewTask("gelf_message", buf.Bytes()), asynq.Queue(LogQueue))
	return err
}

func (r *redisBackend) Close() error {
	return r.client.Close()
}

func (r *redisBackend) LaunchConsumeSync(f func(message *GELFMessage) error) error {
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
