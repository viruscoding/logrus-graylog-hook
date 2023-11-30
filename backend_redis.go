package graylog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisOptions struct {
	Addr     string
	Username string
	Password string
	DB       int
}

type redisBackend struct {
	mu    *sync.Mutex
	redis *redis.Client
}

func NewRedisBackend(opts RedisOptions) (Backend, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Username: opts.Username,
		Password: opts.Password,
		DB:       opts.DB,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return &redisBackend{
		mu:    &sync.Mutex{},
		redis: client,
	}, nil
}

func (r *redisBackend) SendMessage(message *GELFMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

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

	// base64 encode
	b64Data := base64.RawStdEncoding.EncodeToString(buf.Bytes())

	return r.redis.RPush(context.Background(), "graylog", b64Data).Err()
}

func (r *redisBackend) Close() error {
	return r.redis.Close()
}

func (r *redisBackend) LaunchConsumeSync(f func(message *GELFMessage) error) error {
	for {
		// items[0] - the name of the key where an element was popped
		// items[1] - the value of the popped element
		items, err := r.redis.BLPop(context.Background(), 0, "graylog").Result()
		if err != nil || len(items) != 2 || items[1] == "" {
			time.Sleep(time.Second)
			continue
		}

		// base64 decode
		decodeBytes, err := base64.RawStdEncoding.DecodeString(items[1])
		if err != nil {
			return err
		}

		// 解压
		zr, err := gzip.NewReader(bytes.NewReader(decodeBytes))
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
			// 处理失败，将消息重新放回队列
			r.redis.RPush(context.Background(), "graylog", items[1])
			return err
		}
	}
}
