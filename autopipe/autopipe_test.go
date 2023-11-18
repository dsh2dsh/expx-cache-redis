package autopipe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/caarlos0/env/v10"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/redis/go-redis/v9"
)

func MustNew() *AutoPipe {
	rdb, err := NewRedisClient()
	if err != nil {
		panic(err)
	} else if rdb == nil {
		panic("requires redis connection")
	}
	return New(rdb)
}

func NewRedisClient() (*redis.Client, error) {
	cfg := struct {
		WithRedis string `env:"WITH_REDIS"`
	}{
		WithRedis: "skip", // "redis://localhost:6379/1",
	}

	//nolint:wrapcheck
	err := dotenv.New().WithDepth(3).WithEnvSuffix("test").Load(func() error {
		return env.Parse(&cfg)
	})
	if err != nil {
		return nil, fmt.Errorf("load .env: %w", err)
	} else if cfg.WithRedis == "skip" {
		return nil, nil
	}

	opt, err := redis.ParseURL(cfg.WithRedis)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL %q: %w", cfg.WithRedis, err)
	}

	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %q: %w", cfg.WithRedis, err)
	}

	return rdb, nil
}

// --------------------------------------------------

func BenchmarkAutoPipe_Get(b *testing.B) {
	redisCache := MustNew()
	ctx, cancel := context.WithCancel(context.Background())
	redisCache.Go(ctx)

	allKeys := []string{"key1"}

	b.SetParallelism(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := redisCache.Get(MakeGetIter3(ctx, allKeys))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()

	cancel()
	redisCache.Wait()
}

func BenchmarkAutoPipe_Set(b *testing.B) {
	redisCache := MustNew()
	ctx, cancel := context.WithCancel(context.Background())
	redisCache.Go(ctx)

	allKeys := []string{"key1"}
	allValues := [][]byte{[]byte("value1")}
	allTimes := []time.Duration{time.Minute}

	b.SetParallelism(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := redisCache.Set(MakeSetIter3(ctx, allKeys, allValues, allTimes))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()

	cancel()
	redisCache.Wait()
}
