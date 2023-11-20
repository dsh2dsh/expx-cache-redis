package autopipe

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/caarlos0/env/v10"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
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

// --------------------------------------------------

func TestAutoPipe(t *testing.T) {
	tests := []struct {
		name   string
		keys   []string
		values [][]byte
		ttl    []time.Duration
		cfg    func(redisCache *AutoPipe)
	}{
		{
			name:   "1 key",
			keys:   []string{"key1"},
			values: [][]byte{[]byte("value1")},
			ttl:    []time.Duration{time.Minute},
		},
		{
			name:   "2 keys",
			keys:   []string{"key1", "key2"},
			values: [][]byte{[]byte("value1"), []byte("value2")},
			ttl:    []time.Duration{time.Minute, time.Minute},
		},
		{
			name:   "3 keys WithMaxWeight 2",
			keys:   []string{"key1", "key2", "key3"},
			values: [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")},
			ttl:    []time.Duration{time.Minute, time.Minute, time.Minute},
			cfg: func(redisCache *AutoPipe) {
				redisCache.WithMaxWeight(2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redisCache := testNew(t, tt.cfg)
			testAutoPipe(t, redisCache, tt.keys, tt.values, tt.ttl)
		})
	}
}

func testNew(t *testing.T, cacheOpts ...func(*AutoPipe)) *AutoPipe {
	rdb := valueNoError[*redis.Client](t)(NewRedisClient())
	if rdb == nil {
		t.Skipf("skip %q, because no Redis connection", t.Name())
	} else {
		require.NoError(t, rdb.FlushDB(context.Background()).Err())
		t.Cleanup(func() {
			require.NoError(t, rdb.FlushDB(context.Background()).Err())
		})
	}

	return testNewWithCmdable(t, rdb, cacheOpts...)
}

func valueNoError[T any](t *testing.T) func(val T, err error) T {
	return func(val T, err error) T {
		require.NoError(t, err)
		return val
	}
}

func testNewWithCmdable(t *testing.T, rdb redis.Cmdable,
	cacheOpts ...func(*AutoPipe),
) *AutoPipe {
	redisCache := New(rdb)
	for _, opt := range cacheOpts {
		if opt != nil {
			opt(redisCache)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	redisCache.Go(ctx)
	t.Cleanup(func() {
		cancel()
		redisCache.Wait()
	})

	return redisCache
}

func testAutoPipe(
	t *testing.T, redisCache *AutoPipe, keys []string, values [][]byte,
	ttl []time.Duration,
) {
	ctx := context.Background()

	require.NoError(t, redisCache.Set(MakeSetIter3(ctx, keys, values, ttl)))
	iterBytes, err := redisCache.Get(MakeGetIter3(ctx, keys))
	require.NoError(t, err)

	var bytes [][]byte
	for b, ok := iterBytes(); ok; b, ok = iterBytes() {
		bytes = append(bytes, b)
	}
	assert.Equal(t, values, bytes)

	require.NoError(t, redisCache.Del(ctx, keys))
}

func TestAutoPipe_keyNotFound(t *testing.T) {
	tests := []struct {
		name   string
		keys   []string
		values [][]byte
	}{
		{
			name:   "1 key",
			keys:   []string{"key1"},
			values: [][]byte{nil},
		},
		{
			name:   "2 keys",
			keys:   []string{"key1", "key2"},
			values: [][]byte{nil, nil},
		},
	}

	redisCache := testNew(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iterBytes, err := redisCache.Get(MakeGetIter3(context.Background(), tt.keys))
			require.NoError(t, err)
			var bytes [][]byte
			for b, ok := iterBytes(); ok; b, ok = iterBytes() {
				bytes = append(bytes, b)
			}
			assert.Equal(t, tt.values, bytes)
		})
	}
}

func TestAutoPipe_WithFlushInterval(t *testing.T) {
	redisCache := New(nil)
	flush := redisCache.flushInterval + time.Second
	assert.Same(t, redisCache, redisCache.WithFlushInterval(flush))
	assert.Equal(t, flush, redisCache.flushInterval)
}

func TestAutoPipe_WithGetRefreshTTL(t *testing.T) {
	redisCache := New(nil)
	ttl := redisCache.refreshTTL + time.Second
	assert.Same(t, redisCache, redisCache.WithGetRefreshTTL(ttl))
	assert.Equal(t, ttl, redisCache.refreshTTL)
}

func TestAutoPipe_WithMaxWeight(t *testing.T) {
	redisCache := New(nil)
	w := redisCache.maxWeight + 100
	assert.Same(t, redisCache, redisCache.WithMaxWeight(w))
	assert.Equal(t, w, redisCache.maxWeight)
}

func TestAutoPipe_handleErrors(t *testing.T) {
	ctx := context.Background()
	key := []string{"key1"}
	keys := []string{"key1", "key2"}
	wantErr := errors.New("expected error")

	var ctxCancel context.CancelFunc

	tests := []struct {
		name     string
		cfgCache func(redisCache *AutoPipe)
		expecter func(pipe *mocks.MockPipeliner)
		cmd      func(redisCache *AutoPipe)
	}{
		{
			name: "Del",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Del(ctx, mock.Anything).Return(
					redis.NewIntResult(0, wantErr))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewIntResult(0, wantErr)}, nil)
			},
			cmd: func(redisCache *AutoPipe) {
				require.ErrorIs(t, redisCache.Del(ctx, key), wantErr)
			},
		},
		{
			name: "Del Exec",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Del(ctx, mock.Anything).Return(redis.NewIntResult(0, nil))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewIntResult(0, wantErr)}, wantErr)
			},
			cmd: func(redisCache *AutoPipe) {
				require.ErrorIs(t, redisCache.Del(ctx, key), wantErr)
			},
		},
		{
			name: "Get 1 key",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Get(ctx, key[0]).Return(
					redis.NewStringResult("", wantErr))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewStringResult("", wantErr)}, nil)
			},
			cmd: func(redisCache *AutoPipe) {
				_, err := redisCache.Get(MakeGetIter3(ctx, key))
				require.ErrorIs(t, err, wantErr)
			},
		},
		{
			name: "Get 2 keys",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Get(ctx, mock.Anything).Return(
					redis.NewStringResult("", wantErr))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{
						redis.NewStringResult("", wantErr),
						redis.NewStringResult("", wantErr),
					}, wantErr)
			},
			cmd: func(redisCache *AutoPipe) {
				_, err := redisCache.Get(MakeGetIter3(ctx, keys))
				require.ErrorIs(t, err, wantErr)
			},
		},
		{
			name: "Get Exec",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Get(ctx, key[0]).Return(redis.NewStringResult("", nil))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewStringResult("", wantErr)}, wantErr)
			},
			cmd: func(redisCache *AutoPipe) {
				_, err := redisCache.Get(MakeGetIter3(ctx, key))
				require.ErrorIs(t, err, wantErr)
			},
		},
		{
			name: "Get unexpected type",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Get(ctx, key[0]).Return(redis.NewStringResult("", nil))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewBoolResult(false, wantErr)}, wantErr)
			},
			cmd: func(redisCache *AutoPipe) {
				_, err := redisCache.Get(MakeGetIter3(ctx, key))
				require.ErrorContains(t, err, "unexpected type")
			},
		},
		{
			name: "Get canceled 1",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Exec(mock.Anything).Return(nil, nil)
			},
			cmd: func(redisCache *AutoPipe) {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				_, err := redisCache.Get(MakeGetIter3(ctx, key))
				require.ErrorIs(t, err, context.Canceled)
			},
		},
		{
			name: "Get canceled 2",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Get(mock.Anything, key[0]).RunAndReturn(
					func(ctx context.Context, key string) *redis.StringCmd {
						ctxCancel()
						ctxCancel = nil
						return redis.NewStringResult("", nil)
					})
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewStringResult("", nil)}, nil)
			},
			cmd: func(redisCache *AutoPipe) {
				ctx, cancel := context.WithCancel(ctx)
				ctxCancel = cancel
				_, err := redisCache.Get(MakeGetIter3(ctx, key))
				require.ErrorIs(t, err, context.Canceled)
			},
		},
		{
			name: "GetEx",
			cfgCache: func(redisCache *AutoPipe) {
				redisCache.WithGetRefreshTTL(time.Minute)
			},
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().GetEx(ctx, key[0], time.Minute).Return(
					redis.NewStringResult("", wantErr))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewStringResult("", wantErr)}, nil)
			},
			cmd: func(redisCache *AutoPipe) {
				_, err := redisCache.Get(MakeGetIter3(ctx, key))
				require.ErrorIs(t, err, wantErr)
			},
		},
		{
			name: "Set",
			expecter: func(pipe *mocks.MockPipeliner) {
				pipe.EXPECT().Set(ctx, key[0], mock.Anything, mock.Anything).Return(
					redis.NewStatusResult("", wantErr))
				pipe.EXPECT().Exec(mock.Anything).Return(
					[]redis.Cmder{redis.NewStatusResult("", wantErr)}, nil)
			},
			cmd: func(redisCache *AutoPipe) {
				values := [][]byte{[]byte("value1")}
				ttl := []time.Duration{time.Minute}
				require.ErrorIs(t, redisCache.Set(MakeSetIter3(
					ctx, key, values, ttl)), wantErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe := mocks.NewMockPipeliner(t)
			tt.expecter(pipe)
			rdb := mocks.NewMockCmdable(t)
			rdb.EXPECT().Pipeline().Return(pipe)
			redisCache := testNewWithCmdable(t, rdb, tt.cfgCache)
			tt.cmd(redisCache)
		})
	}
}
