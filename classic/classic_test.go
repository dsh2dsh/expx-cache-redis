package classic

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

const testKey = "mykey"

func MustNew() *Classic {
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

func valueNoError[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

func bytesFromIter(iter func() ([]byte, bool)) []byte {
	b, _ := iter()
	return b
}

// --------------------------------------------------

func TestNew(t *testing.T) {
	redisCache := New(nil)
	require.IsType(t, new(Classic), redisCache)
	assert.Nil(t, redisCache.rdb)
	assert.Equal(t, defaultBatchSize, redisCache.batchSize)
}

func TestClassic_errors(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	ttls := []time.Duration{ttl, ttl, ttl}
	wantErr := errors.New("expected error")
	strResult := redis.NewStringResult("", nil)

	tests := []struct {
		name      string
		configure func(t *testing.T, rdb *mocks.MockCmdable)
		do        func(t *testing.T, redisCache *Classic) error
		assertErr func(t *testing.T, err error)
	}{
		{
			name: "Del",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Del(ctx, []string{testKey}).
					Return(redis.NewIntResult(0, wantErr))
			},
			do: func(t *testing.T, redisCache *Classic) error {
				return redisCache.Del(ctx, []string{testKey})
			},
		},
		{
			name: "Get error from getter 1",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Get(ctx, testKey).Return(redis.NewStringResult("", wantErr))
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey}))
				return err
			},
		},
		{
			name: "Get error from getter 2",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				pipe.EXPECT().Get(ctx, testKey).Return(
					redis.NewStringResult("", wantErr))
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(
					MakeGetIter3(ctx, []string{testKey, "key2", "key3"}))
				return err
			},
		},
		{
			name: "Get error from batchSize",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2", "key3"}
				var pipeLen int
				for _, expectedKey := range expectedKeys {
					pipe.EXPECT().Get(ctx, expectedKey).RunAndReturn(
						func(ctx context.Context, key string) *redis.StringCmd {
							pipeLen++
							return strResult
						})
				}
				pipe.EXPECT().Len().RunAndReturn(func() int { return pipeLen })
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, wantErr
					})
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(
					MakeGetIter3(ctx, []string{testKey, "key2", "key3"}))
				return err
			},
		},
		{
			name: "Get error from Exec",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
				for _, expectedKey := range expectedKeys {
					pipe.EXPECT().Get(ctx, expectedKey).Return(strResult)
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, wantErr
					})
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey, "key2"}))
				return err
			},
		},
		{
			name: "Get error from StringCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
				for _, expectedKey := range expectedKeys {
					pipe.EXPECT().Get(ctx, expectedKey).Return(strResult)
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewStringResult("", wantErr)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey, "key2"}))
				return err
			},
		},
		{
			name: "Get error from BoolCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
				for _, expectedKey := range expectedKeys {
					pipe.EXPECT().Get(ctx, expectedKey).Return(strResult)
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, wantErr)}
						return cmds, wantErr
					})
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey, "key2"}))
				return err
			},
		},
		{
			name: "Get unexpected type",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
				for _, expectedKey := range expectedKeys {
					pipe.EXPECT().Get(ctx, expectedKey).Return(strResult)
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, nil)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey, "key2"}))
				return err
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "unexpected type=")
			},
		},
		{
			name: "Get with Nil",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				pipe.EXPECT().Get(ctx, mock.Anything).Return(redis.NewStringResult("", nil))
				pipe.EXPECT().Len().Return(2)
				pipe.EXPECT().Exec(ctx).Return(
					[]redis.Cmder{
						redis.NewStringResult("", nil),
						redis.NewStringResult("", redis.Nil),
					},
					nil)
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisCache *Classic) error {
				_, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey, "key2"}))
				return err
			},
			assertErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "Set error from SET 1",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", wantErr))
			},
			do: func(t *testing.T, redisCache *Classic) error {
				err := redisCache.Set(
					MakeSetIter3(ctx, []string{testKey}, [][]byte{[]byte("abc")}, ttls))
				return err
			},
		},
		{
			name: "Set error from SET 2",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", nil))
				pipe.EXPECT().Len().Return(1)
				pipe.EXPECT().Set(ctx, "key2", mock.Anything, ttl).Return(
					redis.NewStatusResult("", wantErr))
			},
			do: func(t *testing.T, redisCache *Classic) error {
				err := redisCache.Set(
					MakeSetIter3(ctx, []string{testKey, "key2"},
						[][]byte{[]byte("abc"), []byte("abc")}, ttls))
				return err
			},
		},
		{
			name: "Set error from batchSize",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				wantKeys := []string{testKey, "key2", "key3"}
				var pipeLen int
				for _, wantKey := range wantKeys {
					pipe.EXPECT().Set(ctx, wantKey, mock.Anything, ttl).RunAndReturn(
						func(
							ctx context.Context, key string, v any, ttl time.Duration,
						) *redis.StatusCmd {
							pipeLen++
							return redis.NewStatusResult("", nil)
						})
				}
				pipe.EXPECT().Len().RunAndReturn(func() int { return pipeLen })
				pipe.EXPECT().Exec(ctx).Return(nil, wantErr)
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisCache *Classic) error {
				err := redisCache.Set(
					MakeSetIter3(ctx, []string{testKey, "key2", "key3"},
						[][]byte{[]byte("abc"), []byte("abc"), []byte("abc")}, ttls))
				return err
			},
		},
		{
			name: "Set error from Exec",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", nil))
				pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", nil))
				pipe.EXPECT().Len().Return(2)
				pipe.EXPECT().Exec(ctx).Return(nil, wantErr)
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisCache *Classic) error {
				err := redisCache.Set(
					MakeSetIter3(ctx, []string{testKey, testKey},
						[][]byte{[]byte("abc"), []byte("abc")}, ttls))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb := mocks.NewMockCmdable(t)
			redisCache := New(rdb).WithBatchSize(3)
			tt.configure(t, rdb)
			err := tt.do(t, redisCache)
			if tt.assertErr != nil {
				tt.assertErr(t, err)
			} else {
				require.ErrorIs(t, err, wantErr)
			}
		})
	}
}

func TestClassic_WithBatchSize(t *testing.T) {
	redisCache := New(nil)
	require.NotNil(t, redisCache)

	assert.Equal(t, defaultBatchSize, redisCache.batchSize)

	batchSize := redisCache.batchSize * 2
	assert.Same(t, redisCache, redisCache.WithBatchSize(batchSize))
	assert.Equal(t, batchSize, redisCache.batchSize)
}

func TestClassic_MGetSet_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	blobs := make([][]byte, len(keys))
	times := make([]time.Duration, len(keys))

	blob := []byte("foobar")
	ttl := time.Minute
	ctx := context.Background()

	tests := []struct {
		name     string
		singleOp func(rdb *mocks.MockCmdable)
		pipeOp   func(pipe *mocks.MockPipeliner, fn func(cmd redis.Cmder))
		cacheOp  func(t *testing.T, redisCache *Classic, nKeys int)
	}{
		{
			name: "Get",
			singleOp: func(rdb *mocks.MockCmdable) {
				rdb.EXPECT().Get(ctx, mock.Anything).Return(
					redis.NewStringResult("", nil))
			},
			pipeOp: func(pipe *mocks.MockPipeliner, fn func(cmd redis.Cmder)) {
				pipe.EXPECT().Get(ctx, mock.Anything).RunAndReturn(
					func(ctx context.Context, key string) *redis.StringCmd {
						cmd := redis.NewStringResult("", nil)
						fn(cmd)
						return cmd
					})
			},
			cacheOp: func(t *testing.T, redisCache *Classic, nKeys int) {
				bytesIter := valueNoError[func() ([]byte, bool)](t)(
					redisCache.Get(MakeGetIter3(ctx, keys[:nKeys])))
				for b, ok := bytesIter(); ok; b, ok = bytesIter() {
					assert.Nil(t, b)
				}
			},
		},
		{
			name: "Set",
			singleOp: func(rdb *mocks.MockCmdable) {
				rdb.EXPECT().Set(ctx, mock.Anything, blob, ttl).Return(
					redis.NewStatusResult("", nil))
			},
			pipeOp: func(pipe *mocks.MockPipeliner, fn func(cmd redis.Cmder)) {
				pipe.EXPECT().Set(ctx, mock.Anything, blob, ttl).RunAndReturn(
					func(
						ctx context.Context, key string, v any, ttl time.Duration,
					) *redis.StatusCmd {
						cmd := redis.NewStatusResult("", nil)
						fn(cmd)
						return cmd
					},
				)
			},
			cacheOp: func(t *testing.T, redisCache *Classic, nKeys int) {
				require.NoError(t, redisCache.Set(
					MakeSetIter3(ctx, keys[:nKeys], blobs[:nKeys], times[:nKeys])))
			},
		},
	}

	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("key-%00d", i)
		blobs[i] = blob
		times[i] = ttl
	}

	for _, tt := range tests {
		for nKeys := 0; nKeys <= len(keys); nKeys++ {
			t.Run(fmt.Sprintf("%s with %d keys", tt.name, nKeys), func(t *testing.T) {
				rdb := mocks.NewMockCmdable(t)
				redisCache := New(rdb).WithBatchSize(batchSize)
				require.NotNil(t, redisCache)

				if nKeys == 1 && tt.singleOp != nil {
					tt.singleOp(rdb)
					tt.cacheOp(t, redisCache, nKeys)
					return
				}

				var expect []int
				nExec := nKeys / redisCache.batchSize
				for i := 0; i < nExec; i++ {
					expect = append(expect, redisCache.batchSize)
				}
				expect = append(expect, nKeys%redisCache.batchSize)

				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)

				var cmds []redis.Cmder
				if nKeys > 0 {
					tt.pipeOp(pipe, func(cmd redis.Cmder) { cmds = append(cmds, cmd) })
					pipe.EXPECT().Len().RunAndReturn(func() int { return len(cmds) })
				}

				var got []int
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						got = append(got, len(cmds))
						gotCmds := cmds
						cmds = cmds[:0]
						return gotCmds, nil
					}).Times(len(expect))

				tt.cacheOp(t, redisCache, nKeys)
				assert.Equal(t, expect, got)
			})
		}
	}
}

func TestStdRedis_Del_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("key-%00d", i)
	}
	ctx := context.Background()

	for nKeys := 0; nKeys <= len(keys); nKeys++ {
		t.Run(fmt.Sprintf("with %d keys", nKeys), func(t *testing.T) {
			rdb := mocks.NewMockCmdable(t)
			redisCache := New(rdb)
			require.NotNil(t, redisCache)
			redisCache.WithBatchSize(batchSize)

			var wantKeys [][]string
			nDel := nKeys / redisCache.batchSize
			for i := 0; i < nDel; i++ {
				low := i * redisCache.batchSize
				high := low + redisCache.batchSize
				wantKeys = append(wantKeys, keys[low:high])
			}
			if nKeys%redisCache.batchSize > 0 {
				low := nDel * redisCache.batchSize
				high := low + nKeys%redisCache.batchSize
				wantKeys = append(wantKeys, keys[low:high])
			}

			var gotKeys [][]string
			for i := range wantKeys {
				rdb.EXPECT().Del(ctx, wantKeys[i]).RunAndReturn(
					func(ctx context.Context, keys ...string) *redis.IntCmd {
						gotKeys = append(gotKeys, keys)
						return redis.NewIntResult(int64(len(keys)), nil)
					})
			}

			require.NoError(t, redisCache.Del(ctx, keys[:nKeys]))
			assert.Equal(t, wantKeys, gotKeys)
		})
	}
}

func TestStdRedis_WithGetRefreshTTL(t *testing.T) {
	redisCache := New(nil)
	require.NotNil(t, redisCache)

	assert.Equal(t, time.Duration(0), redisCache.refreshTTL)

	ttl := time.Minute
	assert.Same(t, redisCache, redisCache.WithGetRefreshTTL(ttl))
	assert.Equal(t, ttl, redisCache.refreshTTL)
}

func TestStdRedis_respectRefreshTTL(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	strResult := redis.NewStringResult("", nil)

	tests := []struct {
		name   string
		expect func(redisCache *Classic, rdb *mocks.MockCmdable) ([]byte, error)
	}{
		{
			name: "Get without refreshTTL",
			expect: func(redisCache *Classic, rdb *mocks.MockCmdable) ([]byte, error) {
				rdb.EXPECT().Get(ctx, testKey).Return(strResult)
				bytesIter, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey}))
				return bytesFromIter(bytesIter), err
			},
		},
		{
			name: "Get with refreshTTL",
			expect: func(redisCache *Classic, rdb *mocks.MockCmdable) ([]byte, error) {
				redisCache.WithGetRefreshTTL(ttl)
				rdb.EXPECT().GetEx(ctx, testKey, ttl).Return(strResult)
				bytesIter, err := redisCache.Get(MakeGetIter3(ctx, []string{testKey}))
				return bytesFromIter(bytesIter), err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb := mocks.NewMockCmdable(t)
			redisCache := New(rdb)
			require.NotNil(t, redisCache)
			b := valueNoError[[]byte](t)(tt.expect(redisCache, rdb))
			assert.Nil(t, b)
		})
	}
}

func TestStdRedis_Set_skipEmptyItems(t *testing.T) {
	ctx := context.Background()
	foobar := []byte("foobar")
	ttl := time.Minute

	pipe := mocks.NewMockPipeliner(t)
	cmds := make([]redis.Cmder, 0, 1)
	pipe.EXPECT().Set(ctx, testKey, foobar, ttl).RunAndReturn(
		func(context.Context, string, any, time.Duration) *redis.StatusCmd {
			cmd := redis.NewStatusResult("", nil)
			cmds = append(cmds, cmd)
			return cmd
		})
	pipe.EXPECT().Len().Return(len(cmds))
	pipe.EXPECT().Exec(ctx).RunAndReturn(
		func(ctx context.Context) ([]redis.Cmder, error) {
			return cmds, nil
		})

	rdb := mocks.NewMockCmdable(t)
	rdb.EXPECT().Pipeline().Return(pipe)

	redisCache := New(rdb)
	require.NotNil(t, redisCache)
	require.NoError(t, redisCache.Set(
		MakeSetIter3(ctx,
			[]string{testKey, testKey, testKey, testKey},
			[][]byte{{}, foobar, foobar, foobar},
			[]time.Duration{ttl, 0, -1, ttl})))
}

// --------------------------------------------------

func BenchmarkClassic_Get(b *testing.B) {
	redisCache := MustNew()
	ctx := context.Background()

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
}

func BenchmarkClassic_Set(b *testing.B) {
	redisCache := MustNew()
	ctx := context.Background()

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
}
