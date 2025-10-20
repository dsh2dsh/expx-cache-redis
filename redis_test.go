package redis

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"

	"github.com/caarlos0/env/v10"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

const (
	rdbOffset      = 0
	cacheNamespace = "expx-cache/redis-test:"
	keyLocked      = cacheNamespace + "lock:"
	testKey        = cacheNamespace + "test-key"
)

func MustNew() (*RedisCache, *redis.Client) {
	rdb, err := NewRedisClient()
	if err != nil {
		panic(err)
	} else if rdb == nil {
		panic("requires redis connection")
	}
	return New(rdb), rdb
}

func NewRedisClient() (*redis.Client, error) {
	cfg := struct {
		WithRedis string `env:"WITH_REDIS"`
	}{
		WithRedis: "skip", // "redis://localhost:6379/1",
	}

	err := dotenv.Load(func() error { return env.Parse(&cfg) })
	if err != nil {
		return nil, fmt.Errorf("load .env: %w", err)
	} else if cfg.WithRedis == "skip" {
		return nil, nil
	}

	opt, err := redis.ParseURL(cfg.WithRedis)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL %q: %w", cfg.WithRedis, err)
	}
	opt.DB += rdbOffset

	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %q: %w", cfg.WithRedis, err)
	}

	return rdb, nil
}

// --------------------------------------------------

func TestRedisCacheSuite(t *testing.T) {
	rdb := valueNoError[*redis.Client](t)(NewRedisClient())
	if rdb == nil {
		t.Skipf("skip %q, because no Redis connection", t.Name())
	} else {
		ctx := t.Context()
		require.NoError(t, rdb.FlushDB(ctx).Err())
		t.Cleanup(func() { require.NoError(t, rdb.Close()) })
	}
	suite.Run(t, &RedisCacheTestSuite{rdb: rdb})
}

func valueNoError[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

type RedisCacheTestSuite struct {
	suite.Suite

	rdb Cmdable
}

func (self *RedisCacheTestSuite) TearDownTest() {
	self.Require().NoError(self.rdb.FlushDB(context.Background()).Err())
}

func (self *RedisCacheTestSuite) TestRedisCache() {
	tests := []struct {
		name   string
		keys   []string
		values [][]byte
		ttl    []time.Duration
		cfg    func(redisCache *RedisCache)
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
			name:   "3 keys WithBatchSize 2",
			keys:   []string{"key1", "key2", "key3"},
			values: [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")},
			ttl:    []time.Duration{time.Minute, time.Minute, time.Minute},
			cfg: func(redisCache *RedisCache) {
				redisCache.WithBatchSize(2)
			},
		},
	}

	for _, tt := range tests {
		self.Run(tt.name, func() {
			redisCache := self.testNew(tt.cfg)
			self.testRedisCache(redisCache, tt.keys, tt.values, tt.ttl)
		})
	}
}

func (self *RedisCacheTestSuite) testNew(cacheOpts ...func(*RedisCache)) *RedisCache {
	redisCache := New(self.rdb)
	for _, opt := range cacheOpts {
		if opt != nil {
			opt(redisCache)
		}
	}
	return redisCache
}

func (self *RedisCacheTestSuite) testRedisCache(redisCache *RedisCache,
	keys []string, values [][]byte, ttl []time.Duration,
) {
	ctx := context.Background()

	self.Require().NoError(redisCache.Set(itemsSeq(ctx, keys, values, ttl)))
	iterBytes := redisCache.Get(ctx, len(keys), slices.Values(keys))

	bytes := make([][]byte, 0, len(keys))
	for b, err := range iterBytes {
		self.Require().NoError(err)
		bytes = append(bytes, b)
	}
	self.Equal(values, bytes)

	self.Require().NoError(redisCache.Del(ctx, keys))
}

func itemsSeq(
	ctx context.Context, keys []string, blobs [][]byte, times []time.Duration,
) (context.Context, int, iter.Seq[Item]) {
	return ctx, len(keys), func(yield func(Item) bool) {
		for i := range keys {
			if !yield(NewItem(keys[i], blobs[i], times[i])) {
				return
			}
		}
	}
}

func (self *RedisCacheTestSuite) TestLockGet() {
	const bar = "bar"
	keySet := self.resolveKeyLock("tests-key")
	self.T().Logf("keySet=%q", keySet)
	ctx := context.Background()
	ttl := time.Minute
	foobar := []byte("foobar")

	r := self.testNew()
	ok, b, err := r.LockGet(ctx, keySet, "foo", ttl, testKey)
	self.Require().NoError(err)
	self.True(ok)
	self.Empty(b)
	self.Equal("foo", string(self.getKey(r, keySet)))

	ok, b, err = r.LockGet(ctx, keySet, bar, ttl, testKey)
	self.Require().NoError(err)
	self.False(ok)
	self.Empty(b)
	self.Equal("foo", string(self.getKey(r, keySet)))

	self.setKey(r, testKey, foobar, ttl)
	ok, b, err = r.LockGet(ctx, keySet, bar, ttl, testKey)
	self.Require().NoError(err)
	self.False(ok)
	self.Equal(foobar, b)

	self.Require().NoError(r.Del(ctx, []string{keySet}))
	ok, b, err = r.LockGet(ctx, keySet, bar, ttl, testKey)
	self.Require().NoError(err)
	self.True(ok)
	self.Equal(foobar, b)
	self.Equal(bar, string(self.getKey(r, keySet)))
}

func (self *RedisCacheTestSuite) resolveKeyLock(key string) string {
	return keyLocked + key
}

func (self *RedisCacheTestSuite) getKey(r *RedisCache, key string) []byte {
	iterBytes := r.Get(context.Background(), 1, slices.Values([]string{key}))
	for b, err := range iterBytes {
		self.Require().NoError(err)
		return b
	}
	self.Require().Fail("Get returned empty iterator")
	return nil
}

func (self *RedisCacheTestSuite) setKey(r *RedisCache, key string, b []byte,
	ttl time.Duration,
) {
	ctx := context.Background()
	self.Require().NoError(r.Set(ctx, 1, slices.Values([]Item{
		NewItem(key, b, ttl),
	})))
}

func TestRedisCache_errors(t *testing.T) {
	ctx := t.Context()
	ttl := time.Minute
	ttls := []time.Duration{ttl, ttl, ttl}
	wantErr := errors.New("expected error")
	strResult := redis.NewStringResult("", nil)

	tests := []struct {
		name      string
		configure func(t *testing.T, rdb *mocks.MockCmdable)
		do        func(t *testing.T, redisCache *RedisCache) error
		assertErr func(t *testing.T, err error)
	}{
		{
			name: "Del",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Del(ctx, []string{testKey}).
					Return(redis.NewIntResult(0, wantErr))
			},
			do: func(t *testing.T, redisCache *RedisCache) error {
				return redisCache.Del(ctx, []string{testKey})
			},
		},
		{
			name: "Get error from getter 1",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Get(ctx, testKey).Return(redis.NewStringResult("", wantErr))
			},
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(
					ctx, 1, slices.Values([]string{testKey})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 3, slices.Values(
					[]string{testKey, "key2", "key3"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 3, slices.Values(
					[]string{testKey, "key2", "key3"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				return iterBytesErr(redisCache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				err := redisCache.Set(ctx, 1, slices.Values([]Item{
					NewItem(testKey, []byte("abc"), ttl),
				}))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				err := redisCache.Set(itemsSeq(ctx,
					[]string{testKey, "key2"},
					[][]byte{[]byte("abc"), []byte("abc")},
					ttls))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				err := redisCache.Set(itemsSeq(ctx,
					[]string{testKey, "key2", "key3"},
					[][]byte{[]byte("abc"), []byte("abc"), []byte("abc")},
					ttls))
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
			do: func(t *testing.T, redisCache *RedisCache) error {
				err := redisCache.Set(itemsSeq(ctx,
					[]string{testKey, testKey},
					[][]byte{[]byte("abc"), []byte("abc")},
					ttls))
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

func iterBytesErr(seq iter.Seq2[[]byte, error]) error {
	for _, err := range seq {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestRedisCache_WithBatchSize(t *testing.T) {
	redisCache := New(nil)
	require.NotNil(t, redisCache)

	assert.Equal(t, defaultBatchSize, redisCache.batchSize)

	batchSize := redisCache.batchSize * 2
	assert.Same(t, redisCache, redisCache.WithBatchSize(batchSize))
	assert.Equal(t, batchSize, redisCache.batchSize)
}

func TestRedisCache_MGetSet_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	blobs := make([][]byte, len(keys))
	times := make([]time.Duration, len(keys))

	blob := []byte("foobar")
	ttl := time.Minute
	ctx := t.Context()

	tests := []struct {
		name     string
		singleOp func(rdb *mocks.MockCmdable)
		pipeOp   func(pipe *mocks.MockPipeliner, fn func(cmd redis.Cmder))
		cacheOp  func(t *testing.T, redisCache *RedisCache, nKeys int)
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
			cacheOp: func(t *testing.T, redisCache *RedisCache, nKeys int) {
				bytesIter := redisCache.Get(ctx, len(keys[:nKeys]),
					slices.Values(keys[:nKeys]))
				for b, err := range bytesIter {
					require.NoError(t, err)
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
			cacheOp: func(t *testing.T, redisCache *RedisCache, nKeys int) {
				require.NoError(t, redisCache.Set(itemsSeq(ctx,
					keys[:nKeys], blobs[:nKeys], times[:nKeys])))
			},
		},
	}

	for i := range len(keys) {
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
				for range nExec {
					expect = append(expect, redisCache.batchSize)
				}
				if n := nKeys % redisCache.batchSize; n > 0 {
					expect = append(expect, n)
				}

				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)

				var cmds []redis.Cmder
				if nKeys > 0 {
					tt.pipeOp(pipe, func(cmd redis.Cmder) { cmds = append(cmds, cmd) })
				}
				pipe.EXPECT().Len().RunAndReturn(func() int { return len(cmds) })

				var got []int
				if n := len(expect); n > 0 {
					pipe.EXPECT().Exec(ctx).RunAndReturn(
						func(ctx context.Context) ([]redis.Cmder, error) {
							got = append(got, len(cmds))
							gotCmds := cmds
							cmds = cmds[:0]
							return gotCmds, nil
						}).Times(n)
				}

				tt.cacheOp(t, redisCache, nKeys)
				assert.Equal(t, expect, got)
			})
		}
	}
}

func TestRedisCache_Del_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	for i := range len(keys) {
		keys[i] = fmt.Sprintf("key-%00d", i)
	}
	ctx := t.Context()

	for nKeys := 0; nKeys <= len(keys); nKeys++ {
		t.Run(fmt.Sprintf("with %d keys", nKeys), func(t *testing.T) {
			rdb := mocks.NewMockCmdable(t)
			redisCache := New(rdb)
			require.NotNil(t, redisCache)
			redisCache.WithBatchSize(batchSize)

			var wantKeys [][]string
			nDel := nKeys / redisCache.batchSize
			for i := range nDel {
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

func TestRedisCache_WithGetRefreshTTL(t *testing.T) {
	redisCache := New(nil)
	require.NotNil(t, redisCache)

	assert.Equal(t, time.Duration(0), redisCache.refreshTTL)

	ttl := time.Minute
	assert.Same(t, redisCache, redisCache.WithGetRefreshTTL(ttl))
	assert.Equal(t, ttl, redisCache.refreshTTL)
}

func TestRedisCache_respectRefreshTTL(t *testing.T) {
	ctx := t.Context()
	ttl := time.Minute
	strResult := redis.NewStringResult("", nil)

	tests := []struct {
		name   string
		expect func(redisCache *RedisCache, rdb *mocks.MockCmdable) ([]byte, error)
	}{
		{
			name: "Get without refreshTTL",
			expect: func(redisCache *RedisCache, rdb *mocks.MockCmdable) ([]byte, error) {
				rdb.EXPECT().Get(ctx, testKey).Return(strResult)
				return firstBytes(redisCache.Get(
					ctx, 1, slices.Values([]string{testKey})))
			},
		},
		{
			name: "Get with refreshTTL",
			expect: func(redisCache *RedisCache, rdb *mocks.MockCmdable) ([]byte, error) {
				redisCache.WithGetRefreshTTL(ttl)
				rdb.EXPECT().GetEx(ctx, testKey, ttl).Return(strResult)
				return firstBytes(redisCache.Get(
					ctx, 1, slices.Values([]string{testKey})))
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

func firstBytes(seq iter.Seq2[[]byte, error]) ([]byte, error) {
	for b, err := range seq {
		return b, err
	}
	return nil, nil
}

func TestRedisCache_Set_skipEmptyItems(t *testing.T) {
	ctx := t.Context()
	foobar := []byte("foobar")
	ttl := time.Minute

	pipe := mocks.NewMockPipeliner(t)
	cmds := make([]redis.Cmder, 0, 1)
	pipe.EXPECT().Set(ctx, testKey, foobar, mock.Anything).RunAndReturn(
		func(context.Context, string, any, time.Duration) *redis.StatusCmd {
			cmd := redis.NewStatusResult("", nil)
			cmds = append(cmds, cmd)
			return cmd
		}).Times(3)
	pipe.EXPECT().Len().RunAndReturn(func() int { return len(cmds) })
	pipe.EXPECT().Exec(ctx).RunAndReturn(
		func(ctx context.Context) ([]redis.Cmder, error) { return cmds, nil })

	rdb := mocks.NewMockCmdable(t)
	rdb.EXPECT().Pipeline().Return(pipe)

	redisCache := New(rdb)
	require.NotNil(t, redisCache)
	require.NoError(t, redisCache.Set(itemsSeq(ctx,
		[]string{testKey, testKey, testKey, testKey},
		[][]byte{{}, foobar, foobar, foobar},
		[]time.Duration{ttl, 0, -1, ttl})))
}

// --------------------------------------------------

func BenchmarkRedisCache_Get(b *testing.B) {
	redisCache, rdb := MustNew()
	ctx := b.Context()

	allKeys := []string{"key1"}

	b.SetParallelism(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bytesIter := redisCache.Get(ctx, len(allKeys), slices.Values(allKeys))
			for _, err := range bytesIter {
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.StopTimer()

	rdb.Close()
}

func BenchmarkRedisCache_Set(b *testing.B) {
	redisCache, rdb := MustNew()
	ctx := b.Context()

	b.SetParallelism(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := redisCache.Set(ctx, 1, slices.Values([]Item{
				NewItem("key1", []byte("value1"), time.Minute),
			}))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()

	rdb.Close()
}
