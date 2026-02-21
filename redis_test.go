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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	rdbOffset      = 0
	cacheNamespace = "expx-cache-redis/redis-test:"
	keyLocked      = cacheNamespace + "lock:"
	testKey        = cacheNamespace + "test-key"
)

func MustNew() (*Cache, *redis.Client) {
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

func TestCacheSuite(t *testing.T) {
	rdb := mustValue[*redis.Client](t)(NewRedisClient())
	if rdb == nil {
		t.Skipf("skip %q, because no Redis connection", t.Name())
	} else {
		ctx := t.Context()
		require.NoError(t, rdb.FlushDB(ctx).Err())
		t.Cleanup(func() { require.NoError(t, rdb.Close()) })
	}
	suite.Run(t, &CacheTestSuite{rdb: rdb})
}

func mustValue[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

type CacheTestSuite struct {
	suite.Suite

	rdb Cmdable
}

func (self *CacheTestSuite) TearDownTest() {
	self.Require().NoError(self.rdb.FlushDB(self.T().Context()).Err())
}

func (self *CacheTestSuite) TestCache() {
	tests := []struct {
		name   string
		keys   []string
		values [][]byte
		ttl    []time.Duration
		cfg    func(cache *Cache)
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
			cfg:    func(cache *Cache) { cache.WithBatchSize(2) },
		},
	}

	for _, tt := range tests {
		self.Run(tt.name, func() {
			cache := self.testNew(tt.cfg)
			self.testCache(cache, tt.keys, tt.values, tt.ttl)
		})
	}
}

func (self *CacheTestSuite) testNew(cacheOpts ...func(*Cache)) *Cache {
	cache := New(self.rdb)
	for _, opt := range cacheOpts {
		if opt != nil {
			opt(cache)
		}
	}
	return cache
}

func (self *CacheTestSuite) testCache(cache *Cache, keys []string,
	values [][]byte, ttl []time.Duration,
) {
	ctx := self.T().Context()

	self.Require().NoError(cache.Set(itemsSeq(ctx, keys, values, ttl)))
	iterBytes := cache.Get(ctx, len(keys), slices.Values(keys))

	bytes := make([][]byte, 0, len(keys))
	for b, err := range iterBytes {
		self.Require().NoError(err)
		bytes = append(bytes, b)
	}
	self.Equal(values, bytes)

	self.Require().NoError(cache.Del(ctx, keys))
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

func (self *CacheTestSuite) TestLockGet() {
	const bar = "bar"
	keySet := self.resolveKeyLock("tests-key")
	self.T().Logf("keySet=%q", keySet)
	ctx := self.T().Context()
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

func (self *CacheTestSuite) resolveKeyLock(key string) string {
	return keyLocked + key
}

func (self *CacheTestSuite) getKey(r *Cache, key string) []byte {
	iterBytes := r.Get(self.T().Context(), 1, slices.Values([]string{key}))
	for b, err := range iterBytes {
		self.Require().NoError(err)
		return b
	}
	self.Require().Fail("Get returned empty iterator")
	return nil
}

func (self *CacheTestSuite) setKey(r *Cache, key string, b []byte,
	ttl time.Duration,
) {
	ctx := self.T().Context()
	self.Require().NoError(r.Set(ctx, 1, slices.Values([]Item{
		NewItem(key, b, ttl),
	})))
}

func TestCache_errors(t *testing.T) {
	ctx := t.Context()
	ttl := time.Minute
	ttls := []time.Duration{ttl, ttl, ttl}
	wantErr := errors.New("expected error")
	strResult := redis.NewStringResult("", nil)

	tests := []struct {
		name      string
		configure func(t *testing.T, rdb *MoqCmdable)
		do        func(t *testing.T, cache *Cache) error
		assertErr func(t *testing.T, err error)
		assertMoq func(t *testing.T, rdb *MoqCmdable)
	}{
		{
			name: "Del",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				rdb.DelFunc = func(ctx context.Context, keys ...string) *redis.IntCmd {
					return redis.NewIntResult(0, wantErr)
				}
			},
			do: func(t *testing.T, cache *Cache) error {
				return cache.Del(ctx, []string{testKey})
			},
		},
		{
			name: "Get error from getter 1",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				rdb.GetFunc = func(ctx context.Context, key string) *redis.StringCmd {
					return redis.NewStringResult("", wantErr)
				}
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 1, slices.Values([]string{testKey})))
			},
		},
		{
			name: "Get error from getter 2",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						return redis.NewStringResult("", wantErr)
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 3, slices.Values(
					[]string{testKey, "key2", "key3"})))
			},
		},
		{
			name: "Get error from batchSize",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				expectedKeys := []string{testKey, "key2", "key3"}
				var pipeLen int
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						assert.Equal(t, expectedKeys[pipeLen], key)
						pipeLen++
						return strResult
					},

					LenFunc: func() int { return pipeLen },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, wantErr
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 3, slices.Values(
					[]string{testKey, "key2", "key3"})))
			},
		},
		{
			name: "Get error from Exec",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				expectedKeys := []string{testKey, "key2"}
				var pipeLen int
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						assert.Equal(t, expectedKeys[pipeLen], key)
						pipeLen++
						return strResult
					},

					LenFunc: func() int { return pipeLen },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, wantErr
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
			},
		},
		{
			name: "Get error from StringCmd",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				expectedKeys := []string{testKey, "key2"}
				var pipeLen int
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						assert.Equal(t, expectedKeys[pipeLen], key)
						pipeLen++
						return strResult
					},

					LenFunc: func() int { return pipeLen },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewStringResult("", wantErr)}
						return cmds, nil
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
			},
		},
		{
			name: "Get error from BoolCmd",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				expectedKeys := []string{testKey, "key2"}
				var pipeLen int
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						assert.Equal(t, expectedKeys[pipeLen], key)
						pipeLen++
						return strResult
					},

					LenFunc: func() int { return pipeLen },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, wantErr)}
						return cmds, wantErr
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
			},
		},
		{
			name: "Get unexpected type",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				expectedKeys := []string{testKey, "key2"}
				var pipeLen int
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						assert.Equal(t, expectedKeys[pipeLen], key)
						pipeLen++
						return strResult
					},

					LenFunc: func() int { return pipeLen },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, nil)}
						return cmds, nil
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "unexpected type=")
			},
		},
		{
			name: "Get with Nil",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				pipe := &MoqPipeliner{
					GetFunc: func(ctx context.Context, key string) *redis.StringCmd {
						return redis.NewStringResult("", nil)
					},

					LenFunc: func() int { return 2 },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						return []redis.Cmder{
							redis.NewStringResult("", nil),
							redis.NewStringResult("", redis.Nil),
						}, nil
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				return iterBytesErr(cache.Get(ctx, 2, slices.Values(
					[]string{testKey, "key2"})))
			},
			assertErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "Set error from SET 1",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				rdb.SetFunc = func(ctx context.Context, key string, value any,
					expiration time.Duration,
				) *redis.StatusCmd {
					assert.Equal(t, testKey, key)
					assert.Equal(t, ttl, expiration)
					return redis.NewStatusResult("", wantErr)
				}
			},
			do: func(t *testing.T, cache *Cache) error {
				err := cache.Set(ctx, 1, slices.Values([]Item{
					NewItem(testKey, []byte("abc"), ttl),
				}))
				return err
			},
		},
		{
			name: "Set error from SET 2",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				pipe := &MoqPipeliner{
					SetFunc: func(ctx context.Context, key string, value any,
						expiration time.Duration,
					) *redis.StatusCmd {
						assert.Equal(t, ttl, expiration)
						if key == testKey {
							return redis.NewStatusResult("", nil)
						}
						assert.Equal(t, "key2", key)
						return redis.NewStatusResult("", wantErr)
					},

					LenFunc: func() int { return 1 },
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				err := cache.Set(itemsSeq(ctx,
					[]string{testKey, "key2"},
					[][]byte{[]byte("abc"), []byte("abc")},
					ttls))
				return err
			},
		},
		{
			name: "Set error from batchSize",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				wantKeys := []string{testKey, "key2", "key3"}
				var pipeLen int
				pipe := &MoqPipeliner{
					SetFunc: func(ctx context.Context, key string, value any,
						expiration time.Duration,
					) *redis.StatusCmd {
						assert.Equal(t, wantKeys[pipeLen], key)
						assert.Equal(t, ttl, expiration)
						pipeLen++
						return redis.NewStatusResult("", nil)
					},

					LenFunc: func() int { return pipeLen },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, wantErr
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				err := cache.Set(itemsSeq(ctx,
					[]string{testKey, "key2", "key3"},
					[][]byte{[]byte("abc"), []byte("abc"), []byte("abc")},
					ttls))
				return err
			},
		},
		{
			name: "Set error from Exec",
			configure: func(t *testing.T, rdb *MoqCmdable) {
				pipe := &MoqPipeliner{
					SetFunc: func(ctx context.Context, key string, value any,
						expiration time.Duration,
					) *redis.StatusCmd {
						assert.Equal(t, testKey, key)
						assert.Equal(t, ttl, expiration)
						return redis.NewStatusResult("", nil)
					},

					LenFunc: func() int { return 2 },

					ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, wantErr
					},
				}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }
			},
			do: func(t *testing.T, cache *Cache) error {
				err := cache.Set(itemsSeq(ctx,
					[]string{testKey, testKey},
					[][]byte{[]byte("abc"), []byte("abc")},
					ttls))
				return err
			},
			assertMoq: func(t *testing.T, rdb *MoqCmdable) {
				pipe := rdb.Pipeline().(*MoqPipeliner)
				assert.Len(t, pipe.SetCalls(), 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb := &MoqCmdable{}
			cache := New(rdb).WithBatchSize(3)
			tt.configure(t, rdb)
			err := tt.do(t, cache)
			if tt.assertErr != nil {
				tt.assertErr(t, err)
				return
			}
			require.ErrorIs(t, err, wantErr)

			if tt.assertMoq != nil {
				tt.assertMoq(t, rdb)
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

func TestCache_WithBatchSize(t *testing.T) {
	cache := New(nil)
	require.NotNil(t, cache)

	assert.Equal(t, defaultBatchSize, cache.batchSize)

	batchSize := cache.batchSize * 2
	assert.Same(t, cache, cache.WithBatchSize(batchSize))
	assert.Equal(t, batchSize, cache.batchSize)
}

func TestCache_MGetSet_WithBatchSize(t *testing.T) {
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
		singleOp func(rdb *MoqCmdable)
		pipeOp   func(pipe *MoqPipeliner, fn func(cmd redis.Cmder))
		cacheOp  func(t *testing.T, cache *Cache, nKeys int)
	}{
		{
			name: "Get",
			singleOp: func(rdb *MoqCmdable) {
				rdb.GetFunc = func(ctx context.Context, key string) *redis.StringCmd {
					return redis.NewStringResult("", nil)
				}
			},
			pipeOp: func(pipe *MoqPipeliner, fn func(cmd redis.Cmder)) {
				pipe.GetFunc = func(ctx context.Context, key string) *redis.StringCmd {
					cmd := redis.NewStringResult("", nil)
					fn(cmd)
					return cmd
				}
			},
			cacheOp: func(t *testing.T, cache *Cache, nKeys int) {
				bytesIter := cache.Get(ctx, len(keys[:nKeys]),
					slices.Values(keys[:nKeys]))
				for b, err := range bytesIter {
					require.NoError(t, err)
					assert.Nil(t, b)
				}
			},
		},
		{
			name: "Set",
			singleOp: func(rdb *MoqCmdable) {
				rdb.SetFunc = func(ctx context.Context, key string, value any,
					expiration time.Duration,
				) *redis.StatusCmd {
					return redis.NewStatusResult("", nil)
				}
			},
			pipeOp: func(pipe *MoqPipeliner, fn func(cmd redis.Cmder)) {
				pipe.SetFunc = func(ctx context.Context, key string, value any,
					expiration time.Duration,
				) *redis.StatusCmd {
					assert.Equal(t, blob, value)
					assert.Equal(t, ttl, expiration)
					cmd := redis.NewStatusResult("", nil)
					fn(cmd)
					return cmd
				}
			},
			cacheOp: func(t *testing.T, cache *Cache, nKeys int) {
				require.NoError(t, cache.Set(itemsSeq(ctx,
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
				rdb := &MoqCmdable{}
				cache := New(rdb).WithBatchSize(batchSize)
				require.NotNil(t, cache)

				if nKeys == 1 && tt.singleOp != nil {
					tt.singleOp(rdb)
					tt.cacheOp(t, cache, nKeys)
					return
				}

				var expect []int
				nExec := nKeys / cache.batchSize
				for range nExec {
					expect = append(expect, cache.batchSize)
				}
				if n := nKeys % cache.batchSize; n > 0 {
					expect = append(expect, n)
				}

				pipe := &MoqPipeliner{}
				rdb.PipelineFunc = func() redis.Pipeliner { return pipe }

				var cmds []redis.Cmder
				if nKeys > 0 {
					tt.pipeOp(pipe, func(cmd redis.Cmder) { cmds = append(cmds, cmd) })
				}
				pipe.LenFunc = func() int { return len(cmds) }

				var got []int
				if n := len(expect); n > 0 {
					pipe.ExecFunc = func(ctx context.Context) ([]redis.Cmder, error) {
						got = append(got, len(cmds))
						gotCmds := cmds
						cmds = cmds[:0]
						return gotCmds, nil
					}
				}

				tt.cacheOp(t, cache, nKeys)
				assert.Equal(t, expect, got)
				assert.Len(t, pipe.ExecCalls(), len(expect))
			})
		}
	}
}

func TestCache_Del_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	for i := range len(keys) {
		keys[i] = fmt.Sprintf("key-%00d", i)
	}
	ctx := t.Context()

	for nKeys := 0; nKeys <= len(keys); nKeys++ {
		t.Run(fmt.Sprintf("with %d keys", nKeys), func(t *testing.T) {
			rdb := &MoqCmdable{}
			cache := New(rdb)
			require.NotNil(t, cache)
			cache.WithBatchSize(batchSize)

			var wantKeys [][]string
			nDel := nKeys / cache.batchSize
			for i := range nDel {
				low := i * cache.batchSize
				high := low + cache.batchSize
				wantKeys = append(wantKeys, keys[low:high])
			}
			if nKeys%cache.batchSize > 0 {
				low := nDel * cache.batchSize
				high := low + nKeys%cache.batchSize
				wantKeys = append(wantKeys, keys[low:high])
			}

			var gotKeys [][]string
			rdb.DelFunc = func(ctx context.Context, keys ...string) *redis.IntCmd {
				gotKeys = append(gotKeys, keys)
				return redis.NewIntResult(int64(len(keys)), nil)
			}

			require.NoError(t, cache.Del(ctx, keys[:nKeys]))
			assert.Equal(t, wantKeys, gotKeys)
		})
	}
}

func TestCache_WithGetRefreshTTL(t *testing.T) {
	cache := New(nil)
	require.NotNil(t, cache)

	assert.Equal(t, time.Duration(0), cache.refreshTTL)

	ttl := time.Minute
	assert.Same(t, cache, cache.WithGetRefreshTTL(ttl))
	assert.Equal(t, ttl, cache.refreshTTL)
}

func TestCache_respectRefreshTTL(t *testing.T) {
	ctx := t.Context()
	ttl := time.Minute
	strResult := redis.NewStringResult("", nil)

	tests := []struct {
		name   string
		expect func(cache *Cache, rdb *MoqCmdable) ([]byte, error)
	}{
		{
			name: "Get without refreshTTL",
			expect: func(cache *Cache, rdb *MoqCmdable) ([]byte,
				error,
			) {
				rdb.GetFunc = func(ctx context.Context, key string) *redis.StringCmd {
					assert.Equal(t, testKey, key)
					return strResult
				}
				return firstBytes(cache.Get(ctx, 1,
					slices.Values([]string{testKey})))
			},
		},
		{
			name: "Get with refreshTTL",
			expect: func(cache *Cache, rdb *MoqCmdable) ([]byte,
				error,
			) {
				cache.WithGetRefreshTTL(ttl)
				rdb.GetExFunc = func(ctx context.Context, key string,
					expiration time.Duration,
				) *redis.StringCmd {
					assert.Equal(t, testKey, key)
					assert.Equal(t, ttl, expiration)
					return strResult
				}
				return firstBytes(cache.Get(ctx, 1,
					slices.Values([]string{testKey})))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb := &MoqCmdable{}
			cache := New(rdb)
			require.NotNil(t, cache)
			b := mustValue[[]byte](t)(tt.expect(cache, rdb))
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

func TestCache_Set_skipEmptyItems(t *testing.T) {
	ctx := t.Context()
	foobar := []byte("foobar")
	ttl := time.Minute

	cmds := make([]redis.Cmder, 0, 1)
	pipe := &MoqPipeliner{
		SetFunc: func(ctx context.Context, key string, value any,
			expiration time.Duration,
		) *redis.StatusCmd {
			cmd := redis.NewStatusResult("", nil)
			cmds = append(cmds, cmd)
			return cmd
		},

		LenFunc: func() int { return len(cmds) },

		ExecFunc: func(ctx context.Context) ([]redis.Cmder, error) {
			return cmds, nil
		},
	}

	rdb := &MoqCmdable{
		PipelineFunc: func() redis.Pipeliner { return pipe },
	}

	cache := New(rdb)
	require.NotNil(t, cache)
	require.NoError(t, cache.Set(itemsSeq(ctx,
		[]string{testKey, testKey, testKey, testKey},
		[][]byte{{}, foobar, foobar, foobar},
		[]time.Duration{ttl, 0, -1, ttl})))
	assert.Len(t, pipe.SetCalls(), 3)
}

// --------------------------------------------------

func BenchmarkCache_Get(b *testing.B) {
	cache, rdb := MustNew()
	ctx := b.Context()

	allKeys := []string{"key1"}

	b.SetParallelism(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bytesIter := cache.Get(ctx, len(allKeys), slices.Values(allKeys))
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

func BenchmarkCache_Set(b *testing.B) {
	cache, rdb := MustNew()
	ctx := b.Context()

	b.SetParallelism(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := cache.Set(ctx, 1, slices.Values([]Item{
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
