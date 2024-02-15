package redis

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
	"github.com/stretchr/testify/suite"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

const (
	rdbOffset = 0
	testKey   = "mykey"
)

func MustNew() (*Classic, *redis.Client) {
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

func bytesFromIter(iter func() ([]byte, bool)) []byte {
	b, _ := iter()
	return b
}

// --------------------------------------------------

func TestClassicSuite(t *testing.T) {
	rdb := valueNoError[*redis.Client](t)(NewRedisClient())
	if rdb == nil {
		t.Skipf("skip %q, because no Redis connection", t.Name())
	} else {
		ctx := context.Background()
		require.NoError(t, rdb.FlushDB(ctx).Err())
		t.Cleanup(func() { require.NoError(t, rdb.Close()) })
	}
	suite.Run(t, &ClassicTestSuite{rdb: rdb})
}

func valueNoError[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

type ClassicTestSuite struct {
	suite.Suite
	rdb Cmdable
}

func (self *ClassicTestSuite) TearDownTest() {
	self.Require().NoError(self.rdb.FlushDB(context.Background()).Err())
}

func (self *ClassicTestSuite) TestClassic() {
	tests := []struct {
		name   string
		keys   []string
		values [][]byte
		ttl    []time.Duration
		cfg    func(redisCache *Classic)
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
			cfg: func(redisCache *Classic) {
				redisCache.WithBatchSize(2)
			},
		},
	}

	for _, tt := range tests {
		self.Run(tt.name, func() {
			redisCache := self.testNew(tt.cfg)
			self.testClassic(redisCache, tt.keys, tt.values, tt.ttl)
		})
	}
}

func (self *ClassicTestSuite) testNew(cacheOpts ...func(*Classic)) *Classic {
	redisCache := New(self.rdb)
	for _, opt := range cacheOpts {
		if opt != nil {
			opt(redisCache)
		}
	}
	return redisCache
}

func (self *ClassicTestSuite) testClassic(
	redisCache *Classic, keys []string, values [][]byte, ttl []time.Duration,
) {
	ctx := context.Background()

	self.Require().NoError(redisCache.Set(MakeSetIter3(ctx, keys, values, ttl)))
	iterBytes, err := redisCache.Get(MakeGetIter3(ctx, keys))
	self.Require().NoError(err)

	var bytes [][]byte
	for b, ok := iterBytes(); ok; b, ok = iterBytes() {
		bytes = append(bytes, b)
	}
	self.Equal(values, bytes)

	self.Require().NoError(redisCache.Del(ctx, keys))
}

func (self *ClassicTestSuite) TestLockGet() {
	const keySet = "key1"
	const keyGet = "key2"
	const bar = "bar"
	ctx := context.Background()
	ttl := time.Minute
	foobar := []byte("foobar")

	r := self.testNew()
	ok, b, err := r.LockGet(ctx, keySet, "foo", ttl, keyGet)
	self.Require().NoError(err)
	self.True(ok)
	self.Empty(b)
	self.Equal("foo", string(self.getKey(r, keySet)))

	ok, b, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	self.Require().NoError(err)
	self.False(ok)
	self.Empty(b)
	self.Equal("foo", string(self.getKey(r, keySet)))

	self.setKey(r, keyGet, foobar, ttl)
	ok, b, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	self.Require().NoError(err)
	self.False(ok)
	self.Equal(foobar, b)

	self.Require().NoError(r.Del(ctx, []string{keySet}))
	ok, b, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	self.Require().NoError(err)
	self.True(ok)
	self.Equal(foobar, b)
	self.Equal(bar, string(self.getKey(r, keySet)))
}

func (self *ClassicTestSuite) getKey(r *Classic, key string) []byte {
	iterBytes, err := r.Get(MakeGetIter3(context.Background(), []string{key}))
	self.Require().NoError(err)
	b, ok := iterBytes()
	self.Require().True(ok)
	return b
}

func (self *ClassicTestSuite) setKey(r *Classic, key string, b []byte,
	ttl time.Duration,
) {
	self.Require().NoError(r.Set(MakeSetIter3(
		context.Background(), []string{key}, [][]byte{b}, []time.Duration{ttl})))
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

func TestClassic_Del_WithBatchSize(t *testing.T) {
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

func TestClassic_WithGetRefreshTTL(t *testing.T) {
	redisCache := New(nil)
	require.NotNil(t, redisCache)

	assert.Equal(t, time.Duration(0), redisCache.refreshTTL)

	ttl := time.Minute
	assert.Same(t, redisCache, redisCache.WithGetRefreshTTL(ttl))
	assert.Equal(t, ttl, redisCache.refreshTTL)
}

func TestClassic_respectRefreshTTL(t *testing.T) {
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

func TestClassic_Set_skipEmptyItems(t *testing.T) {
	ctx := context.Background()
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
	redisCache, rdb := MustNew()
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
	b.StopTimer()

	rdb.Close()
}

func BenchmarkClassic_Set(b *testing.B) {
	redisCache, rdb := MustNew()
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
	b.StopTimer()

	rdb.Close()
}
