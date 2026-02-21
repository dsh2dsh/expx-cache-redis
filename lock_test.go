package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_LockGet_errors(t *testing.T) {
	const keySet = "key1"
	const keyGet = "key2"
	const bar = "bar"
	ctx := t.Context()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := &MoqCmdable{
		PipelinedFunc: func(ctx context.Context, fn func(redis.Pipeliner) error,
		) ([]redis.Cmder, error) {
			return nil, wantErr
		},
	}
	r := New(rdb)
	_, _, err := r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)
	assert.Len(t, rdb.PipelinedCalls(), 1)

	pipe := &MoqPipeliner{
		SetNXFunc: func(ctx context.Context, key string, value any,
			expiration time.Duration,
		) *redis.BoolCmd {
			assert.Equal(t, keySet, key)
			assert.Equal(t, bar, value)
			assert.Equal(t, ttl, expiration)
			return redis.NewBoolResult(false, wantErr)
		},
	}
	rdb.PipelinedFunc = func(ctx context.Context, fn func(redis.Pipeliner) error,
	) ([]redis.Cmder, error) {
		return nil, fn(pipe)
	}
	rdb.ResetPipelinedCalls()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)
	assert.Len(t, pipe.SetNXCalls(), 1)
	assert.Len(t, rdb.PipelinedCalls(), 1)

	rdb.PipelinedFunc = func(ctx context.Context, fn func(redis.Pipeliner) error,
	) ([]redis.Cmder, error) {
		return []redis.Cmder{}, nil
	}
	rdb.ResetPipelinedCalls()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.Error(t, err)
	assert.Len(t, rdb.PipelinedCalls(), 1)

	rdb.PipelinedFunc = func(ctx context.Context, fn func(redis.Pipeliner) error,
	) ([]redis.Cmder, error) {
		return []redis.Cmder{
			redis.NewBoolResult(false, wantErr),
			redis.NewStringResult("", wantErr),
		}, nil
	}
	rdb.ResetPipelinedCalls()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)
	assert.Len(t, rdb.PipelinedCalls(), 1)

	rdb.PipelinedFunc = func(ctx context.Context, fn func(redis.Pipeliner) error,
	) ([]redis.Cmder, error) {
		return []redis.Cmder{
			redis.NewBoolResult(false, nil),
			redis.NewStringResult("", wantErr),
		}, nil
	}
	rdb.ResetPipelinedCalls()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)
	assert.Len(t, rdb.PipelinedCalls(), 1)

	rdb.PipelinedFunc = func(ctx context.Context, fn func(redis.Pipeliner) error,
	) ([]redis.Cmder, error) {
		return []redis.Cmder{
			redis.NewStringResult("", wantErr),
			redis.NewStringResult("", wantErr),
		}, nil
	}
	rdb.ResetPipelinedCalls()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.Error(t, err)
	assert.Len(t, rdb.PipelinedCalls(), 1)

	rdb.PipelinedFunc = func(ctx context.Context, fn func(redis.Pipeliner) error,
	) ([]redis.Cmder, error) {
		return []redis.Cmder{
			redis.NewBoolResult(false, nil),
			redis.NewStringResult("", nil),
		}, nil
	}
	rdb.ResetPipelinedCalls()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.NoError(t, err)
	assert.Len(t, rdb.PipelinedCalls(), 1)
}

func (self *CacheTestSuite) TestExpire() {
	ctx := self.T().Context()
	keyLock := self.resolveKeyLock("test-key")
	ttl := time.Minute

	r := self.testNew()
	ok, err := r.Expire(ctx, keyLock, ttl)
	self.Require().NoError(err)
	self.False(ok)

	self.setKey(r, keyLock, []byte("foobar"), ttl)
	ok, err = r.Expire(ctx, keyLock, ttl)
	self.Require().NoError(err)
	self.True(ok)

	gotTTL, err := self.rdb.TTL(ctx, keyLock).Result()
	self.Require().NoError(err)
	self.Equal(ttl, gotTTL)

	ttl2 := 2 * time.Minute
	ok, err = r.Expire(ctx, keyLock, ttl2)
	self.Require().NoError(err)
	self.True(ok)

	gotTTL, err = self.rdb.TTL(ctx, keyLock).Result()
	self.Require().NoError(err)
	self.Equal(ttl2, gotTTL)
}

func TestExpire_error(t *testing.T) {
	ctx := t.Context()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := &MoqCmdable{
		ExpireFunc: func(ctx context.Context, key string, expiration time.Duration,
		) *redis.BoolCmd {
			return redis.NewBoolResult(false, wantErr)
		},
	}
	r := New(rdb)

	ok, err := r.Expire(ctx, testKey, ttl)
	require.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}

func (self *CacheTestSuite) TestUnlock() {
	const foobar = "foobar"
	ctx := self.T().Context()
	keyLock := self.resolveKeyLock("test-key")
	ttl := time.Minute

	r := self.testNew()
	ok, err := r.Unlock(ctx, keyLock, foobar)
	self.Require().NoError(err)
	self.False(ok)

	self.setKey(r, keyLock, []byte(foobar), ttl)
	ok, err = r.Unlock(ctx, keyLock, "foobaz")
	self.Require().NoError(err)
	self.False(ok)

	ok, err = r.Unlock(ctx, keyLock, foobar)
	self.Require().NoError(err)
	self.True(ok)
}

func TestUnlock_error(t *testing.T) {
	ctx := t.Context()
	wantErr := errors.New("test error")

	rdb := &MoqCmdable{
		EvalShaFunc: func(ctx context.Context, sha1 string, keys []string,
			args ...any,
		) *redis.Cmd {
			assert.Equal(t, []string{testKey}, keys)
			return redis.NewCmdResult(0, wantErr)
		},
	}
	r := New(rdb)

	ok, err := r.Unlock(ctx, testKey, "foobar")
	require.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}
