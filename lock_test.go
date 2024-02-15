package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestRedisCache_LockGet_errors(t *testing.T) {
	const keySet = "key1"
	const keyGet = "key2"
	const bar = "bar"
	ctx := context.Background()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	r := New(rdb)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return(nil, wantErr).Once()
	_, _, err := r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	pipe := mocks.NewMockPipeliner(t)
	pipe.EXPECT().SetNX(ctx, keySet, bar, ttl).Return(
		redis.NewBoolResult(false, wantErr)).Once()
	rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
		func(ctx context.Context, fn func(redis.Pipeliner) error,
		) ([]redis.Cmder, error) {
			return nil, fn(pipe)
		}).Once()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{}, nil).Once()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.Error(t, err)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewBoolResult(false, wantErr),
		redis.NewStringResult("", wantErr),
	}, nil).Once()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewBoolResult(false, nil),
		redis.NewStringResult("", wantErr),
	}, nil).Once()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewStringResult("", wantErr),
		redis.NewStringResult("", wantErr),
	}, nil).Once()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.Error(t, err)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewBoolResult(false, nil),
		redis.NewStringResult("", nil),
	}, nil).Once()
	_, _, err = r.LockGet(ctx, keySet, bar, ttl, keyGet)
	require.NoError(t, err)
}

func (self *RedisCacheTestSuite) TestExpire() {
	ctx := context.Background()
	ttl := time.Minute

	r := self.testNew()
	ok, err := r.Expire(ctx, testKey, ttl)
	self.Require().NoError(err)
	self.False(ok)

	self.setKey(r, testKey, []byte("foobar"), ttl)
	ok, err = r.Expire(ctx, testKey, ttl)
	self.Require().NoError(err)
	self.True(ok)

	gotTTL, err := self.rdb.TTL(ctx, testKey).Result()
	self.Require().NoError(err)
	self.Equal(ttl, gotTTL)

	ttl2 := 2 * time.Minute
	ok, err = r.Expire(ctx, testKey, ttl2)
	self.Require().NoError(err)
	self.True(ok)

	gotTTL, err = self.rdb.TTL(ctx, testKey).Result()
	self.Require().NoError(err)
	self.Equal(ttl2, gotTTL)
}

func TestExpire_error(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	r := New(rdb)

	rdb.EXPECT().Expire(ctx, testKey, ttl).Return(redis.NewBoolResult(false, wantErr))
	ok, err := r.Expire(ctx, testKey, ttl)
	require.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}

func (self *RedisCacheTestSuite) TestUnlock() {
	const foobar = "foobar"
	ctx := context.Background()
	ttl := time.Minute

	r := self.testNew()
	ok, err := r.Unlock(ctx, testKey, foobar)
	self.Require().NoError(err)
	self.False(ok)

	self.setKey(r, testKey, []byte(foobar), ttl)
	ok, err = r.Unlock(ctx, testKey, "foobaz")
	self.Require().NoError(err)
	self.False(ok)

	ok, err = r.Unlock(ctx, testKey, foobar)
	self.Require().NoError(err)
	self.True(ok)
}

func TestUnlock_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	r := New(rdb)

	rdb.EXPECT().EvalSha(ctx, mock.Anything, []string{testKey}, mock.Anything).
		Return(redis.NewCmdResult(0, wantErr))
	ok, err := r.Unlock(ctx, testKey, "foobar")
	require.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}

func (self *RedisCacheTestSuite) TestListen() {
	const foobar = "foobar"
	ctx := context.Background()

	ch := make(chan struct{})
	go func() {
		<-ch
		time.Sleep(100 * time.Millisecond)
		self.NoError(self.rdb.Publish(ctx, testKey, foobar).Err())
		ch <- struct{}{}
	}()
	ch <- struct{}{}

	r := self.testNew()
	value, err := r.Listen(ctx, testKey)
	<-ch
	self.Require().NoError(err)
	self.Equal(foobar, value)
}

func (self *RedisCacheTestSuite) TestListen_cancel() {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan struct{})
	go func() {
		<-ch
		time.Sleep(100 * time.Millisecond)
		cancel()
		ch <- struct{}{}
	}()
	ch <- struct{}{}

	r := self.testNew()
	value, err := r.Listen(ctx, testKey)
	<-ch
	self.Require().ErrorIs(err, context.Canceled)
	self.Empty(value)
}

func (self *RedisCacheTestSuite) TestListen_subscribeError() {
	ctx := context.Background()
	rdb := mocks.NewMockCmdable(self.T())
	rdb.EXPECT().Subscribe(ctx).RunAndReturn(
		func(ctx context.Context, keys ...string) *redis.PubSub {
			pubsub := self.rdb.Subscribe(ctx, keys...)
			pubsub.Close()
			return pubsub
		})

	r := self.testNew(func(redisCache *RedisCache) { redisCache.rdb = rdb })
	value, err := r.Listen(ctx, testKey)
	self.Require().ErrorContains(err, "subscribe channel")
	self.Empty(value)
}

func (self *RedisCacheTestSuite) TestListen_messageError() {
	ctx := context.Background()
	rdb := mocks.NewMockCmdable(self.T())
	rdb.EXPECT().Subscribe(ctx).RunAndReturn(
		func(ctx context.Context, keys ...string) *redis.PubSub {
			pubsub := self.rdb.Subscribe(ctx, keys...)
			go func() {
				time.Sleep(100 * time.Millisecond)
				pubsub.Close()
			}()
			return pubsub
		})

	r := self.testNew(func(redisCache *RedisCache) { redisCache.rdb = rdb })
	value, err := r.Listen(ctx, testKey)
	self.Require().ErrorContains(err, "pubsub message from channel")
	self.Empty(value)
}
