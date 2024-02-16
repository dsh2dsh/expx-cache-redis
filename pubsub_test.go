package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func (self *RedisCacheTestSuite) TestListen() {
	const foobar = "foobar"
	ctx := context.Background()

	ch := make(chan struct{})
	go func() {
		<-ch
		self.NoError(self.rdb.Publish(ctx, testKey, foobar).Err())
		ch <- struct{}{}
	}()

	r := self.testNew()
	value, err := r.Listen(ctx, testKey, func() error {
		ch <- struct{}{}
		return nil
	})
	<-ch
	self.Require().NoError(err)
	self.Equal(foobar, value)
}

func (self *RedisCacheTestSuite) TestListen_readyCallbackErr() {
	testErr := errors.New("test error")
	r := self.testNew()
	value, err := r.Listen(context.Background(), testKey, func() error {
		return testErr
	})
	self.Require().ErrorIs(err, testErr)
	self.Empty(value)
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
	self.Require().ErrorContains(err, "pubsub message")
	self.Empty(value)
}
