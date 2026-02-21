package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

func (self *RedisCacheTestSuite) TestListen() {
	ctx, cancel := context.WithDeadline(self.T().Context(),
		time.Now().Add(5*time.Second))
	defer cancel()

	const foobar = "foobar"
	keyLock := self.resolveKeyLock("test-key")
	self.T().Logf("keyLock=%q", keyLock)

	ch := make(chan struct{})
	go func() {
		<-ch
		self.NoError(self.rdb.Publish(ctx, keyLock, foobar).Err())
		close(ch)
	}()

	r := self.testNew()
	value, err := r.Listen(ctx, keyLock, func() error {
		ch <- struct{}{}
		return nil
	})
	<-ch
	self.Require().NoError(err)
	self.Equal(foobar, value)
}

func (self *RedisCacheTestSuite) TestListen_readyCallbackErr() {
	r := self.testNew()
	keyLock := self.resolveKeyLock("test-key")
	testErr := errors.New("test error")

	ctx, cancel := context.WithDeadline(self.T().Context(),
		time.Now().Add(time.Second))
	defer cancel()

	value, err := r.Listen(ctx, keyLock, func() error {
		return testErr
	})
	self.Require().ErrorIs(err, testErr)
	self.Empty(value)
}

func (self *RedisCacheTestSuite) TestListen_cancel() {
	ctx, cancel := context.WithDeadline(self.T().Context(),
		time.Now().Add(time.Second))
	defer cancel()

	ch := make(chan struct{})
	go func() {
		<-ch
		cancel()
		close(ch)
	}()

	r := self.testNew()
	keyLock := self.resolveKeyLock("test-key")
	value, err := r.Listen(ctx, keyLock, func() error {
		ch <- struct{}{}
		return nil
	})
	<-ch

	self.Require().ErrorIs(err, context.Canceled)
	self.Empty(value)
}

func (self *RedisCacheTestSuite) TestListen_subscribeError() {
	ctx, cancel := context.WithDeadline(self.T().Context(),
		time.Now().Add(time.Second))
	defer cancel()

	rdb := &MoqCmdable{
		SubscribeFunc: func(ctx context.Context, channels ...string) *redis.PubSub {
			pubsub := self.rdb.Subscribe(ctx, channels...)
			pubsub.Close()
			return pubsub
		},
	}

	r := self.testNew(func(redisCache *RedisCache) { redisCache.rdb = rdb })
	keyLock := self.resolveKeyLock("test-key")
	value, err := r.Listen(ctx, keyLock)
	self.Require().ErrorContains(err, "subscribe channel")
	self.Empty(value)
}

func (self *RedisCacheTestSuite) TestListen_subscriptionError() {
	ctx, cancel := context.WithDeadline(self.T().Context(),
		time.Now().Add(time.Second))
	defer cancel()

	r := self.testNew(func(redisCache *RedisCache) {
		redisCache.subscribed = func(pubsub *redis.PubSub) { pubsub.Close() }
	})

	keyLock := self.resolveKeyLock("test-key")
	value, err := r.Listen(ctx, keyLock)
	self.Require().ErrorContains(err, "receive subscription from channel")
	self.Empty(value)
}

func (self *RedisCacheTestSuite) TestListen_messageError() {
	ctx, cancel := context.WithDeadline(self.T().Context(),
		time.Now().Add(time.Second))
	defer cancel()

	ch := make(chan struct{})
	rdb := &MoqCmdable{
		SubscribeFunc: func(ctx context.Context, channels ...string) *redis.PubSub {
			pubsub := self.rdb.Subscribe(ctx, channels...)
			go func() {
				<-ch
				pubsub.Close()
			}()
			return pubsub
		},
	}

	r := self.testNew(func(redisCache *RedisCache) { redisCache.rdb = rdb })
	keyLock := self.resolveKeyLock("test-key")
	value, err := r.Listen(ctx, keyLock, func() error {
		ch <- struct{}{}
		return nil
	})
	self.Require().ErrorContains(err, "pubsub message")
	self.Empty(value)
}
