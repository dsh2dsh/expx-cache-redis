package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func (self *RedisCache) Listen(ctx context.Context, key string,
	ready ...func() error,
) (string, error) {
	pubsub := self.rdb.Subscribe(ctx)
	defer pubsub.Close()
	if err := pubsub.Subscribe(ctx, key); err != nil {
		return "", fmt.Errorf("subscribe channel %q: %w", key, err)
	}

	for _, fn := range ready {
		if err := fn(); err != nil {
			return "", err
		}
	}

	msg, err := self.receiveMessage(ctx, pubsub)
	if err != nil {
		err = fmt.Errorf("receive from channel %q: %w", key, err)
	}
	return msg, err
}

func (self *RedisCache) receiveMessage(ctx context.Context, pubsub *redis.PubSub,
) (string, error) {
	type message struct {
		*redis.Message
		Err error
	}
	ch := make(chan message)

	const errMsg = "pubsub message: %w"
	go func() {
		m, err := pubsub.ReceiveMessage(ctx)
		if ctx.Err() != nil {
			err = fmt.Errorf(errMsg, context.Cause(ctx))
		} else if err != nil {
			err = fmt.Errorf(errMsg, err)
		}
		ch <- message{Message: m, Err: err}
	}()

	payload := func(m message) (string, error) {
		if m.Err != nil {
			return "", m.Err
		}
		return m.Payload, nil
	}

	select {
	case <-ctx.Done():
		pubsub.Close()
	case m := <-ch:
		return payload(m)
	}
	return payload(<-ch)
}
