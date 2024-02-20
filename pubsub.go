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

	type message struct {
		Value string
		Err   error
	}
	ch := make(chan message)

	go func() {
		if err := self.subscribe(ctx, pubsub, key, ready...); err != nil {
			ch <- message{Err: err}
			return
		}

		value, err := self.receiveMessage(ctx, pubsub)
		if err != nil {
			err = fmt.Errorf("receive from channel %q: %w", key, err)
		}
		ch <- message{Value: value, Err: err}
	}()

	select {
	case <-ctx.Done():
		pubsub.Close()
	case m := <-ch:
		return m.Value, m.Err
	}

	m := <-ch
	return m.Value, m.Err
}

func (self *RedisCache) subscribe(ctx context.Context, pubsub *redis.PubSub,
	key string, ready ...func() error,
) error {
	if err := pubsub.Subscribe(ctx, key); err != nil {
		return fmt.Errorf("subscribe channel %q: %w", key, err)
	} else if self.subscribed != nil {
		self.subscribed(pubsub)
	}

	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("receive subscription from channel %q: %w", key, err)
	}

	for _, fn := range ready {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (self *RedisCache) receiveMessage(ctx context.Context, pubsub *redis.PubSub,
) (string, error) {
	m, err := pubsub.ReceiveMessage(ctx)
	const errMsg = "pubsub message: %w"
	if ctx.Err() != nil {
		return "", fmt.Errorf(errMsg, context.Cause(ctx))
	} else if err != nil {
		return "", fmt.Errorf(errMsg, err)
	}
	return m.Payload, nil
}
