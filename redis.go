package redis

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultBatchSize = 1000

type Cmdable interface {
	redis.Cmdable

	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

func New(rdb Cmdable) *RedisCache {
	return &RedisCache{
		rdb:       rdb,
		batchSize: defaultBatchSize,
	}
}

type RedisCache struct {
	rdb        Cmdable
	batchSize  int
	refreshTTL time.Duration

	subscribed func(pubsub *redis.PubSub)
}

func (self *RedisCache) WithBatchSize(size int) *RedisCache {
	self.batchSize = size
	return self
}

func (self *RedisCache) WithGetRefreshTTL(ttl time.Duration) *RedisCache {
	self.refreshTTL = ttl
	return self
}

func (self *RedisCache) Del(ctx context.Context, keys []string) error {
	for batch := range slices.Chunk(keys, self.batchSize) {
		if err := self.rdb.Del(ctx, batch...).Err(); err != nil {
			return fmt.Errorf("redis del: %w", err)
		}
	}
	return nil
}

// --------------------------------------------------

func (self *RedisCache) Get(ctx context.Context, maxItems int,
	keys iter.Seq[string],
) iter.Seq2[[]byte, error] {
	if maxItems == 1 {
		for key := range keys {
			return self.singleGet(ctx, key)
		}
	}

	return func(yield func([]byte, error) bool) {
		pipe := self.rdb.Pipeline()
		for key := range keys {
			_, err := self.getter(ctx, pipe, key)
			if err != nil {
				yield(nil, fmt.Errorf("getter get %q: %w", key, err))
				return
			} else if pipe.Len() < self.batchSize {
				continue
			}

			for b, err := range self.mgetPipeExec(ctx, pipe) {
				if !yield(b, err) {
					return
				}
			}
		}

		if pipe.Len() == 0 {
			return
		}

		for b, err := range self.mgetPipeExec(ctx, pipe) {
			if !yield(b, err) {
				return
			}
		}
	}
}

func (self *RedisCache) singleGet(ctx context.Context, key string,
) iter.Seq2[[]byte, error] {
	blob, err := self.getter(ctx, self.rdb, key)
	return func(yield func([]byte, error) bool) {
		if err != nil && !keyNotFound(err) {
			yield(nil, fmt.Errorf("getter get %q: %w", key, err))
		} else {
			yield(blob, nil)
		}
	}
}

//nolint:wrapcheck // wrap it later
func (self *RedisCache) getter(ctx context.Context, rdb redis.Cmdable,
	key string,
) ([]byte, error) {
	if self.refreshTTL > 0 {
		return rdb.GetEx(ctx, key, self.refreshTTL).Bytes()
	}
	return rdb.Get(ctx, key).Bytes()
}

func (self *RedisCache) mgetPipeExec(ctx context.Context, pipe redis.Pipeliner,
) iter.Seq2[[]byte, error] {
	cmds, err := pipe.Exec(ctx)
	return func(yield func([]byte, error) bool) {
		if err != nil && !keyNotFound(err) {
			yield(nil, fmt.Errorf("pipeline: %w", err))
			return
		}

		for _, cmd := range cmds {
			if b, err := cmdBytes(cmd); err != nil {
				yield(nil, fmt.Errorf("pipelined: %w", err))
				return
			} else if !yield(b, nil) {
				return
			}
		}
	}
}

func cmdBytes(cmd redis.Cmder) ([]byte, error) {
	if strCmd, ok := cmd.(interface{ Bytes() ([]byte, error) }); ok {
		if b, err := strCmd.Bytes(); err == nil {
			return b, nil
		} else if !keyNotFound(err) {
			return nil, fmt.Errorf("bytes %q: %w", cmd.Name(), err)
		}
		return nil, nil
	}

	return nil, fmt.Errorf("bytes %q: unexpected type=%T", cmd.Name(), cmd)
}

func keyNotFound(err error) bool {
	return err != nil && errors.Is(err, redis.Nil)
}

// --------------------------------------------------

func (self *RedisCache) Set(
	ctx context.Context, maxItems int,
	iter func(itemIdx int) (key string, b []byte, ttl time.Duration),
) error {
	if maxItems == 1 {
		key, b, ttl := iter(0)
		return singleSet(ctx, self.rdb, key, b, ttl)
	}

	pipe := self.rdb.Pipeline()
	for i := 0; i < maxItems; i++ {
		key, b, ttl := iter(i)
		if err := singleSet(ctx, pipe, key, b, ttl); err != nil {
			return err
		} else if pipe.Len() < self.batchSize {
			continue
		}
		if err := self.msetPipeExec(ctx, pipe); err != nil {
			return err
		}
	}

	if pipe.Len() == 0 {
		return nil
	}
	return self.msetPipeExec(ctx, pipe)
}

func singleSet(ctx context.Context, pipe redis.Cmdable, key string, b []byte,
	ttl time.Duration,
) error {
	if len(b) == 0 {
		return nil
	}
	if err := pipe.Set(ctx, key, b, ttl).Err(); err != nil {
		return fmt.Errorf("pipelined set: %w", err)
	}
	return nil
}

func (self *RedisCache) msetPipeExec(ctx context.Context, pipe redis.Pipeliner,
) error {
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}
	return nil
}
