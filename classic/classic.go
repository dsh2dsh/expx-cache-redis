package classic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultBatchSize = 1000

func New(rdb redis.Cmdable) *Classic {
	return &Classic{
		rdb:       rdb,
		batchSize: defaultBatchSize,
	}
}

type Classic struct {
	rdb        redis.Cmdable
	batchSize  int
	refreshTTL time.Duration
}

func (self *Classic) WithBatchSize(size int) *Classic {
	self.batchSize = size
	return self
}

func (self *Classic) WithGetRefreshTTL(ttl time.Duration) *Classic {
	self.refreshTTL = ttl
	return self
}

func (self *Classic) Del(ctx context.Context, keys []string) error {
	for low := 0; low < len(keys); low += self.batchSize {
		high := min(len(keys), low+self.batchSize)
		if err := self.rdb.Del(ctx, keys[low:high]...).Err(); err != nil {
			return fmt.Errorf("redis del: %w", err)
		}
	}
	return nil
}

// --------------------------------------------------

func (self *Classic) Get(ctx context.Context, maxItems int,
	keyIter func(itemIdx int) (key string),
) (func() ([]byte, bool), error) {
	if maxItems == 1 {
		return self.singleGet(ctx, keyIter(0))
	}

	blobs := make([][]byte, 0, maxItems)
	pipe := self.rdb.Pipeline()

	for i := 0; i < maxItems; i++ {
		key := keyIter(i)
		_, err := self.getter(ctx, pipe, key)
		if err != nil {
			return nil, fmt.Errorf("getter get %q: %w", key, err)
		}
		if pipe.Len() == self.batchSize {
			if blobs, err = self.mgetPipeExec(ctx, pipe, blobs); err != nil {
				return nil, err
			}
		}
	}

	blobs, err := self.mgetPipeExec(ctx, pipe, blobs)
	if err != nil {
		return nil, err
	}

	return makeBytesIter(blobs), nil
}

func (self *Classic) singleGet(
	ctx context.Context, key string,
) (func() ([]byte, bool), error) {
	blob, err := self.getter(ctx, self.rdb, key)
	if err != nil && !keyNotFound(err) {
		return nil, fmt.Errorf("getter get %q: %w", key, err)
	}

	var done bool
	return func() (b []byte, ok bool) {
		if !done {
			b, ok, done = blob, true, true
		}
		return
	}, nil
}

//nolint:wrapcheck // wrap it later
func (self *Classic) getter(
	ctx context.Context, rdb redis.Cmdable, key string,
) ([]byte, error) {
	if self.refreshTTL > 0 {
		return rdb.GetEx(ctx, key, self.refreshTTL).Bytes()
	}
	return rdb.Get(ctx, key).Bytes()
}

func (self *Classic) mgetPipeExec(
	ctx context.Context, pipe redis.Pipeliner, blobs [][]byte,
) ([][]byte, error) {
	cmds, err := pipe.Exec(ctx)
	if err != nil && !keyNotFound(err) {
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	for _, cmd := range cmds {
		if b, err := cmdBytes(cmd); err != nil {
			return nil, fmt.Errorf("pipelined: %w", err)
		} else {
			blobs = append(blobs, b)
		}
	}

	return blobs, nil
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

func makeBytesIter(blobs [][]byte) func() ([]byte, bool) {
	var nextItem int
	return func() (b []byte, ok bool) {
		if nextItem < len(blobs) {
			b, ok = blobs[nextItem], true
			nextItem++
		}
		return
	}
}

// --------------------------------------------------

func (self *Classic) Set(
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
		} else if pipe.Len() == self.batchSize {
			if err := self.msetPipeExec(ctx, pipe); err != nil {
				return err
			}
		}
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

func (self *Classic) msetPipeExec(
	ctx context.Context, pipe redis.Pipeliner,
) error {
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}
	return nil
}

// --------------------------------------------------

func (self *Classic) Expire(ctx context.Context, key string, ttl time.Duration,
) (bool, error) {
	ok, err := self.rdb.Expire(ctx, key, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("expire %q, %v: %w", key, ttl, err)
	}
	return ok, nil
}
