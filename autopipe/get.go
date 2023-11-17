package autopipe

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func (self *AutoPipe) Get(ctx context.Context, maxItems int,
	keyIter func(itemIdx int) (key string),
) (func() ([]byte, bool), error) {
	items := cmdItems{}
	items.Init(maxItems)

	for i := 0; i < maxItems; i++ {
		self.appendGet(ctx, &items, keyIter(i))
	}

	if err := self.queueItems(&items); err != nil {
		return nil, fmt.Errorf("pipelined get: %w", err)
	}

	return items.Bytes(), nil
}

func (self *AutoPipe) appendGet(ctx context.Context, items *cmdItems,
	key string,
) {
	item := items.Append(cmdItem{
		Ctx: ctx,
		Cmd: func(ctx context.Context, pipe redis.Pipeliner) error {
			return self.getter(ctx, pipe, key)
		},
		Batch: true,
	})
	item.Callback = func(cmd redis.Cmder, canceled error) error {
		defer items.Done()
		b, err := cmdBytes(cmd, canceled)
		if err != nil {
			return err
		}
		item.Bytes = b
		return nil
	}
}

//nolint:wrapcheck // wrap it later
func (self *AutoPipe) getter(
	ctx context.Context, rdb redis.Cmdable, key string,
) error {
	if self.refreshTTL > 0 {
		return rdb.GetEx(ctx, key, self.refreshTTL).Err()
	}
	return rdb.Get(ctx, key).Err()
}
