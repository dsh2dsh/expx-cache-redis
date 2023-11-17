package autopipe

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (self *AutoPipe) Set(
	ctx context.Context, maxItems int,
	iter func(itemIdx int) (key string, b []byte, ttl time.Duration),
) error {
	items := cmdItems{}
	items.Init(maxItems)

	for i := 0; i < maxItems; i++ {
		key, b, ttl := iter(i)
		if len(b) > 0 && ttl > 0 {
			appendSet(ctx, &items, key, b, ttl)
		}
	}

	if err := self.queueItems(&items); err != nil {
		return fmt.Errorf("pipelined set: %w", err)
	}

	return nil
}

func appendSet(ctx context.Context, items *cmdItems, key string, blob []byte,
	ttl time.Duration,
) {
	items.Append(cmdItem{
		Ctx: ctx,
		//nolint:wrapcheck // wrap it later
		Cmd: func(ctx context.Context, pipe redis.Pipeliner) error {
			return pipe.Set(ctx, key, blob, ttl).Err()
		},
		Callback: func(cmd redis.Cmder, canceled error) error {
			defer items.Done()
			return cmdErr(cmd, canceled)
		},
		Batch: true,
	})
}
