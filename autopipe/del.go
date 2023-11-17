package autopipe

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func (self *AutoPipe) Del(ctx context.Context, keys []string) error {
	maxItems := len(keys) / self.maxWeight
	if len(keys)%self.maxWeight > 0 {
		maxItems++
	}
	items := cmdItems{}
	items.Init(maxItems)

	if len(keys) <= self.maxWeight {
		appendDel(ctx, &items, keys)
	} else {
		for low := 0; low < len(keys); low += self.maxWeight {
			high := min(len(keys), low+self.maxWeight)
			appendDel(ctx, &items, keys[low:high])
		}
	}

	if err := self.queueItems(&items); err != nil {
		return fmt.Errorf("pipelined del: %w", err)
	}
	return nil
}

func appendDel(ctx context.Context, items *cmdItems, keys []string) {
	items.Append(cmdItem{
		Ctx: ctx,
		//nolint:wrapcheck // wrap it later
		Cmd: func(ctx context.Context, pipe redis.Pipeliner) error {
			return pipe.Del(ctx, keys...).Err()
		},
		Callback: func(cmd redis.Cmder, canceled error) error {
			defer items.Done()
			return cmdErr(cmd, canceled)
		},
		Weight: len(keys),
	})
}
