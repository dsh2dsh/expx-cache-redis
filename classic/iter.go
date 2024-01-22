package classic

import (
	"context"
	"time"
)

func MakeSetIter3(
	ctx context.Context, keys []string, blobs [][]byte, times []time.Duration,
) (context.Context, int, func(itemIdx int) (key string, b []byte, ttl time.Duration)) {
	return ctx, len(keys),
		func(itemIdx int) (key string, b []byte, ttl time.Duration) {
			return keys[itemIdx], blobs[itemIdx], times[itemIdx]
		}
}

func MakeGetIter3(
	ctx context.Context, keys []string,
) (context.Context, int, func(itemIdx int) string) {
	return ctx, len(keys), func(itemIdx int) string { return keys[itemIdx] }
}
