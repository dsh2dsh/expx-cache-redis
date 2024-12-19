package redis_test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/dsh2dsh/expx-cache/redis"
)

func Example() {
	redisCache, _ := redis.MustNew()
	ctx := context.Background()

	err := redisCache.Set(redis.MakeSetIter3(ctx, []string{"key1", "key2"},
		[][]byte{[]byte("value1"), []byte("value2")},
		[]time.Duration{time.Minute, 2 * time.Minute}))
	if err != nil {
		log.Fatal(err)
	}

	iterBytes := redisCache.Get(ctx, 2, slices.Values([]string{"key1", "key2"}))
	for b, err := range iterBytes {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	}

	err = redisCache.Del(ctx, []string{"key1", "key2"})
	if err != nil {
		log.Fatal(err)
	}
}
