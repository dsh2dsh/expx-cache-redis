package redis_test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/dsh2dsh/expx-cache/model"

	cacheRedis "github.com/dsh2dsh/expx-cache-redis"
)

func Example() {
	redisCache, _ := cacheRedis.MustNew()
	ctx := context.Background()

	err := redisCache.Set(ctx, 2, slices.Values([]model.RedisItem{
		model.NewRedisItem("key1", []byte("value1"), time.Minute),
		model.NewRedisItem("key2", []byte("value2"), 2*time.Minute),
	}))
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
