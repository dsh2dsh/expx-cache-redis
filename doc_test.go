package redis_test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	redis "github.com/dsh2dsh/expx-cache-redis"
)

func Example() {
	cache, _ := redis.MustNew()
	ctx := context.Background()

	err := cache.Set(ctx, 2, slices.Values([]redis.Item{
		redis.NewItem("key1", []byte("value1"), time.Minute),
		redis.NewItem("key2", []byte("value2"), 2*time.Minute),
	}))
	if err != nil {
		log.Fatal(err)
	}

	iterBytes := cache.Get(ctx, 2, slices.Values([]string{"key1", "key2"}))
	for b, err := range iterBytes {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	}

	err = cache.Del(ctx, []string{"key1", "key2"})
	if err != nil {
		log.Fatal(err)
	}
}
