package autopipe_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dsh2dsh/expx-cache/redis/autopipe"
)

func Example() {
	redisCache := autopipe.MustNew()
	ctx, cancel := context.WithCancel(context.Background())
	redisCache.Go(ctx)

	err := redisCache.Set(autopipe.MakeSetIter3(ctx, []string{"key1", "key2"},
		[][]byte{[]byte("value1"), []byte("value2")},
		[]time.Duration{time.Minute, 2 * time.Minute}))
	if err != nil {
		log.Fatal(err)
	}

	iterBytes, err := redisCache.Get(
		autopipe.MakeGetIter3(ctx, []string{"key1", "key2"}))
	if err != nil {
		log.Fatal(err)
	}
	for b, ok := iterBytes(); ok; b, ok = iterBytes() {
		fmt.Println(string(b))
	}

	err = redisCache.Del(ctx, []string{"key1", "key2"})
	if err != nil {
		log.Fatal(err)
	}

	cancel()
	redisCache.Wait()
}
