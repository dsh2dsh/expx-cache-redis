package redis

import "time"

func NewItem(key string, value []byte, ttl time.Duration) Item {
	return Item{key: key, value: value, ttl: ttl}
}

type Item struct {
	key   string
	value []byte
	ttl   time.Duration
}
