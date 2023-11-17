package autopipe

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func New(rdb redis.Cmdable) *AutoPipe {
	r := &AutoPipe{
		maxWeight: 1000,

		rdb: rdb,
	}
	r.itemsPool = sync.Pool{New: r.newItemsBuf}
	return r
}

type AutoPipe struct {
	maxWeight  int
	refreshTTL time.Duration

	itemsPool sync.Pool
	queue     chan *cmdItem
	wg        sync.WaitGroup

	rdb redis.Cmdable
}

func (self *AutoPipe) WithGetRefreshTTL(ttl time.Duration) *AutoPipe {
	self.refreshTTL = ttl
	return self
}

func (self *AutoPipe) WithMaxWeight(w int) *AutoPipe {
	self.maxWeight = w
	return self
}

func (self *AutoPipe) queueItems(items *cmdItems) error {
	items.EndBatch()
	err := items.Each(func(item *cmdItem) { self.queue <- item }).Wait()
	if err != nil {
		return err
	}
	return nil
}

// --------------------------------------------------

func cmdBytes(cmd redis.Cmder, canceled error) ([]byte, error) {
	if canceled != nil {
		return nil, canceled
	}

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

//nolint:wrapcheck // wrap it later
func cmdErr(cmd redis.Cmder, canceled error) error {
	if canceled != nil {
		return canceled
	} else if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}
