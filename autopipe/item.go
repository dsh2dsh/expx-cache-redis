package autopipe

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

type cmdItem struct {
	Ctx      context.Context
	Cmd      func(ctx context.Context, pipe redis.Pipeliner) error
	Callback func(cmd redis.Cmder, canceled error) error
	Bytes    []byte

	Batch  bool
	Weight int

	err error
}

func (self *cmdItem) Exec(pipe redis.Pipeliner) {
	if err := self.Ctx.Err(); err != nil {
		self.Cancel(err)
	} else if err := self.Cmd(self.Ctx, pipe); err != nil {
		self.Cancel(err)
	}
}

func (self *cmdItem) Cancel(err error) {
	if err = self.Callback(nil, err); err != nil {
		self.err = err
	}
}

func (self *cmdItem) Process(cmd redis.Cmder) {
	if err := self.Ctx.Err(); err != nil {
		self.Cancel(err)
	} else if err := self.Callback(cmd, nil); err != nil {
		self.err = err
	}
}

func (self *cmdItem) Err() error {
	return self.err
}

// --------------------------------------------------

type cmdItems struct {
	item     cmdItem
	items    []cmdItem
	itemsLen int

	wg sync.WaitGroup
}

func (self *cmdItems) Init(maxItems int) {
	if maxItems > 1 {
		self.items = make([]cmdItem, 0, maxItems)
	}
}

func (self *cmdItems) Append(item cmdItem) *cmdItem {
	if self.len() == 0 {
		self.item = item
	} else {
		self.items = append(self.items, item)
	}
	self.itemsLen++
	self.wg.Add(1)
	return self.last()
}

func (self *cmdItems) len() int { return self.itemsLen }

func (self *cmdItems) Each(fn func(item *cmdItem)) *cmdItems {
	if self.len() > 0 {
		iter := self.Iter()
		for item, ok := iter(); ok; item, ok = iter() {
			fn(item)
		}
	}
	return self
}

func (self *cmdItems) Iter() func() (*cmdItem, bool) {
	var nextItem int
	return func() (item *cmdItem, ok bool) {
		if nextItem < self.len() {
			if nextItem == 0 {
				item, ok = &self.item, true
			} else {
				item, ok = &self.items[nextItem-1], true
			}
			nextItem++
		}
		return
	}
}

func (self *cmdItems) Done() {
	self.wg.Done()
}

func (self *cmdItems) Wait() error {
	if self.len() == 0 {
		return nil
	}
	self.wg.Wait()

	iter := self.Iter()
	for item, ok := iter(); ok; item, ok = iter() {
		if item.Err() != nil {
			return item.Err()
		}
	}

	return nil
}

func (self *cmdItems) EndBatch() {
	if self.len() > 0 {
		self.last().Batch = false
	}
}

func (self *cmdItems) last() *cmdItem {
	if self.len() > 1 {
		return &self.items[len(self.items)-1]
	}
	return &self.item
}

func (self *cmdItems) Bytes() func() ([]byte, bool) {
	iter := self.Iter()
	return func() ([]byte, bool) {
		item, ok := iter()
		return item.Bytes, ok
	}
}
