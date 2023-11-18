package autopipe

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func (self *AutoPipe) newItemsBuf() any {
	return &itemsBuf{
		Items: make([]*cmdItem, 0, self.maxWeight),
	}
}

func (self *AutoPipe) itemsBuf() *itemsBuf {
	return self.itemsPool.Get().(*itemsBuf)
}

func (self *AutoPipe) freeItemsBuf(b *itemsBuf) {
	b.Reset()
	self.itemsPool.Put(b)
}

func (self *AutoPipe) cancelItemsBuf(b *itemsBuf, err error) {
	b.Cancel(err)
	self.freeItemsBuf(b)
}

// --------------------------------------------------

type itemsBuf struct {
	Items     []*cmdItem
	extWeight int
}

func (self *itemsBuf) Append(ctx context.Context, item *cmdItem) int {
	self.Items = append(self.Items, item)
	if item.Weight > 1 {
		self.extWeight += item.Weight - 1
	}
	return self.weight()
}

func (self *itemsBuf) weight() int {
	return len(self.Items) + self.extWeight
}

func (self *itemsBuf) Empty() bool {
	return len(self.Items) == 0
}

func (self *itemsBuf) Exec(ctx context.Context, pipe redis.Pipeliner) {
	for _, item := range self.Items {
		item.Exec(pipe)
	}
	cmds, _ := pipe.Exec(ctx)

	var nextItem int
	for _, cmd := range cmds {
		for self.Items[nextItem].Err() != nil {
			nextItem++
		}
		item := self.Items[nextItem]
		item.Process(cmd)
		nextItem++
	}
}

func (self *itemsBuf) Reset() {
	clear(self.Items)
	self.Items = self.Items[:0]
	self.extWeight = 0
}

func (self *itemsBuf) Cancel(err error) {
	for _, item := range self.Items {
		item.Cancel(err)
	}
}

func (self *itemsBuf) WantFlush() bool {
	if item := self.lastItem(); item != nil {
		return item.Ctx.Err() != nil
	}
	return false
}

func (self *itemsBuf) lastItem() *cmdItem {
	if len(self.Items) == 0 {
		return nil
	}
	return self.Items[len(self.Items)-1]
}
