package autopipe

import (
	"context"
	"time"
)

func (self *AutoPipe) Go(ctx context.Context) {
	self.queue = make(chan *cmdItem, self.maxWeight)
	self.wg.Add(1)
	go self.loop(ctx)
}

func (self *AutoPipe) loop(ctx context.Context) {
	defer self.wg.Done()

	b := self.itemsBuf()
	tick := time.NewTicker(self.flushInterval)
	defer tick.Stop()

	for {
		var wantFlush bool
		select {
		case item := <-self.queue:
			if b.Append(item) >= self.maxWeight {
				wantFlush = true
			}
		case <-ctx.Done():
			wantFlush = true
		case <-tick.C:
			wantFlush = !b.Empty()
		}
		if wantFlush {
			self.flushQueue(ctx, b)
			if ctx.Err() != nil {
				break
			}
			b = self.itemsBuf()
		}
	}
}

func (self *AutoPipe) flushQueue(ctx context.Context, b *itemsBuf) {
	switch {
	case ctx.Err() != nil:
		self.cancelItemsBuf(b, ctx.Err())
	case b.Empty():
		self.freeItemsBuf(b)
	default:
		self.wg.Add(1)
		go func() {
			defer self.wg.Done()
			b.Exec(ctx, self.rdb.Pipeline())
			self.freeItemsBuf(b)
		}()
	}
}

func (self *AutoPipe) Wait() {
	self.wg.Wait()
}
