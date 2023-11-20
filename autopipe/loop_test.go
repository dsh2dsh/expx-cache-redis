package autopipe

import (
	"context"
	"testing"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestAutoPipe_flushQueue_empty(t *testing.T) {
	b := itemsBuf{Items: make([]*cmdItem, 0)}
	New(mocks.NewMockCmdable(t)).flushQueue(context.Background(), &b)
}
