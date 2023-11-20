package autopipe

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestItemsBuf_Cancel(t *testing.T) {
	wantErr := errors.New("expected error")
	var gotErr error

	redisCache := New(mocks.NewMockCmdable(t))
	b := redisCache.newItemsBuf().(*itemsBuf)
	b.Append(&cmdItem{
		Ctx: context.Background(),
		Callback: func(cmd redis.Cmder, canceled error) error {
			gotErr = canceled
			return canceled
		},
	})

	b.Cancel(wantErr)
	require.ErrorIs(t, gotErr, wantErr)
}
