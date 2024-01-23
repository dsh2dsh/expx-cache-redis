package classic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestClassic_SetNxGet_errors(t *testing.T) {
	const keySet = "key1"
	const keyGet = "key2"
	const bar = "bar"
	ctx := context.Background()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	r := New(rdb)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return(nil, wantErr).Once()
	_, _, err := r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	pipe := mocks.NewMockPipeliner(t)
	pipe.EXPECT().SetNX(ctx, keySet, bar, ttl).Return(
		redis.NewBoolResult(false, wantErr)).Once()
	rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
		func(ctx context.Context, fn func(redis.Pipeliner) error,
		) ([]redis.Cmder, error) {
			return nil, fn(pipe)
		}).Once()
	_, _, err = r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{}, nil).Once()
	_, _, err = r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.Error(t, err)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewBoolResult(false, wantErr),
		redis.NewStringResult("", wantErr),
	}, nil).Once()
	_, _, err = r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewBoolResult(false, nil),
		redis.NewStringResult("", wantErr),
	}, nil).Once()
	_, _, err = r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.ErrorIs(t, err, wantErr)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewStringResult("", wantErr),
		redis.NewStringResult("", wantErr),
	}, nil).Once()
	_, _, err = r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.Error(t, err)

	rdb.EXPECT().Pipelined(ctx, mock.Anything).Return([]redis.Cmder{
		redis.NewBoolResult(false, nil),
		redis.NewStringResult("", nil),
	}, nil).Once()
	_, _, err = r.SetNxGet(ctx, keySet, bar, ttl, keyGet)
	require.NoError(t, err)
}
