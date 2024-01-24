package classic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
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

func (self *ClassicTestSuite) TestExpire() {
	ctx := context.Background()
	ttl := time.Minute

	r := self.testNew()
	ok, err := r.Expire(ctx, testKey, ttl)
	self.Require().NoError(err)
	self.False(ok)

	self.setKey(r, testKey, []byte("foobar"), ttl)
	ok, err = r.Expire(ctx, testKey, ttl)
	self.Require().NoError(err)
	self.True(ok)

	gotTTL, err := self.rdb.TTL(ctx, testKey).Result()
	self.Require().NoError(err)
	self.Equal(ttl, gotTTL)

	ttl2 := 2 * time.Minute
	ok, err = r.Expire(ctx, testKey, ttl2)
	self.Require().NoError(err)
	self.True(ok)

	gotTTL, err = self.rdb.TTL(ctx, testKey).Result()
	self.Require().NoError(err)
	self.Equal(ttl2, gotTTL)
}

func TestExpire_error(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	r := New(rdb)

	rdb.EXPECT().Expire(ctx, testKey, ttl).Return(redis.NewBoolResult(false, wantErr))
	ok, err := r.Expire(ctx, testKey, ttl)
	require.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}

func (self *ClassicTestSuite) TestDeleteWithValue() {
	const foobar = "foobar"
	ctx := context.Background()
	ttl := time.Minute

	r := self.testNew()
	ok, err := r.DeleteWithValue(ctx, testKey, foobar)
	self.Require().NoError(err)
	self.False(ok)

	self.setKey(r, testKey, []byte(foobar), ttl)
	ok, err = r.DeleteWithValue(ctx, testKey, "foobaz")
	self.Require().NoError(err)
	self.False(ok)

	ok, err = r.DeleteWithValue(ctx, testKey, foobar)
	self.Require().NoError(err)
	self.True(ok)
}

func TestDeleteWithValue_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	r := New(rdb)

	rdb.EXPECT().EvalSha(ctx, mock.Anything, []string{testKey}, mock.Anything).
		Return(redis.NewCmdResult(0, wantErr))
	ok, err := r.DeleteWithValue(ctx, testKey, "foobar")
	require.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}
