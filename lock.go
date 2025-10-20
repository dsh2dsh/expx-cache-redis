package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (self *RedisCache) LockGet(ctx context.Context, keySet, value string,
	ttl time.Duration, keyGet string,
) (ok bool, b []byte, err error) {
	cmds, err := self.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		//nolint:wrapcheck // wrap it outside of Pipelined()
		if err := pipe.SetNX(ctx, keySet, value, ttl).Err(); err != nil {
			return err
		}
		_, err := self.getter(ctx, pipe, keyGet)
		return err
	})

	const errMsg = "setnx %q = %q, get %q: %w"
	if err != nil && !keyNotFound(err) {
		err = fmt.Errorf(errMsg, keySet, value, keyGet, err)
	} else if ok, b, err = cmdBoolBytes(cmds); err != nil {
		err = fmt.Errorf(errMsg, keySet, value, keyGet, err)
	}
	return ok, b, err
}

func cmdBoolBytes(cmds []redis.Cmder) (ok bool, b []byte, err error) {
	if len(cmds) < 2 {
		err = fmt.Errorf("unexpected length of cmds: %v", len(cmds))
		return ok, b, err
	} else if ok, err = cmdBool(cmds[0]); err != nil {
		return ok, b, err
	} else if b, err = cmdBytes(cmds[1]); err != nil {
		return ok, b, err
	}
	return ok, b, err
}

func cmdBool(cmd redis.Cmder) (bool, error) {
	if boolCmd, ok := cmd.(interface{ Result() (bool, error) }); ok {
		if ok, err := boolCmd.Result(); err != nil {
			return false, fmt.Errorf("result %q: %w", cmd.Name(), err)
		} else {
			return ok, nil
		}
	}
	return false, fmt.Errorf("result %q: unexpected type=%T", cmd.Name(), cmd)
}

// --------------------------------------------------

func (self *RedisCache) Expire(ctx context.Context, key string, ttl time.Duration,
) (bool, error) {
	ok, err := self.rdb.Expire(ctx, key, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("expire %q, %v: %w", key, ttl, err)
	}
	return ok, nil
}

func (self *RedisCache) Unlock(ctx context.Context, key, value string,
) (bool, error) {
	n, err := unlockLua.Run(ctx, self.rdb, []string{key}, value).Int64()
	if err != nil {
		return false, fmt.Errorf("delete %q with value %q: %w", key, value, err)
	}
	return n == 1, nil
}

var unlockLua = redis.NewScript(`
if redis.call( "get", KEYS[1] ) == ARGV[1] then
  local deleted = redis.call( "del", KEYS[1] )
  if deleted > 0 then
    redis.call( "publish", KEYS[1], ARGV[1] )
  end
  return deleted
end
return 0`)
