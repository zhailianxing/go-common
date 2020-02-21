package disRedis

import (
	"github.com/go-redis/redis"
	"sync"
	"time"
)

type RedisLocker struct {
	client     *redis.Client // Redis 客户端
	expiration time.Duration // Redis 缓存的有效期
	retries    int           // 获取锁的重试次数
	interval   time.Duration // 获取锁的重试间隔
	locker     sync.Mutex
}

func NewRedisLocker(client *redis.Client, expiration time.Duration, retries int, interval time.Duration) *RedisLocker {
	locker := &RedisLocker{client: client, locker: sync.Mutex{}}
	locker.SetExpiration(expiration)
	locker.SetRetries(retries)
	locker.SetInterval(interval)
	return locker
}

func (locker *RedisLocker) SetExpiration(expiration time.Duration) *RedisLocker {
	if expiration < 0 {
		locker.expiration = 0
	} else {
		locker.expiration = expiration
	}

	return locker
}

func (locker *RedisLocker) SetRetries(retries int) *RedisLocker {
	if retries < 0 {
		locker.retries = 0
	} else {
		locker.retries = retries
	}

	return locker
}

func (locker *RedisLocker) SetInterval(interval time.Duration) *RedisLocker {
	// The minimum retry interval is 5 milliseconds.
	min := time.Millisecond * 5

	if interval < min {
		locker.interval = min
	} else {
		locker.interval = interval
	}

	return locker
}

func (locker *RedisLocker) Lock(key string) bool {
	locker.locker.Lock()
	defer locker.locker.Unlock()

	if locker.client != nil {
		for i := 0; i <= locker.retries; i++ {
			if ok, err := locker.client.SetNX(key, 1, locker.expiration).Result(); err == nil && ok {
				return true
			} else {
				time.Sleep(locker.interval)
			}
		}
	}

	return false
}

func (locker *RedisLocker) Unlock(key string) bool {
	locker.locker.Lock()
	defer locker.locker.Unlock()

	if locker.client != nil {
		for i := 0; i <= locker.retries; i++ {
			if err := locker.client.Del(key).Err(); err == nil {
				return true
			} else {
				time.Sleep(locker.interval)
			}
		}
	}

	return false
}

/*usage :
	redisCLient := global.GetRedisClient(global.PROJECT_NAME)
	lockKey := fmt.Sprintf("user_get_award:%d", uid)
	locker := NewRedisLocker(redisCLient, 2*time.Second, 3, 100*time.Microsecond)
	if locker.Lock(lockKey) {
		defer locker.Unlock(lockKey)
		// do your logic things
	} else {
		log.Fatal("get redis lock error")
		return "活动太火爆了"
	}
}
*/
