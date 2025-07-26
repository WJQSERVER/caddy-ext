package ratelimit

import (
	"fmt"
	"time"

	sw "github.com/RussellLuo/slidingwindow"
	lru "github.com/hashicorp/golang-lru/v2"
)

type Zone struct {
	// 使用 lru/v2 的泛型缓存, 类型安全
	limiters *lru.Cache[string, *sw.Limiter]

	rateSize  time.Duration
	rateLimit int64
}

// NewZone 创建一个新的速率限制区域
func NewZone(size int, rateSize time.Duration, rateLimit int64) (*Zone, error) {
	// 使用 lru/v2 的构造函数, 并指定键值类型
	cache, err := lru.New[string, *sw.Limiter](size)
	if err != nil {
		return nil, err
	}
	return &Zone{
		limiters:  cache,
		rateSize:  rateSize,
		rateLimit: rateLimit,
	}, nil
}

// Purge 用于完全清除区域内的所有限速器
func (z *Zone) Purge() {
	z.limiters.Purge()
}

// Allow 检查给定的键是否在速率限制内
func (z *Zone) Allow(key string) bool {
	lim := z.getLimiter(key)
	return lim.Allow()
}

// RateLimitPolicyHeader 生成用于HTTP响应头的速率策略字符串
func (z *Zone) RateLimitPolicyHeader() string {
	return fmt.Sprintf("%d; w=%d", z.rateLimit, int(z.rateSize.Seconds()))
}

// getLimiter 获取或创建与给定键关联的限速器.
// 此实现解决了 v2 中缺少原子 GetOrAdd 方法的问题.
func (z *Zone) getLimiter(key string) *sw.Limiter {
	// 快速路径: 这是一个乐观的检查. `Get` 方法是线程安全的, 并且会更新键的近况.
	if lim, ok := z.limiters.Get(key); ok {
		return lim
	}

	// 慢速路径: 缓存中不存在限速器, 我们需要创建一个.
	newLim, _ := sw.NewLimiter(z.rateSize, z.rateLimit, func() (sw.Window, sw.StopFunc) {
		// NewLocalWindow 返回一个空的 stop 函数, 因此无需在之后调用它.
		return sw.NewLocalWindow()
	})

	// 再次检查: 在我们创建 newLim 的期间, 可能有另一个 goroutine 已经添加了一个限速器.
	// 我们再次调用 Get, 如果成功, 就使用已存在的那个, 以避免覆盖.
	if existingLim, ok := z.limiters.Get(key); ok {
		return existingLim
	}

	// 添加我们新创建的限速器.
	// 注意: 如果多个 goroutine 同时到达这里, `Add` 操作可能会覆盖彼此的结果.
	// 这不是问题, 因为下面的最终 `Get` 会确保所有 goroutine 都获得最终写入缓存的"获胜者".
	z.limiters.Add(key, newLim)

	// 最终确认: 必须再次从缓存中获取, 以确保所有并发的调用者都获得相同的规范实例.
	if lim, ok := z.limiters.Get(key); ok {
		return lim
	}

	// 这个分支在逻辑上几乎不可能到达, 除非缓存大小为1,
	// 且我们刚添加的条目立即被另一个添加不同键的 goroutine 淘汰.
	// 在这种极端情况下, 返回我们创建的实例是安全的回退.
	return newLim
}
