package ratelimit

import (
	"sync"
	"testing"
	"time"

	sw "github.com/RussellLuo/slidingwindow"
)

func TestZone_getLimiter(t *testing.T) {
	zone, _ := NewZone(2, time.Second, 10)
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"

	// 第一次获取 key1
	lim1_first := zone.getLimiter(key1)
	if lim1_first == nil {
		t.Fatalf("Limiter for key1 should not be nil")
	}

	// 第二次获取 key1, 应该返回同一个实例
	lim1_second := zone.getLimiter(key1)
	if lim1_first != lim1_second {
		t.Fatalf("Expected same limiter instance for key1, got %p and %p", lim1_first, lim1_second)
	}

	// 添加 key2
	zone.getLimiter(key2)
	if zone.limiters.Len() != 2 {
		t.Fatalf("Expected cache length 2, got %d", zone.limiters.Len())
	}

	// 添加 key3, 这应该会导致 key1 被淘汰 (因为 key1 是最久未使用的)
	zone.getLimiter(key3)
	if zone.limiters.Len() != 2 {
		t.Fatalf("Expected cache length 2 after eviction, got %d", zone.limiters.Len())
	}

	if zone.limiters.Contains(key1) {
		t.Fatalf("Expected key1 to be evicted from the cache")
	}
	if !zone.limiters.Contains(key2) || !zone.limiters.Contains(key3) {
		t.Fatalf("Expected key2 and key3 to be in the cache")
	}
}

func TestZone_getLimiterConcurrently(t *testing.T) {
	test := func(n int) {
		zone, _ := NewZone(1, time.Second, 10)
		key := "key1"

		limC := make(chan *sw.Limiter, n)
		var wg sync.WaitGroup
		startC := make(chan struct{})

		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startC
				// 调用简化后的 getLimiter
				lim := zone.getLimiter(key)
				limC <- lim
			}()
		}

		// 发送开始信号给所有 goroutine
		close(startC)

		wg.Wait()
		close(limC)

		// 从缓存中获取期望的唯一限速器实例
		wantLim, ok := zone.limiters.Get(key)
		if !ok {
			t.Fatalf("Found no limiter in cache")
		}

		// 检查所有 goroutine 获取到的是否都是这同一个实例
		for lim := range limC {
			if lim != wantLim {
				t.Fatalf("Limiter: got (%p), want (%p)", lim, wantLim)
			}
		}
	}

	cases := []struct {
		name   string
		degree int
	}{
		{"concurrency-degree-8", 8},
		{"concurrency-degree-32", 32},
		{"concurrency-degree-64", 64},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			test(c.degree)
		})
	}
}
