package ratelimiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// 初始化 redis 客户端, 创建并且验证 redis 连接
func setupRedis(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// 测试 Redis 连接
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	assert.NoError(t, err, "Redis 连接失败")

	return client
}

// 测试限流器的基本功能
func TestRateLimiter_Basic(t *testing.T) {
	// 初始化 redis 客户端
	client := setupRedis(t)
	defer client.Close()

	// 创建上下文和限流器, 使用 test 作为 key 前缀
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 设置限流配置：最大 2000 个令牌，每毫秒产生 2 个令牌
	err := limiter.SetRate(ctx, "api1", 200.0, 0.2)
	assert.NoError(t, err)

	// 测试正常请求
	allowed := limiter.Allow(ctx, "api1")
	assert.True(t, allowed, "首次请求应该被允许")

	// 获取当前令牌数
	tokens, lastRefresh, err := limiter.GetTokens(ctx, "api1")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, 199.0, tokens, "应该剩余至少199个令牌")
	assert.Greater(t, lastRefresh, int64(0))
}

// 测试初始令牌限制、令牌耗尽时的限流功能以及令牌的自动恢复机制
func TestRateLimiter_RateLimit(t *testing.T) {
	// 初始化设置
	client := setupRedis(t)
	defer client.Close()
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 设置限流配置：最大 3 个令牌，每毫秒产生 0.001 个令牌
	err := limiter.SetRate(ctx, "api2", 3.0, 0.001)
	assert.NoError(t, err)

	// 连续发送 5 个请求，第 4 和第 5 个应该被限流
	for i := 0; i < 5; i++ {
		if i < 3 {
			assert.True(t, limiter.Allow(ctx, "api2"), fmt.Sprintf("请求%d应该通过", i+1))
		} else {
			assert.False(t, limiter.Allow(ctx, "api2"), fmt.Sprintf("请求%d应该通过", i+1))
		}
	}

	// 等待 200 毫秒，应该有新的令牌产生
	time.Sleep(1000 * time.Millisecond)
	assert.True(t, limiter.Allow(ctx, "api2"), "等待后的请求应该通过")
}

// 测试并发请求下的限流效果
func TestRateLimiter_Concurrent(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 设置限流配置：最大 5000 个令牌，每毫秒产生 1 个令牌
	err := limiter.SetRate(ctx, "api3", 5000.0, 1.0)
	assert.NoError(t, err)

	// 并发测试, 使用 sync.WaitGroup 控制并发
	var wg sync.WaitGroup
	successCount := int32(0)
	totalRequests := 10000

	// 启动100个并发请求
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.Allow(ctx, "api3") {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	// 验证通过的请求数应该小于等于初始令牌数
	assert.LessOrEqual(t, int(successCount), 6000, "通过的请求数不应超过令牌桶容量")
}

// 测试动态调整限流配置的功能
func TestRateLimiter_DynamicRate(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 初始限流设置
	err := limiter.SetRate(ctx, "api4", 1500.0, 0)
	assert.NoError(t, err)

	// 消耗所有令牌
	for i := 0; i < 1500; i++ {
		assert.True(t, limiter.Allow(ctx, "api4"))
	}
	assert.False(t, limiter.Allow(ctx, "api4"))

	// 动态调整限流阈值
	err = limiter.SetRate(ctx, "api4", 2500.0, 2.0)
	assert.NoError(t, err)

	// 等待令牌重新生成
	time.Sleep(2 * time.Millisecond)

	// 验证新的限流规则是否生效
	tokens, _, err := limiter.GetTokens(ctx, "api4")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, tokens, 4.0, "应该至少生成4个新令牌")
}

// TestRateLimiter_TokenRateChange 测试修改令牌生成速率后的限流效果
func TestRateLimiter_TokenRateChange(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	fmt.Printf("函数开始执行的时间：%d ms\n", time.Now().UnixMilli())

	// 初始配置：最大5个令牌，每毫秒生成0.001个令牌（相当于每秒1个）
	err := limiter.SetRate(ctx, "api5", 5.0, 0.001)
	assert.NoError(t, err)

	// 消耗3个令牌
	for i := 0; i < 3; i++ {
		assert.True(t, limiter.Allow(ctx, "api5"), "前3个请求应该通过")
	}

	// 获取当前令牌数
	tokens, _, err := limiter.GetTokens(ctx, "api5")
	fmt.Printf("初始最大容量为 5 的令牌桶, 消耗完 3 个令牌之后, 当前令牌数：%.3f\n, 当前之间为 %d ms\n", tokens, time.Now().UnixMilli())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, 2.0, tokens, "应该剩余至少2个令牌")

	// 修改配置：最大10个令牌，每毫秒生成0.002个令牌（相当于每秒2个）
	err = limiter.SetRate(ctx, "api5", 10.0, 0.002)
	tokens, _, err = limiter.GetTokens(ctx, "api5")
	fmt.Printf("令牌桶更新时间 %d ms, 当前桶内的令牌数为 %.3f \n", time.Now().UnixMilli(), tokens)
	assert.NoError(t, err)

	// 等待0.5秒，应该生成1个新令牌
	time.Sleep(500 * time.Millisecond)

	// 验证新令牌生成
	tokens, _, err = limiter.GetTokens(ctx, "api5")
	fmt.Printf("当前时间为 %d ms, 当前令牌数：%.3f\n", time.Now().UnixMilli(), tokens)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, tokens, 3.0, "应该至少有3个令牌")

	// 尝试快速消耗6个令牌，应该只能消耗5个
	successCount := 0
	for i := 0; i < 7; i++ {
		if limiter.Allow(ctx, "api5") {
			successCount++
		}
	}
	tokens, _, err = limiter.GetTokens(ctx, "api5")
	fmt.Printf("当前令牌数：%.3f\n", tokens)
	assert.Equal(t, 3, successCount, "应该通过3个请求")
}
