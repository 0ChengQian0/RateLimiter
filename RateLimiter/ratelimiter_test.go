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

	// 设置限流配置：每秒产生 200 个令牌
	err := limiter.SetRate(ctx, "api1", 200.0)
	assert.NoError(t, err)

	// 测试正常请求
	allowed := limiter.Allow(ctx, "api1")
	assert.True(t, allowed, "首次请求应该被允许")

	// 获取当前令牌数
	tokens, lastRefresh, err := limiter.GetTokens(ctx, "api1")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 199.0, tokens, "应该剩余至少199个令牌")
	assert.Greater(t, lastRefresh, int64(0))
}

// 测试初始令牌限制、令牌耗尽时的限流功能以及令牌的自动恢复机制
func TestRateLimiter_RateLimit(t *testing.T) {
	// 初始化设置
	client := setupRedis(t)
	defer client.Close()
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 设置限流配置：每秒产生 10 个令牌
	err := limiter.SetRate(ctx, "api2", 10.0)
	assert.NoError(t, err)

	// 连续发送 5 个请求，第 4 和第 5 个应该被限流
	for i := 0; i < 12; i++ {
		if i < 10 {
			assert.True(t, limiter.Allow(ctx, "api2"), fmt.Sprintf("请求%d应该通过", i+1))
		} else {
			assert.False(t, limiter.Allow(ctx, "api2"), fmt.Sprintf("请求%d应该通过", i+1))
		}
	}

	// 等待 1 秒，应该有新的令牌产生
	time.Sleep(1000 * time.Millisecond)
	assert.True(t, limiter.Allow(ctx, "api2"), "等待后的请求应该通过")
}

// 测试并发请求下的限流效果
func TestRateLimiter_Concurrent(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 设置限流配置：每秒产生 5000 个令牌
	err := limiter.SetRate(ctx, "api3", 5000.0)
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
	fmt.Printf("实际通过的请求数量：%d\n", successCount)
	assert.LessOrEqual(t, int(successCount), 6000, "通过的请求数不应超过令牌桶容量")
}

// 测试动态调整限流配置的功能
func TestRateLimiter_DynamicRate(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	// 初始限流设置
	err := limiter.SetRate(ctx, "api4", 10.0)
	assert.NoError(t, err)

	// 消耗所有令牌
	for i := 0; i < 10; i++ {
		assert.True(t, limiter.Allow(ctx, "api4"))
	}
	assert.False(t, limiter.Allow(ctx, "api4"))

	// 动态调整限流阈值
	err = limiter.SetRate(ctx, "api4", 2000.0)
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

	// 初始配置：每毫秒生成0.005个令牌（相当于每秒5个）
	err := limiter.SetRate(ctx, "api5", 5.0)
	assert.NoError(t, err)

	// 消耗3个令牌
	for i := 0; i < 3; i++ {
		assert.True(t, limiter.Allow(ctx, "api5"), "前3个请求应该通过")
	}

	// 获取当前令牌数
	tokens, _, err := limiter.GetTokens(ctx, "api5")
	fmt.Printf("初始最大容量为 5 的令牌桶, 消耗完 3 个令牌之后, 当前令牌数：%.3f\n, 当前之间为 %d ms\n", tokens, time.Now().UnixMilli())
	assert.NoError(t, err)
	assert.LessOrEqual(t, 2.0, tokens, "应该剩余至少2个令牌")

	// 修改配置：每毫秒生成0.01个令牌（相当于每秒10个）
	err = limiter.SetRate(ctx, "api5", 10.0)
	tokens, _, err = limiter.GetTokens(ctx, "api5")
	fmt.Printf("令牌桶更新时间 %d ms, 当前桶内的令牌数为 %.3f \n", time.Now().UnixMilli(), tokens)
	assert.NoError(t, err)

	// 等待0.1秒，应该生成1个新令牌
	time.Sleep(100 * time.Millisecond)

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

// TestRateLimiter_DecreaseRate 测试 QPS 降低时是否正确限流
func TestRateLimiter_DecreaseRate(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()
	ctx := context.Background()
	limiter := NewRateLimiter(client, "test")

	startTime := time.Now().UnixMilli()
	fmt.Printf("[+0 ms] 函数开始执行\n")

	// 初始配置：每秒10个令牌
	err := limiter.SetRate(ctx, "api6", 10.0)
	assert.NoError(t, err)

	// 等待令牌桶装满
	tokens, _, err := limiter.GetTokens(ctx, "api6")
	fmt.Printf("[+%d ms] 初始令牌数：%.3f\n", time.Now().UnixMilli()-startTime, tokens)
	assert.NoError(t, err)

	// 快速消耗8个令牌
	successCount := 0
	for i := 0; i < 2; i++ {
		if limiter.Allow(ctx, "api6") {
			successCount++
		}
	}
	assert.Equal(t, 2, successCount, "应该通过8个请求")

	tokens, _, err = limiter.GetTokens(ctx, "api6")
	fmt.Printf("[+%d ms] 消耗2个令牌后，剩余令牌数：%.3f\n", time.Now().UnixMilli()-startTime, tokens)

	// 降低速率：从每秒10个降至每秒5个
	err = limiter.SetRate(ctx, "api6", 5.0)
	assert.NoError(t, err)

	// 立即获取令牌数，验证最大令牌数是否已调整
	tokens, _, err = limiter.GetTokens(ctx, "api6")
	fmt.Printf("[+%d ms] 降低速率后，当前令牌数：%.3f\n", time.Now().UnixMilli()-startTime, tokens)
	assert.NoError(t, err)
	assert.Equal(t, tokens, 5.0, "令牌数不应超过新的最大值")
}
