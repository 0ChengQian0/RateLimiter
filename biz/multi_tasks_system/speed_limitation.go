package multi_tasks_system

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type RateLimiter struct {
	client    *redis.Client // redis 客户端
	keyPrefix string        // redis key 的前缀
}

// NewRateLimiter 创建一个新的限流器
func NewRateLimiter(c *redis.Client, key_prefix string) *RateLimiter {
	return &RateLimiter{
		client:    c,
		keyPrefix: key_prefix,
	}
}

// SetRate 设置指定 key 的限流阈值
func (rl *RateLimiter) SetRate(ctx context.Context, key string, qps float64) error {
	// 使用 redis pipline 来原子性地更新限流配置
	// rate 表示每毫秒生成的令牌数量, 原来的 rate 是每毫秒生成整数个 token, 现在支持小数级别的 rate
	rateKey := rl.keyPrefix + ":" + key + ":rate"
	maxTokensKey := rl.keyPrefix + ":" + key + ":max_tokens"

	// 根据 qps 计算 token 生成速率和最大令牌数量
	tokenRate := qps / 1000
	maxTokens := qps

	pipe := rl.client.Pipeline()
	pipe.Set(ctx, rateKey, tokenRate, 0)
	pipe.Set(ctx, maxTokensKey, maxTokens, 0)
	_, err := pipe.Exec(ctx)
	return err
}

// Allow 函数判断是否允许请求通过
// requested_tokens 表示本次请求需要的令牌数量、
// 返回本次发放的令牌数量
func (rl *RateLimiter) Allow(ctx context.Context, key string, requested_tokens int) int {
	// 使用 lua 脚本实现令牌桶算法
	// 获取键名以及相关配置, 计算可用的令牌数量, 处理请求, 返回可用的令牌数量
	script := `
        local tokens_key = KEYS[1]
		local timestamp_key = KEYS[2]
		local rate_key = KEYS[3]
		local max_tokens_key = KEYS[4]
		
		-- 获取最大令牌数量和令牌生成速率
		local max_tokens = tonumber(redis.call('get', max_tokens_key))
		local token_rate = tonumber(redis.call('get', rate_key))
		
		if not max_tokens or not token_rate then
			return 0
		end
		
		local now = tonumber(ARGV[1])
		local requested = tonumber(ARGV[2])

		-- 获取上次的令牌数量以及上次更新时间
		local last_tokens = tonumber(redis.call('get', tokens_key) or max_tokens)
		local last_refreshed = tonumber(redis.call('get', timestamp_key) or now)
		
		-- 根据时间差计算当前可用的令牌数量
		local elapsed = now - last_refreshed
		local new_tokens = math.min(max_tokens, last_tokens + (elapsed * token_rate))
		
		-- 如果有足够的令牌, 则更新令牌数量和上次更新时间
		if new_tokens >= requested then
			-- 使用 string.format 格式化浮点数, 保留 3 位小数
			-- 若 1s 内无请求, 刷新令牌桶, 令牌数量恢复到 max_tokens
			redis.call('setex', tokens_key, 1, string.format("%.3f", new_tokens - requested))
			redis.call('setex', timestamp_key, 1, now)
			return requested
		else
			-- 如果令牌不足, 返回可用的令牌数量
			local available_tokens = math.floor(new_tokens)
			-- 更新令牌数量和上次更新时间
			redis.call('setex', tokens_key, 1, string.format("%.3f", new_tokens - available_tokens))
			redis.call('setex', timestamp_key, 1, now)
			return available_tokens
		end
    `

	keys := []string{
		rl.keyPrefix + ":" + key + ":tokens",     // 令牌数量
		rl.keyPrefix + ":" + key + ":ts",         // 上次更新时间
		rl.keyPrefix + ":" + key + ":rate",       // 令牌生成速率
		rl.keyPrefix + ":" + key + ":max_tokens", // 最大令牌数量
	}

	args := []interface{}{
		time.Now().UnixMilli(), // 以 ms 为单位来生成 token
		requested_tokens,       // 某一个任务所请求的 token 数量
	}

	available_tokens, err := rl.client.Eval(ctx, script, keys, args...).Int()
	if err != nil {
		fmt.Printf("请求令牌失败! %v\n", err)
		return 0
	}

	return available_tokens
}

// GetTokens 获取当前令牌数量和上次更新时间
func (rl *RateLimiter) GetTokens(ctx context.Context, key string) (float64, int64, error) {
	// lua 脚本根据时间差计算当前可用令牌的数量, 主要用于查看当前限流器的状态
	script := `
        local tokens_key = KEYS[1]
        local timestamp_key = KEYS[2]
        local rate_key = KEYS[3]
        local max_tokens_key = KEYS[4]
        
        local max_tokens = tonumber(redis.call('get', max_tokens_key))
        local token_rate = tonumber(redis.call('get', rate_key))
        
        if not max_tokens or not token_rate then
            return {0, 0}
        end

        local now = tonumber(ARGV[1])
        local last_tokens = tonumber(redis.call('get', tokens_key) or max_tokens)
        local last_refreshed = tonumber(redis.call('get', timestamp_key) or now)
        
        local elapsed = now - last_refreshed
        local new_tokens = math.min(max_tokens, last_tokens + (elapsed * token_rate))
        
		-- lua 脚本返回的数值会被 Redis 自动转换为 int64 类型
        return {string.format("%.3f", new_tokens), last_refreshed}
    `

	keys := []string{
		rl.keyPrefix + ":" + key + ":tokens",
		rl.keyPrefix + ":" + key + ":ts",
		rl.keyPrefix + ":" + key + ":rate",
		rl.keyPrefix + ":" + key + ":max_tokens",
	}

	args := []interface{}{
		time.Now().UnixMilli(),
	}

	result, err := rl.client.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		return 0, 0, err
	}

	values := result.([]interface{})
	tokens, err := strconv.ParseFloat(values[0].(string), 64)
	lastRefresh := values[1].(int64)
	return tokens, lastRefresh, nil
}
