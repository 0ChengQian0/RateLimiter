package main

// 测试限流器
func main() {
	// // 创建 Redis 客户端
	// client := redis.NewClient(&redis.Options{
	// 	Addr:     "localhost:6379",
	// 	Password: "", // 如果有密码，在这里设置
	// 	DB:       0,
	// })

	// // 创建限流器实例
	// limiter := ratelimiter.NewRateLimiter(
	// 	client,
	// 	10,     // 最大令牌数
	// 	10,     // 每秒产生的令牌数
	// 	"test", // Redis key前缀
	// )

	// // 测试限流器
	// // ctx := context.Background()
	// // for i := 0; i < 15; i++ {
	// // 	time.Sleep(1000 * time.Millisecond)
	// // 	if limiter.Allow(ctx, "user1") {
	// // 		fmt.Printf("请求 %d 通过\n", i+1)
	// // 	} else {
	// // 		fmt.Printf("请求 %d 被限流\n", i+1)
	// // 	}
	// // 	time.Sleep(1000 * time.Millisecond)
	// // }
	// // 测试限流器
	// ctx := context.Background()
	// for i := 0; i < 15; i++ {
	// 	tokens, lastRefresh, err := limiter.GetTokens(ctx, "user1")
	// 	if err != nil {
	// 		fmt.Printf("获取令牌信息失败: %v\n", err)
	// 	} else {
	// 		fmt.Printf("当前令牌数: %d, 上次更新时间: %s\n",
	// 			tokens,
	// 			time.Unix(lastRefresh, 0).Format("15:04:05"))
	// 	}

	// 	if limiter.Allow(ctx, "user1") {
	// 		fmt.Printf("请求 %d 通过\n", i+1)
	// 	} else {
	// 		fmt.Printf("请求 %d 被限流\n", i+1)
	// 	}
	// 	time.Sleep(500 * time.Millisecond)
	// }
}
