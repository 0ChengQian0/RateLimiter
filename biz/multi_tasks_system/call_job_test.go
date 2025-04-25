package multi_tasks_system

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestProcessTask(t *testing.T) {
	// 设置 Redis 客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer redisClient.Close()

	// 测试前清理数据
	ctx := context.Background()
	taskSetKey := "test:tasks"
	redisClient.Del(ctx, taskSetKey)

	// 创建限流器
	rateLimiter := NewRateLimiter(redisClient, "test") // 假设每秒允许10个请求
	err := rateLimiter.SetRate(context.Background(), "global", 200.0)
	if err != nil {
		print("全局限速器设置失败！")
	}

	// 创建 TaskProcessor
	processor := NewTaskProcessor(redisClient, taskSetKey, 1*time.Second, rateLimiter)

	// 准备测试数据
	testTask := "test_task_1"
	listKey := fmt.Sprintf("task:id:%s", testTask)

	// 添加测试任务到 Redis
	redisClient.SAdd(ctx, taskSetKey, testTask)
	// 添加测试电话号码
	phoneNumbers := []string{"1234567890", "0987654321", "5555555555"}
	redisClient.RPush(ctx, listKey, phoneNumbers)

	// 启动处理器
	go processor.Start(ctx)

	// 等待一段时间让处理器运行
	time.Sleep(3 * time.Second)

	// 验证号码是否被处理（列表应该为空）
	length, err := redisClient.LLen(ctx, listKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length, "所有号码应该已被处理")

	// 清理数据
	redisClient.Del(ctx, taskSetKey, listKey)
}

func TestPauseTask(t *testing.T) {
	// 设置 Redis 客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer redisClient.Close()

	// 测试前清理数据
	ctx := context.Background()
	taskSetKey := "test:tasks"
	redisClient.Del(ctx, taskSetKey)

	// 创建限流器
	rateLimiter := NewRateLimiter(redisClient, "test") // 假设每秒允许10个请求
	err := rateLimiter.SetRate(context.Background(), "global", 2.0)
	if err != nil {
		print("全局限速器设置失败！")
	}

	// 创建 TaskProcessor
	processor := NewTaskProcessor(redisClient, taskSetKey, 5*time.Second, rateLimiter)

	// 准备测试数据
	testTask := "test_task_2"
	listKey := fmt.Sprintf("task:id:%s", testTask)

	// 添加测试任务到 Redis
	redisClient.SAdd(ctx, taskSetKey, testTask)
	// 添加测试电话号码
	phoneNumbers := []string{"1234567890", "0987654321", "5555555555", "333", "999", "888", "666", "555"}
	redisClient.RPush(ctx, listKey, phoneNumbers)

	// 启动处理器
	go processor.Start(ctx)

	// 等待一段时间让处理器运行
	time.Sleep(6 * time.Second)

	beforePasuLength, _ := redisClient.LLen(ctx, listKey).Result()

	// 暂停任务
	err = processor.PauseTask(testTask)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	afterPauseLength, _ := redisClient.LLen(ctx, listKey).Result()

	assert.Equal(t, beforePasuLength, afterPauseLength, "暂停任务后，号码不应该被处理")

	// 验证 phoneNumbers 列表是否在 redis
	// 验证 phoneNumbers 列表是否在 redis
	_, exists := processor.activeTasks.Load(testTask)
	assert.False(t, exists, "任务应该已被暂停")
	existsList, _ := redisClient.Exists(ctx, listKey).Result()
	assert.Equal(t, int64(1), existsList, "phoneNumbers列表应该仍然存在于Redis中")

	// 清理数据
	redisClient.Del(ctx, taskSetKey, listKey)
}

func TestCancelTask(t *testing.T) {
	// 设置 Redis 客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer redisClient.Close()

	// 测试前清理数据
	ctx := context.Background()
	taskSetKey := "test:tasks"
	redisClient.Del(ctx, taskSetKey)

	// 创建限流器
	rateLimiter := NewRateLimiter(redisClient, "test") // 假设每秒允许10个请求
	err := rateLimiter.SetRate(context.Background(), "global", 2.0)
	if err != nil {
		print("全局限速器设置失败！")
	}

	// 创建 TaskProcessor
	processor := NewTaskProcessor(redisClient, taskSetKey, 5*time.Second, rateLimiter)

	// 准备测试数据
	testTask := "test_task_3"
	listKey := fmt.Sprintf("task:id:%s", testTask)

	// 添加测试任务到 Redis
	redisClient.SAdd(ctx, taskSetKey, testTask)
	// 添加测试电话号码
	phoneNumbers := []string{"1234567890", "0987654321", "5555555555", "333", "999", "888", "666", "555"}
	redisClient.RPush(ctx, listKey, phoneNumbers)

	// 启动处理器
	go processor.Start(ctx)

	// 等待一段时间让处理器运行
	time.Sleep(6 * time.Second)

	// 取消任务
	err = processor.CancelTask(ctx, testTask)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// 验证 phoneNumbers 列表是否在 redis
	// 验证 phoneNumbers 列表是否在 redis
	_, exists := processor.activeTasks.Load(testTask)
	assert.False(t, exists, "任务应该已被暂停")
	existsList, _ := redisClient.Exists(ctx, listKey).Result()
	assert.Equal(t, int64(0), existsList, "phoneNumbers列表应该仍然存在于Redis中")

	// 清理数据
	redisClient.Del(ctx, taskSetKey, listKey)
}

func TestMultiTaskCancel(t *testing.T) {
	// 设置 Redis 客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer redisClient.Close()

	// 测试前清理数据
	ctx := context.Background()
	taskSetKey := "test:tasks"
	redisClient.Del(ctx, taskSetKey)

	// 创建限流器
	rateLimiter := NewRateLimiter(redisClient, "test")
	err := rateLimiter.SetRate(context.Background(), "global", 5.0)
	assert.NoError(t, err, "设置限流器失败")

	// 创建 TaskProcessor
	processor := NewTaskProcessor(redisClient, taskSetKey, 8*time.Second, rateLimiter)

	// 准备两个测试任务的数据
	task1 := "test_task_1"
	task2 := "test_task_2"
	listKey1 := fmt.Sprintf("task:id:%s", task1)
	listKey2 := fmt.Sprintf("task:id:%s", task2)

	// 添加测试任务到 Redis
	redisClient.SAdd(ctx, taskSetKey, task1, task2)

	// 添加测试电话号码
	phoneNumbers1 := []string{"1111111111", "2222222222", "3333333333", "111", "222", "333", "11", "22", "33"}
	phoneNumbers2 := []string{"4444444444", "5555555555", "6666666666", "444", "555", "666", "44", "55", "66", "4", "5", "6", "7", "8", "9"}
	redisClient.RPush(ctx, listKey1, phoneNumbers1)
	redisClient.RPush(ctx, listKey2, phoneNumbers2)

	// 启动处理器
	go processor.Start(ctx)

	// 等待确保两个任务都开始运行
	time.Sleep(9 * time.Second)

	// 获取任务1取消前的号码数量
	beforePause1, _ := redisClient.LLen(ctx, listKey1).Result()

	// 取消任务1
	err = processor.PauseTask(task1)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	afterPause1, _ := redisClient.LLen(ctx, listKey1).Result()
	assert.Equal(t, beforePause1, afterPause1, "暂停任务1后，号码不应该被处理")

	// 验证任务2的列表仍然存在且在处理中
	// 获取并打印任务2中剩余的号码
	remainingNumbers, err := redisClient.LRange(ctx, listKey2, 0, -1).Result()
	assert.NoError(t, err)
	log.Printf("Task2 剩余号码: %v", remainingNumbers)
	beforePause2, _ := redisClient.LLen(ctx, listKey2).Result()

	time.Sleep(2 * time.Second)
	rest, _, _ := rateLimiter.GetTokens(ctx, "global")
	fmt.Printf("剩余令牌: %d\n", int(rest))
	time.Sleep(2 * time.Second)
	// 获取并打印任务2中剩余的号码
	remainingNumbers, err = redisClient.LRange(ctx, listKey2, 0, -1).Result()
	assert.NoError(t, err)
	log.Printf("Task2 剩余号码: %v", remainingNumbers)

	exists2, err := redisClient.Exists(ctx, listKey2).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists2, "任务2的号码列表应该仍然存在")

	// 验证任务2的号码数量减少（说明正在被处理）
	afterCancel2, err := redisClient.LLen(ctx, listKey2).Result()
	assert.NoError(t, err)
	assert.Less(t, afterCancel2, beforePause2, "任务2应该在继续处理号码")

	// 清理数据
	redisClient.Del(ctx, taskSetKey, listKey1, listKey2)
}
