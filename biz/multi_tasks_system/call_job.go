package multi_tasks_system

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"monilive/internal/biz"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// TaskProcessor handles the periodic checking of Redis and manages goroutines
// 定义结构体
type TaskProcessor struct {
	redisClient *redis.Client // redis 客户端
	taskSetKey  string        // redis 中存储任务ID的集合键
	interval    time.Duration // 检查任务的时间间隔
	activeTasks sync.Map      // 改用 sync.Map 替代 map
	rateLimiter *RateLimiter  // 全局限速器
	wg          sync.WaitGroup
}

func (tp *TaskProcessor) WG() {
	panic("unimplemented")
}

func (tp *TaskProcessor) RangeActiveTasks(f func(key any, value any) bool) {
	panic("unimplemented")
}

// NewTaskProcessor creates a new TaskProcessor
// 构造函数，创建并且初始化 TaskProcessor
func NewTaskProcessor(client *redis.Client, taskSetKey string, interval time.Duration, rateLimiter *RateLimiter) *TaskProcessor {
	return &TaskProcessor{
		redisClient: client,
		taskSetKey:  taskSetKey,
		interval:    interval,
		rateLimiter: rateLimiter,
	}
}

// 存放worker数据的字典
var WorkerInfoMap sync.Map

// 存储worker具体信息的结构体
type WorkerInfo struct {
	Worker         biz.AiWorkerDeployEntity
	SpeechCraft    biz.AiSpeechCraftDeployEntity
	PromptTemplate biz.PromptTemplateEntity
	Category       biz.PromptTemplateCategoryEntity
	Voice          biz.GuideShoppingVoiceEntity
}

// 设置计时器，定期刷新worker信息
func (tp *TaskProcessor) StartWorkerInfoRefresher(ctx context.Context, db *gorm.DB) {
	ticker := time.NewTicker(3 * time.Second)

	go func() {
		defer tp.wg.Done()
		for {
			select {
			case <-ticker.C:
				tp.refreshWorkerData(db)
			case <-ctx.Done():
				ticker.Stop()
				fmt.Println(" Worker 数据刷新任务已取消")
				return
			}
		}
	}()
}

// worker具体信息的获取
func (tp *TaskProcessor) refreshWorkerData(db *gorm.DB) {
	var workers []biz.AiWorkerDeployEntity
	err := db.Model(&biz.AiWorkerDeployEntity{}).Find(&workers).Error
	if err != nil {
		fmt.Println(" 获取 Worker 列表失败：", err)
		return
	}

	for _, worker := range workers {
		info := WorkerInfo{Worker: worker}

		//获取话术
		if err := db.Table("ai_speech_craft").Where("worker_id = ?", worker.ID).Scan(&info.SpeechCraft).Error; err != nil {
			fmt.Printf(" 获取话术失败（WorkerID=%d）: %v\n", worker.ID, err)
		}
		//获取提示词
		if err := db.Model(&biz.PromptTemplateEntity{}).Where("id = ?", worker.PromptId).Last(&info.PromptTemplate).Error; err != nil {
			fmt.Printf(" 获取模板失败（WorkerID=%d）: %v\n", worker.ID, err)
		}
		//获取场景
		if err := db.Model(&biz.PromptTemplateCategoryEntity{}).Where("id = ?", info.PromptTemplate.PromptTemplateCategoryId).Last(&info.Category).Error; err != nil {
			fmt.Printf(" 获取场景失败（WorkerID=%d）: %v\n", worker.ID, err)
		}
		//获取语音
		if err := db.Table("guide_shopping_voice").Where("id = ?", worker.VoiceId).Scan(&info.Voice).Error; err != nil {
			fmt.Printf(" 获取语音失败（WorkerID=%d）: %v\n", worker.ID, err)
		}

		WorkerInfoMap.Store(worker.ID, &info)
	}
	fmt.Println(" Worker 数据已刷新")
}

// Start begins the periodic checking process
// 启动定时检查任务，使用 ticker 定期触发检查
func (tp *TaskProcessor) Start(ctx context.Context) {
	// 创建定时器，设置间隔时间，定时检查任务
	ticker := time.NewTicker(tp.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // 定时器触发
			tp.checkTasks(ctx)
		case <-ctx.Done(): // 收到上下文取消信号时
			tp.Stop()
			return
		}
	}
}

// Stop gracefully stops all active tasks
// 优雅地停止所有的任务
func (tp *TaskProcessor) Stop() {
	// 使用 sync.Map 的 Range 方法遍历所有任务
	tp.activeTasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		cancel := value.(context.CancelFunc)
		log.Printf("Stopping task: %s", taskID)
		cancel()
		return true
	})

	// 检查 waitgroup
	tp.wg.Wait()
}

// checkTasks retrieves tasks from Redis and manages goroutines
func (tp *TaskProcessor) checkTasks(ctx context.Context) {
	var cursor uint64
	for {
		// 每次获取100个任务ID
		var keys []string
		var err error
		keys, cursor, err = tp.redisClient.SScan(ctx, tp.taskSetKey, cursor, "*", 100).Result()
		if err != nil {
			log.Printf("Error scanning tasks from Redis: %v", err)
			return
		}

		// 处理这一批任务ID
		for _, taskID := range keys {
			if _, exists := tp.activeTasks.Load(taskID); !exists {
				log.Printf("Starting new task: %s", taskID)
				// 通过 context.WithCancel 创建取消函数
				// 当 cacel 被调用时, 会取消对应的 context, 停止运行对应的 goroutine
				taskCtx, cancel := context.WithCancel(ctx)
				tp.activeTasks.Store(taskID, cancel)
				// wait 删除
				tp.wg.Add(1)
				go tp.processTask(taskCtx, taskID)
			}
		}

		// 如果 cursor 为 0, 表示已经遍历完所有任务
		if cursor == 0 {
			break
		}
	}

	// 清理不存在的任务
	tp.activeTasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		// 使用 SIsMember 检查任务是否还存在
		exists, err := tp.redisClient.SIsMember(ctx, tp.taskSetKey, taskID).Result()
		if err != nil {
			log.Printf("Failed to check task existence: %v", err)
			return true
		}
		if !exists {
			cancel := value.(context.CancelFunc)
			log.Printf("Task no longer in set, cancelling: %s", taskID)
			cancel()
			tp.activeTasks.Delete(taskID)
		}
		return true
	})
}

// processTask handles the processing for a single task ID
// 处理单个任务的具体逻辑，从 redis 列表中获取电话号码，处理每个电话号码
func (tp *TaskProcessor) processTask(ctx context.Context, taskID string) {
	// 使用 defer 确保任务结束时减少 waitgroup 计数
	defer tp.wg.Done()

	log.Printf("Processing task %s", taskID)

	listKey := fmt.Sprintf("task:id:%s", taskID)

	// 无限循环处理任务，使用 select 监听取消信号
	for {
		select {
		case <-ctx.Done():
			log.Printf("Task %s received cancellation signal, exiting", taskID)
			return
		default:
			// Get phone numbers with rate limiting
			// 获取电话号码
			qps := 2
			phoneNumbers, err := tp.getPhoneNumbersWithRateLimit(ctx, listKey, qps)
			if err != nil {
				log.Printf("Error getting phone numbers for task %s: %v", taskID, err)
				time.Sleep(5 * time.Second) // Wait before retry
				continue
			}

			// Process the phone numbers
			// 处理电话号码
			for _, phoneNumber := range phoneNumbers {
				tp.processPhoneNumber(phoneNumber)
			}
		}
	}
}

// 根据 taskID 暂停任务
func (tp *TaskProcessor) PauseTask(taskID string) error {
	value, exists := tp.activeTasks.Load(taskID)
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	// 调用取消函数来停止任务
	cancel := value.(context.CancelFunc)
	cancel()

	// 从 activeTasks 中删除 taskID
	tp.activeTasks.Delete(taskID)

	log.Printf("Task %s has been paused", taskID)

	return nil
}

// 根据 taskID 取消任务
// 删掉 taskid 对应的号码
func (tp *TaskProcessor) CancelTask(ctx context.Context, taskID string) error {
	// 从 activeTasks 中获取任务的取消函数
	value, exists := tp.activeTasks.Load(taskID)
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	// 调用取消函数来停止任务
	cancel := value.(context.CancelFunc)
	cancel()

	// 从 activeTasks 中删除 taskID
	tp.activeTasks.Delete(taskID)

	// 删除 Redis 中的任务列表
	listKey := fmt.Sprintf("task:id:%s", taskID)
	if err := tp.redisClient.Del(ctx, listKey).Err(); err != nil {
		return fmt.Errorf("failed to delete task list: %w", err)
	}

	log.Printf("Task %s has been stopped and its list has been deleted", taskID)

	return nil
}

// getPhoneNumbersWithRateLimit retrieves phone numbers with rate limiting
func (tp *TaskProcessor) getPhoneNumbersWithRateLimit(ctx context.Context, listKey string, qps int) ([]string, error) {
	// For simplicity, our rate limiter always returns maxTasksPerRun items
	// In a real implementation, this would check a global rate limiting system
	// 根据 listkey 从 redis 拿电话号码
	// 实际上从别处获取该任务的 qps, 然后通过 Allow 函数从全局限速器拿取 token 来处理电话号码
	// qps 作为函数的输入参数, 在进入 GetAllowedRun 函数之前, 应该通过计算转换成 token 数量 ( 缺少 qps 与 token 之间的计算公式, 可以通过轮询间隔设置 requested_tokens = qps * interval )
	allowedRun := tp.GetAllowedRun(ctx, "global", qps)

	// Get up to maxTasksPerRun phone numbers from the list
	// 根据全局限速器得到本次可以获取的电话号码数量 tp.maxTasksPerRun
	// 获取 redis 的 list 中的电话号码
	// 从对应的 list 左侧弹出 allowedRun 个电话号码
	// 检查超出情况
	phoneNumbers, err := tp.redisClient.LPopCount(ctx, listKey, allowedRun).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get phone numbers: %w", err)
	}

	// If no numbers were returned, wait before checking again
	// 如果没有获取到电话号码
	// 返回错误, 所有的号码已经被处理完成
	if len(phoneNumbers) == 0 {
		return nil, fmt.Errorf("all phone numbers have been processed")
	}

	return phoneNumbers, nil
}

// processPhoneNumber performs the actual processing for a phone number
// 电话号码的处理
func (tp *TaskProcessor) processPhoneNumber(phoneNumber string) {
	// In a real application, this would contain the actual business logic
	log.Printf("Processing phone number: %s", phoneNumber)

	// Simulate some work
	time.Sleep(100 * time.Millisecond)
}

// 从全局限流器拿取 token 来处理电话号码
func (tp *TaskProcessor) GetAllowedRun(ctx context.Context, key string, requested_tokens int) int {
	// 通过全局限流器的 Allow 函数, 申请全局限流器的 token 来处理电话号码
	allowed := tp.rateLimiter.Allow(ctx, key, requested_tokens)

	return allowed
}
