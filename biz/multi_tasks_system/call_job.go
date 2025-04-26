package multi_tasks_system

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	krlog "github.com/go-kratos/kratos/v2/log"
	"log"
	"math/big"
	mathRand "math/rand"
	"monilive/internal/biz"
	"monilive/internal/conf"
	"monilive/lib"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// TaskProcessor handles the periodic checking of Redis and manages goroutines
// 定义结构体
type TaskProcessor struct {
	redisClient               *redis.Client // redis 客户端
	taskSetKey                string        // redis 中存储任务ID的集合键
	interval                  time.Duration // 检查任务的时间间隔
	activeTasks               sync.Map      // 改用 sync.Map 替代 map
	rateLimiter               *RateLimiter  // 全局限速器
	wg                        sync.WaitGroup
	log                       *krlog.Helper
	CommonUsecase             *biz.CommonUsecase
	conf                      *conf.Data
	SaleCallTaskUsecase       *biz.SaleCallTaskUsecase
	SaleCallTaskDetailUsecase *biz.SaleCallTaskDetailUsecase
	CallbackRecordUsecase     *biz.CallbackRecordUsecase
	biz.AiCallJob[biz.SaleCallTaskEntity, biz.SaleCallTaskDetailEntity]
	biz.AiCallBackJob[biz.SaleCallTaskEntity, biz.SaleCallTaskDetailEntity]
	biz.RepeatCallJob[biz.SaleCallTaskEntity, biz.SaleCallTaskDetailEntity]
	biz.NewCallJob[biz.SaleCallTaskEntity, biz.SaleCallTaskDetailEntity]
	biz.RecoverKey
	biz.ControlConcurrencyJob
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

func (tp *TaskProcessor) getCallLine(task *biz.SaleCallTaskEntity) lib.TypeMap {
	// 外呼线路实现逻辑
	switch task.IsCustomOutput {
	case 1:
		var callLine []biz.CompanyCallLinesDeployEntity
		var total int64
		id, _ := strconv.ParseInt(task.OutputCallNumber, 10, 64)
		tp.CommonUsecase.DB().Model(&biz.CompanyCallLinesDeployEntity{}).Where("status = 1 and type = 1 and company_id = ? and id = ?", task.CompanyId, id).Count(&total).Find(&callLine)
		if total > 0 {
			return lib.TypeMap{"dial": callLine[0].LineTag, "cid_number": callLine[0].SipUserName, "call_line_id": callLine[0].ID, "line_id": callLine[0].CallLinesId}
		}
		tp.CommonUsecase.DB().Model(&biz.CompanyCallLinesDeployEntity{}).Where("status = 1 and type = 1 and company_id = ?", task.CompanyId).Count(&total).Find(&callLine)
		if total == 0 {
			return lib.TypeMap{"dial": "sofia/gateway/liepin", "cid_number": "10001", "call_line_id": 0, "line_id": 0}
		}
		randomNum, _ := rand.Int(rand.Reader, big.NewInt(total))
		num, _ := strconv.ParseInt(randomNum.String(), 10, 64)
		if total == num {
			num--
		}
		return lib.TypeMap{"dial": callLine[num].LineTag, "cid_number": callLine[num].SipUserName, "call_line_id": callLine[num].ID, "line_id": callLine[num].CallLinesId}
	case 2:
		id, _ := strconv.ParseInt(task.OutputCallNumber, 10, 64)
		var companyCallLine biz.CompanyCallLinesDeployEntity
		tp.CommonUsecase.DB().Model(&biz.CompanyCallLinesDeployEntity{}).Where("id = ? and status = 1 and type = 2 and company_id = ?", id, task.CompanyId).First(&companyCallLine)
		fmt.Println(companyCallLine)
		if companyCallLine.ID != 0 {
			return lib.TypeMap{"dial": companyCallLine.LineTag, "cid_number": companyCallLine.SipUserName, "call_line_id": companyCallLine.ID, "line_id": companyCallLine.CallLinesId}
		}
		break
	}
	return nil
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

func (tp *TaskProcessor) getSiliconModel(idSlave int) biz.LlmConfig {
	id := idSlave
	if tp.conf.Crontab.IsSlaveModelSpecify == 1 && tp.conf.Crontab.SlaveModelId != 0 {
		id = int(tp.conf.Crontab.SlaveModelId)
	}
	var blendDeployEntity biz.BlendDeployEntity
	tp.CommonUsecase.DB().Model(biz.BlendDeployEntity{}).Where(id).First(&blendDeployEntity)
	var typeMap lib.TypeMap
	_ = json.Unmarshal([]byte(blendDeployEntity.DeployConfig), &typeMap)
	var siliconModelKey biz.ModelKeyDeployEntity
	tp.CommonUsecase.DB().Model(&biz.ModelKeyDeployEntity{}).Where("status = 1 and blend_deploy_id = ?", id).Order("used_at asc").First(&siliconModelKey)
	var cstSh, _ = time.LoadLocation("Asia/Shanghai")
	tp.CommonUsecase.DB().Model(&biz.ModelKeyDeployEntity{}).Where(siliconModelKey.ID).Update("used_at", time.Now().In(cstSh))
	model := typeMap.GetString("endPoint")
	if siliconModelKey.Model != "" {
		model = siliconModelKey.Model
	}
	return biz.LlmConfig{
		ApiKey:        siliconModelKey.Key,
		BaseURL:       typeMap.GetString("baseUrl"),
		BlendDeployId: blendDeployEntity.ID,
		Model:         model,
	}
}

func (tp *TaskProcessor) getSlaveModelConfig(promptTemplateCategory biz.PromptTemplateCategoryEntity, llmOptions *biz.LlmOptions) biz.LlmConfig {
	if promptTemplateCategory.Content.BlendDeploysSlaveId == nil {
		return biz.LlmConfig{}
	}
	mathRand.Seed(time.Now().UnixNano())
	var blendDeploysSlaveId []int
	_ = json.Unmarshal([]byte(lib.InterfaceToString(promptTemplateCategory.Content.BlendDeploysSlaveId)), &blendDeploysSlaveId)

	index2 := mathRand.Intn(len(blendDeploysSlaveId))
	idSlave := blendDeploysSlaveId[index2]
	var llmConfigs biz.LlmConfig
	// 副配置
	siliconModel := tp.getSiliconModel(idSlave)
	if siliconModel.ApiKey != "" {
		llmConfigs.ApiKey = siliconModel.ApiKey
		llmConfigs.BaseURL = siliconModel.BaseURL
		llmConfigs.Model = siliconModel.Model
		llmConfigs.BlendDeployId = siliconModel.BlendDeployId
		llmOptions.Model = siliconModel.Model
	}
	return llmConfigs
}

func (tp *TaskProcessor) GetLastResponse(ids []int, intention string) (string, string) {

	/*
		### 输出要求：（请严格依照以下意向判断标准）
		1. 电话聊天场景中“嗯”一般用于语气词，而不是肯定答复，一般情况下不允许将“嗯”识别为肯定答复。
		2. 【重要】客户在同一个问题中同时表示肯定和否定，则认为客户给出了否定回复。
		3. 【重要】ASR得出的文本可能不准确，出现读音相似但是文字含义不同的情况，请充分考虑上下文，分析客户表达的意思。
		 **关键人物**： - 在通话开始时，通过询问确认对方是否为准确沟通对象，关键决策者或相关联系人。
		 **明确需求**：   - 在通话中，通过提问了解客户的需求和痛点。如果客户表达了对产品或服务的具体需求，记录为有“明确需求”。
		 **合作意向**：  - 询问客户是否有兴趣进一步了解相关产品或服务，或者是否有意向进行合作。如果客户表现出积极的态度，记录为有“合作意向”。
		 **轻度反感**： - 如果客户在通话中表现出轻微的不耐烦或拒绝，如“不用了，不需要……”，记录为“轻度反感”。
		 **重度排斥**：  - 如果客户在通话中表现出强烈的排斥意愿，如“别再打了”、使用侮辱性言语、威胁投诉，并要求加入黑名单，记录为“重度排斥”。
		**客户忙**： - 如果客户在接通后立即表示当前忙碌或者在开会，询问是否可以安排其他时间回电。记录客户的忙碌状态及回电请求。
		**语音助手**： - 如果接通后客户说“您好，我是XXX的“智能助理”，“语音助手”或“语音邮箱”，记录为“语音助手”。
		基于以上标准快速识别和记录关键信息，提高你的工作效率和准确性，作为金牌电话销售，这些标准将帮助你更精准地判断客户意向，优化销售策略，并提升客户互动的质量。
	*/

	var entityList []*biz.ResponseManageDeployEntity
	tp.CommonUsecase.DB().Model(&biz.ResponseManageDeployEntity{}).Where(biz.ResponseManageDeployEntity{CompanyId: 0, UserId: 0, SourceId: 0, Status: 1, IsFix: 1}).Order("id asc").Find(&entityList)
	var customEntityList []*biz.ResponseManageDeployEntity
	CoverResponse := map[int64]*biz.ResponseManageDeployEntity{}
	tp.CommonUsecase.DB().Model(&biz.ResponseManageDeployEntity{}).Where("id in ? and is_fix = 2", ids).Order("id asc").Find(&customEntityList)
	for _, v := range customEntityList {
		if v.CoverId != 0 {
			CoverResponse[v.CoverId] = v
		}
	}
	// outputRequirements := "### 输出要求：（请严格依照以下意向判断标准）\n\t\t1. 电话聊天场景中“嗯”一般用于语气词，而不是肯定答复，一般情况下不允许将“嗯”识别为肯定答复。\n\t\t2. 【重要】客户在同一个问题中同时表示肯定和否定，则认为客户给出了否定回复。\n\t\t3. 【重要】ASR得出的文本可能不准确，出现读音相似但是文字含义不同的情况，请充分考虑上下文，分析客户表达的意思。"
	// var outputRequirementMap []*ResponseManageDeployEntity
	response := intention
	for _, v := range entityList {
		remark := v.Remark
		if CoverResponse[v.ID] != nil {
			remark = CoverResponse[v.ID].Remark
			v.Remark = CoverResponse[v.ID].Remark
			v.JudgmentCriteria = CoverResponse[v.ID].JudgmentCriteria
			v.Title = CoverResponse[v.ID].Title
		}
		switch v.Type {
		case 1:
			response += "{'name':'" + v.Value + "','type':'tinyint','length':2, 'description':'根据用户对话判断" + remark + ",1是2否', 'judgment_criteria':'" + v.JudgmentCriteria + "'}, "
			break
		case 2:
			response += "{'name':'" + v.Value + "','type':'varchar','length':100, 'description':'" + remark + "', 'judgment_criteria':'" + v.JudgmentCriteria + "'}, "
			break
		default:
			response += "{'name':'" + v.Value + "','type':'varchar','length':100, 'description':'" + remark + "', 'judgment_criteria':'" + v.JudgmentCriteria + "'}, "
			break
		}
		// outputRequirementMap = append(outputRequirementMap, v)
		// outputRequirements += fmt.Sprintf("**%s**：- %s", v.Title, v.JudgmentCriteria)
	}

	for _, v := range customEntityList {
		if v.CoverId != 0 {
			continue
		}
		var field string
		if v.Value != "" {
			field = v.Value
		} else {
			field = fmt.Sprintf("field%d", v.ID)
		}
		switch v.Type {
		case 1:
			response += "{'name':'" + field + "','type':'tinyint','length':2, 'description':'根据用户对话判断" + v.Remark + ",1是2否'}, 'judgment_criteria':'" + v.JudgmentCriteria + "'}, "
			break
		case 2:
			response += "{'name':'" + field + "','type':'varchar','length':100, 'description':'" + v.Remark + "'}, 'judgment_criteria':'" + v.JudgmentCriteria + "'}, "
			break
		default:
			response += "{'name':'" + field + "','type':'varchar','length':100, 'description':'" + v.Remark + "'}, 'judgment_criteria':'" + v.JudgmentCriteria + "'}, "
			break
		}
		// outputRequirementMap = append(outputRequirementMap, v)
		// outputRequirements += fmt.Sprintf("**%s**：- %s", v.Title, v.JudgmentCriteria)
	}
	//for i := range outputRequirementMap {
	//	outputRequirements += fmt.Sprintf("**%s**：- %s", outputRequirementMap[i].Title, outputRequirementMap[i].JudgmentCriteria)
	//}
	// response += intention
	// outputRequirements += "基于以上标准快速识别和记录关键信息，提高你的工作效率和准确性，作为金牌电话销售，这些标准将帮助你更精准地判断客户意向，优化销售策略，并提升客户互动的质量。"

	return response, ""
}

func (c *TaskProcessor) getModelConfig(promptTemplateCategory biz.PromptTemplateCategoryEntity, llmOptions *biz.LlmOptions, param *lib.TypeMap) biz.LlmConfig {
	var blendDeploysId []int
	_ = json.Unmarshal([]byte(lib.InterfaceToString(promptTemplateCategory.Content.BlendDeploysId)), &blendDeploysId)
	var blendDeploysSlaveId []int
	_ = json.Unmarshal([]byte(lib.InterfaceToString(promptTemplateCategory.Content.BlendDeploysSlaveId)), &blendDeploysSlaveId)
	if len(blendDeploysId) == 0 {
		blendDeploysId = append(blendDeploysId, int(promptTemplateCategory.Content.BlendDeploysId.(float64)))
	}
	if len(blendDeploysSlaveId) == 0 {
		if promptTemplateCategory.Content.BlendDeploysSlaveId != nil {
			blendDeploysSlaveId = append(blendDeploysId, int(promptTemplateCategory.Content.BlendDeploysSlaveId.(float64)))
		} else {
			blendDeploysSlaveId = blendDeploysId
		}
	}

	// 初始化种子
	mathRand.Seed(time.Now().UnixNano())
	// 随机选择两个不同的索引
	index1 := mathRand.Intn(len(blendDeploysId))
	index2 := mathRand.Intn(len(blendDeploysSlaveId))

	// 拿出两个元素
	idMain := blendDeploysId[index1]
	idSlave := blendDeploysSlaveId[index2]
	if c.conf.Crontab.IsMainModelSpecify == 1 && c.conf.Crontab.MainModelName != "" {
		idMain = int(c.conf.Crontab.MainModelId)
	}

	var blendDeployEntity biz.BlendDeployEntity
	if param.GetInt("bid") != 0 {
		c.CommonUsecase.DB().Model(biz.BlendDeployEntity{}).Where("id = ?", param.GetInt("bid")).First(&blendDeployEntity)
	} else {
		c.CommonUsecase.DB().Model(biz.BlendDeployEntity{}).Where("id = ?", idMain).First(&blendDeployEntity)
	}
	var typeMap lib.TypeMap
	_ = json.Unmarshal([]byte(blendDeployEntity.DeployConfig), &typeMap)
	var modelKey biz.ModelKeyDeployEntity
	var llmConfigs biz.LlmConfig
	c.CommonUsecase.DB().Model(&biz.ModelKeyDeployEntity{}).Where("status = 1 and blend_deploy_id = ?", blendDeployEntity.ID).Order("used_at asc").First(&modelKey)
	if modelKey.ID == 0 {
		siliconModel := c.getSiliconModel(idSlave)
		if siliconModel.ApiKey == "" {
			s := fmt.Sprintf("任务提示\r\n环境：%s\r\n任务停止，全部key已用完", c.conf.Crontab.Env)
			r, err := biz.GroupNotify(c.conf.Crontab.GetNoKeyFeishuUrl(), s)
			lib.DPrintln(err, r)
			// 停止定时任务
			c.CommonUsecase.DB().Table("status").Where("id = 1").Update("status", 1)
		}
		llmOptions.Model = siliconModel.Model
		return siliconModel
	} else {
		model := typeMap.GetString("endPoint")
		if modelKey.Model != "" {
			model = modelKey.Model
		}

		// 主配置
		llmConfigs = biz.LlmConfig{
			ApiKey:        modelKey.Key,
			BaseURL:       typeMap.GetString("baseUrl"),
			BlendDeployId: blendDeployEntity.ID,
			Model:         model,
		}
		llmOptions.Model = typeMap.GetString("endPoint")

		var cstSh, _ = time.LoadLocation("Asia/Shanghai")
		c.CommonUsecase.DB().Model(&biz.ModelKeyDeployEntity{}).Where(modelKey.ID).Update("used_at", time.Now().In(cstSh))

		// 副配置
		siliconModel := c.getSiliconModel(idSlave)
		if siliconModel.ApiKey != "" {
			llmConfigs.LlmExtra.ApiKey = siliconModel.ApiKey
			llmConfigs.LlmExtra.BaseURL = siliconModel.BaseURL
			llmConfigs.LlmExtra.Model = siliconModel.Model
			llmConfigs.LlmExtra.BlendDeployId = siliconModel.BlendDeployId
		}
	}
	return llmConfigs
}

func (tp *TaskProcessor) detailCall(detailTask *biz.SaleCallTaskDetailEntity, task *biz.SaleCallTaskEntity, typeMap *lib.TypeMap) lib.TypeMap {

	//切换线路
	companyId := task.CompanyId
	if task.CompanyId == 1 && !tp.conf.Liepin.GetIsWhobot() {
		companyId = 2
	}

	switch companyId {
	default:
		if task.WorkerId != 0 || detailTask.WorkerId != 0 {

			workerId := task.WorkerId
			if detailTask.WorkerId != 0 {
				workerId = detailTask.WorkerId
			}
			// 从内存中获取 WorkerInfo
			workerInfoInterface, ok := WorkerInfoMap.Load(workerId)
			if !ok {
				tp.Log.Info(fmt.Sprintf("Worker 数据未找到（WorkerID=%d）", workerId))
				return lib.TypeMap{"code": 0, "errMsg": "", "message": "获取员工失败"}
			}
			workerInfo := workerInfoInterface.(*WorkerInfo)
			worker := &workerInfo.Worker
			if worker.ID == 0 {
				return lib.TypeMap{"code": 0, "errMsg": "", "message": "获取员工失败"}
			}
			speechCraft := &workerInfo.SpeechCraft

			promptTemplate := workerInfo.PromptTemplate
			if promptTemplate.ID == 0 {
				tp.log.Info(fmt.Sprintf("呼叫任务号码%s; 获取模版信息失败", detailTask.CallNumber))
				return lib.TypeMap{"code": 0, "errMsg": "", "message": "获取模版信息失败"}
			}

			//获取场景
			promptTemplateCategory := workerInfo.Category
			if promptTemplateCategory.ID == 0 {

				tp.log.Info(fmt.Sprintf("呼叫任务号码%s; 获取模版信息失败", detailTask.CallNumber))
				return lib.TypeMap{"code": 0, "errMsg": "", "message": "获取模版信息失败"}
			}

			//获取语音
			voice := &workerInfo.Voice
			tp.log.Info(fmt.Sprintf("呼叫任务号码%s; 数据：%s", detailTask.CallNumber, lib.InterfaceToString(voice)))
			var prompt string
			var promptContent biz.PromptContent
			_ = json.Unmarshal(speechCraft.Content, &promptContent)
			hidePrompt := promptTemplateCategory.Content.HidePrompt

			//// todo 临时方案，将822、962转成944
			//if task.IsTest == 2 {
			//	if workerId == 822 || workerId == 962 || workerId == 942 || workerId == 987 {
			//
			//		tempWorkerId := 944
			//		tempWorker := &AiWorkerDeployEntity{Setting: WorkerSetting{BreakDelay: -1}}
			//		tp.CommonUsecase.DB().Model(&AiWorkerDeployEntity{}).Where("id = ?", tempWorkerId).First(&tempWorker)
			//
			//		//获取话术
			//		var tempSpeechCraft AiSpeechCraftDeployEntity
			//		tp.CommonUsecase.DB().Model(&AiSpeechCraftDeployEntity{}).Where("worker_id = ?", tempWorkerId).First(&tempSpeechCraft)
			//		_ = json.Unmarshal(tempSpeechCraft.Content, &promptContent)
			//
			//		var tempPromptTemplate PromptTemplateEntity
			//		tp.CommonUsecase.DB().Model(&PromptTemplateEntity{}).Where("id = ?", worker.PromptId).First(&tempPromptTemplate)
			//
			//		var tempPromptTemplateCategory PromptTemplateCategoryEntity
			//		tp.CommonUsecase.DB().Model(&PromptTemplateCategoryEntity{}).Where("id = ?", promptTemplate.PromptTemplateCategoryId).First(&tempPromptTemplateCategory)
			//		hidePrompt = tempPromptTemplateCategory.Content.HidePrompt
			//	}
			//}

			// 开场白录音
			firsWordVoicePath := ""
			if speechCraft.Extend.FirstWordVoiceId != 0 {
				var firsWordVoice biz.FirstWordVoiceDeployEntity
				tp.CommonUsecase.DB().Model(&biz.FirstWordVoiceDeployEntity{}).First(&firsWordVoice, "id = ?", speechCraft.Extend.FirstWordVoiceId)
				if firsWordVoice.ID != 0 {
					promptContent.Prologue = firsWordVoice.Remark
					firsWordVoicePath = firsWordVoice.Path
				}
			}
			prompt += "开场白：\n" + promptContent.Prologue
			flag := true
			if promptContent.CurrentMode == 1 {
				for i := range promptContent.NormalTagList {
					prompt += "##" + promptContent.NormalTagList[i].TagTitle
					prompt += "\n"
					prompt += promptContent.NormalTagList[i].TagContent
					prompt += "\n"
					if strings.Replace(promptContent.NormalTagList[i].TagTitle, " ", "", -1) == "任务目标" && flag {
						// 获取隐藏prompt
						// promptCategory？
						var promptCategory biz.PromptTemplateCategoryEntity
						tp.CommonUsecase.DB().Model(&biz.PromptTemplateCategoryEntity{}).Where("id = ?", promptTemplate.PromptTemplateCategoryId).First(&promptCategory)
						if promptCategory.ID != 0 {
							prompt += hidePrompt
						}
						flag = false
					} else if strings.Replace(promptContent.NormalTagList[i].TagTitle, " ", "", -1) == "角色" && flag {
						// 获取隐藏prompt
						// promptCategory？
						var promptCategory biz.PromptTemplateCategoryEntity
						tp.CommonUsecase.DB().Model(&biz.PromptTemplateCategoryEntity{}).Where("id = ?", promptTemplate.PromptTemplateCategoryId).First(&promptCategory)
						if promptCategory.ID != 0 {
							prompt += hidePrompt
						}
						flag = false
					}
				}
			} else {
				prompt += "场景： \n" + promptContent.Scene
				for i := range promptContent.TagList {
					prompt += "##" + promptContent.TagList[i].TagTitle
					prompt += "\n"
					prompt += promptContent.TagList[i].TagContent
					prompt += "\n"
				}
			}

			if promptContent.CurrentMode == 1 && flag {
				// 获取隐藏prompt
				// promptCategory？
				var promptCategory biz.PromptTemplateCategoryEntity
				tp.CommonUsecase.DB().Model(&biz.PromptTemplateCategoryEntity{}).Where("id = ?", promptTemplate.PromptTemplateCategoryId).First(&promptCategory)
				if promptCategory.ID != 0 {
					prompt = prompt + hidePrompt
				}
			}

			// 获取变量列表
			var variableList []biz.VariableDeployEntity
			tp.CommonUsecase.DB().Model(&biz.VariableDeployEntity{}).Where("id in ?", speechCraft.Variables).Find(&variableList)
			validateMap := make(map[string]string, len(detailTask.VariableList))
			var lastValidateMap []biz.VariableList
			for i := range detailTask.VariableList {
				validateMap[detailTask.VariableList[i].Origin] = detailTask.VariableList[i].Replace
			}
			for i := range variableList {
				lastValidateMap = append(lastValidateMap, biz.VariableList{Origin: variableList[i].Name, Replace: validateMap[variableList[i].Name], DefaultValue: variableList[i].Value})
			}

			prompt = biz.ReplaceVariable(lib.TypeMap{
				"link_man_company": detailTask.LinkManCompany,
				"link_man":         detailTask.LinkMan,
				"call_number":      detailTask.CallNumber,
				"words":            prompt,
				"variable_list":    lastValidateMap,
			})

			firstWord := biz.ReplaceVariable(lib.TypeMap{
				"link_man_company": detailTask.LinkManCompany,
				"link_man":         detailTask.LinkMan,
				"call_number":      detailTask.CallNumber,
				"words":            promptContent.Prologue,
				"variable_list":    lastValidateMap,
			})
			noticeWords := biz.ReplaceVariable(lib.TypeMap{
				"link_man_company": detailTask.LinkManCompany,
				"link_man":         detailTask.LinkMan,
				"call_number":      detailTask.CallNumber,
				"words":            promptContent.NoticeWords,
				"variable_list":    lastValidateMap,
			})
			// 基础数据变量替换
			kb := worker.KnowledgeBaseList
			if task.KnowledgeBaseList != nil && len(task.KnowledgeBaseList) > 0 {
				kb = task.KnowledgeBaseList
			}

			if kb != nil {
				baseDataStr := "【补充数据】\r\n"
				for i := range kb {
					var baseData []biz.BaseDataDeployEntity
					tp.CommonUsecase.DB().Model(&biz.BaseDataDeployEntity{}).Where("source_id = ? ", kb[i].KbId).Find(&baseData)
					for m := range baseData {
						if baseData[m].Type == 1 {
							prompt = strings.ReplaceAll(prompt, baseData[m].Key, baseData[m].Value)
							firstWord = strings.ReplaceAll(firstWord, baseData[m].Key, baseData[m].Value)
						}
						baseDataStr += fmt.Sprintf("%s：%s\r\n", baseData[m].Key, baseData[m].Value)
					}
				}
				prompt += baseDataStr
			}

			tp.log.Info(fmt.Sprintf("呼叫任务号码%s; 数据：%s", detailTask.CallNumber, lib.InterfaceToString(voice)))

			llmOptions := &biz.LlmOptions{Model: ""}

			var llmConfig biz.LlmConfig
			if typeMap.GetInt("show_main_model") == 2 {
				llmConfig = tp.getSlaveModelConfig(promptTemplateCategory, llmOptions)
			} else {
				llmConfig = tp.getModelConfig(promptTemplateCategory, llmOptions, typeMap)
			}

			if llmConfig.ApiKey == "" {
				return lib.TypeMap{"code": 0, "errMsg": "noKey", "message": "没有可用key"}
			}

			data := struct {
				CallNumber   string        `json:"call_number"`
				FirstWord    string        `json:"first_word"`
				OutPutNumber string        `json:"out_put_number"`
				Ms           string        `json:"ms"`
				Speed        float32       `json:"speed"`
				Prompt       string        `json:"prompt"`
				Voice        string        `json:"voice"`
				Lang         int32         `json:"lang"`
				ServiceName  string        `json:"service_name"`
				Volcengine   string        `json:"volcengine"`
				Cluster      string        `json:"cluster"`
				SpeedRatio   float32       `json:"speedRatio"`
				Summary      string        `json:"summary"`
				LlmConfig    biz.LlmConfig `json:"llmConfig"`
			}{
				CallNumber:   detailTask.CallNumber,
				FirstWord:    firstWord,
				Prompt:       prompt,
				OutPutNumber: task.OutputCallNumber,
				Ms:           voice.Type,
				Speed:        worker.Speed,
				Voice:        voice.VoiceName,
				Lang:         voice.Lang,
				Volcengine:   voice.Type,
				Cluster:      "volcano_tts",
				SpeedRatio:   worker.Speed,
				Summary:      promptContent.ResponseData,
				LlmConfig:    llmConfig,

				//speechNoiseThreshold: 1,
				//VolumeRatio:          0.9,
				//PitchRatio:           1.0,
				//SilenceMs:            300,
				//SilenceDuration:      256,
				//noInputTimeout:       5000,
				//speechTimeout:        6000,
				//nobreakTimeout:       30000,
				//partialEvents:        true,
			}

			// 火山单独复刻处理
			if voice.Type == "volcengine" && voice.VoiceMode == "custom__llm_copy" {
				data.Cluster = "volcano_mega"
			}

			// 猎聘音色单独处理
			dataMap := lib.ToTypeMap(data)
			dataMap.Set("llmOptions.model", llmOptions.Model)
			var speechCraftContent biz.PromptContent
			json.Unmarshal([]byte(speechCraft.Content), &speechCraftContent)
			if speechCraftContent.MaxTokens > 0 {
				dataMap.Set("llmOptions.max_tokens", speechCraftContent.MaxTokens)
			} else if promptTemplateCategory.Content.MaxTokens > 0 {
				dataMap.Set("llmOptions.max_tokens", promptTemplateCategory.Content.MaxTokens)
			}
			if speechCraftContent.Temperature > 0 {
				dataMap.Set("llmOptions.temperature", speechCraftContent.Temperature)
			} else if promptTemplateCategory.Content.Temperature > 0 {
				dataMap.Set("llmOptions.temperature", promptTemplateCategory.Content.Temperature)
			}
			if speechCraftContent.TopP > 0 {
				dataMap.Set("llmOptions.top_p", speechCraftContent.TopP)
			} else if promptTemplateCategory.Content.TopP > 0 {
				dataMap.Set("llmOptions.top_p", promptTemplateCategory.Content.TopP)
			}
			if speechCraftContent.FrequencyPenalty > 0 {
				dataMap.Set("llmOptions.frequency_penalty", speechCraftContent.FrequencyPenalty)
			} else if promptTemplateCategory.Content.FrequencyPenalty > 0 {
				dataMap.Set("llmOptions.frequency_penalty", promptTemplateCategory.Content.FrequencyPenalty)
			}

			if voice.Type == "minimax" {
				dataMap.Set("volcengine", "minimaxv2")
				dataMap.Set("minimax", lib.TypeMap{"speed": worker.Speed, "voice": voice.VoiceName, "group_id": "", "model": "speech-01-240228", "volume": 1.0, "pitch": 0})
			}

			// 腾讯音色单独处理
			if voice.Type == "tencent" {
				dataMap.Set("volcengine", "tencent")
				dataMap.Set("tencent", lib.TypeMap{"fastVoiceType": voice.VoiceName})
			}
			dataMap.Set("voice_mode", voice.VoiceMode)

			// 处理设置
			dataMap.Set("worker_id", worker.ID)
			dataMap.Set("company_id", detailTask.CompanyId)
			if worker.Setting.Pitch != 0 {
				dataMap.Set("pitchRatio", worker.Setting.Pitch)
			}
			if worker.Setting.Volume != 0 {
				dataMap.Set("volumeRatio", worker.Setting.Volume)
			}
			if worker.Setting.SilentStrategy.Time != 0 && worker.Setting.SilentStrategy.Script != "" {
				dataMap.Set("silent_strategy", worker.Setting.SilentStrategy)
			}
			if worker.Setting.AfkStrategy.Time != 0 && worker.Setting.AfkStrategy.Script != "" {
				dataMap.Set("afk_strategy", worker.Setting.AfkStrategy)
			}
			if worker.Setting.SilenceDuration != 0 {
				dataMap.Set("silenceDuration", worker.Setting.SilenceDuration)
			}
			if worker.Setting.BreakDelay != -1 {
				dataMap.Set("break_delay", worker.Setting.BreakDelay)
			}
			if worker.Setting.NoInputTimeout != 0 {
				dataMap.Set("noInputTimeout", worker.Setting.NoInputTimeout)
			}
			if worker.Setting.NoBreakRound != 0 {
				dataMap.Set("no_break_round", worker.Setting.NoBreakRound)
			}
			if worker.Setting.BreakPolicy == 1 {
				dataMap.Set("break_policy", "sentence")
			}
			if worker.Setting.NoBreakAsrIgnore == 1 {
				dataMap.Set("no_break_asr_ignore", 1)
			}

			//if worker.Setting.BufferKeepMsFirst != 0 {
			//	dataMap.Set("bufferKeepMsFirst", worker.Setting.BufferKeepMsFirst)
			//}
			if worker.KnowledgeBaseList != nil && len(worker.KnowledgeBaseList) > 0 {
				dataMap.Set("knowledge_base_list", worker.KnowledgeBaseList)
			}
			// 如果任务中有知识库使用任务中知识库
			if task.KnowledgeBaseList != nil && len(task.KnowledgeBaseList) > 0 {
				dataMap.Set("knowledge_base_list", task.KnowledgeBaseList)
			}
			// 知识库保存下来
			if dataMap.Get("knowledge_base_list") != nil {
				tp.CommonUsecase.DB().Model(&biz.SaleCallTaskDetailEntity{}).Where(detailTask.ID).Update("knowledge_base_list", dataMap.GetString("knowledge_base_list"))
			}
			if worker.Setting.NoBreakGlobal != 0 {
				if worker.Setting.NoBreakGlobal == 1 {
					dataMap.Set("no_break_global", true)
				}
			}
			if worker.Setting.UseBgm != 0 {
				if worker.Setting.UseBgm == 1 {
					dataMap.Set("use_bgm", true)
				}
			}
			if worker.Setting.CallTimeMax != 0 {
				dataMap.Set("call_time_max", worker.Setting.CallTimeMax)
			}

			//asr设置
			if promptTemplateCategory.Content.AsrEngine != "" {
				dataMap.Set("asrEngine", promptTemplateCategory.Content.AsrEngine)
			}
			if promptTemplateCategory.Content.SilenceMs != 0 {
				dataMap.Set("silenceMs", promptTemplateCategory.Content.SilenceMs)
			}
			if promptTemplateCategory.Content.HotWords != "" {
				dataMap.Set("hotWords", promptTemplateCategory.Content.HotWords)
			}

			// 线路逻辑
			line := tp.getCallLine(task)
			if line != nil {
				if line.GetString("dial") != "" {
					dataMap.Set("dial", line.GetString("dial"))
				}
				if line.GetString("cid_number") != "" {
					dataMap.Set("cid_number", line.GetString("cid_number"))
				}
				if line.GetInt("call_line_id") != 0 {
					dataMap.Set("call_line_id", line.GetInt("call_line_id"))
				}
			}

			// 指定线路
			if detailTask.CallLineId != 0 {
				var detailCallLine biz.CompanyCallLinesDeployEntity
				tp.CommonUsecase.DB().Model(&biz.CompanyCallLinesDeployEntity{}).Where("status = 1 and id = ?", detailTask.CallLineId).First(&detailCallLine)
				if detailCallLine.ID != 0 {
					dataMap.Set("dial", detailCallLine.LineTag)
					dataMap.Set("cid_number", detailCallLine.SipUserName)
					dataMap.Set("call_line_id", detailCallLine.ID)
					dataMap.Set("line_id", detailCallLine.CallLinesId)
				}
			}

			// 测试线路
			if typeMap.GetString("dial") != "" && typeMap.GetString("cid_number") != "" {
				dataMap.Set("dial", typeMap.GetString("dial"))
				dataMap.Set("cid_number", typeMap.GetString("cid_number"))
				dataMap.Set("call_line_id", typeMap.GetInt("call_line_id"))
				dataMap.Set("line_id", typeMap.GetInt("line_id"))
			}

			// 标签返回
			if len(speechCraft.ResponseLabels) >= 0 {
				summary, outputRequirements := tp.GetLastResponse(speechCraft.ResponseLabels, promptTemplateCategory.Content.Intention)
				summary = biz.ReplaceVariable(lib.TypeMap{
					"link_man_company": detailTask.LinkManCompany,
					"link_man":         detailTask.LinkMan,
					"call_number":      detailTask.CallNumber,
					"words":            summary,
					"variable_list":    lastValidateMap,
				})
				dataMap.Set("summary", outputRequirements+summary)
				dataMap.Set("prompt", dataMap.GetString("prompt"))
			}
			if firsWordVoicePath != "" {
				dataMap.Set("first_word_audio", firsWordVoicePath)
			}

			// 如果是通知，走通知逻辑
			if speechCraft.SceneType == 2 {
				var noticeWordVoice biz.FirstWordVoiceDeployEntity
				tp.CommonUsecase.DB().Model(&biz.FirstWordVoiceDeployEntity{}).First(&noticeWordVoice, "id = ?", speechCraft.Extend.NoticeWordsVoiceId)
				dataMap.Set("notice_word", noticeWords)
				if noticeWordVoice.ID != 0 {
					dataMap.Set("notice_audio", noticeWordVoice.Path)
				}
			}
			// 获取公司名称
			var company biz.CompanyDeployEntity
			tp.CommonUsecase.DB().Model(&biz.CompanyDeployEntity{}).Where("id = ?", task.CompanyId).First(&company)
			if company.ID != 0 {
				dataMap.Set("company_name", company.Name)
			}
			dataMap.Set("outside_call_type", "批量外呼")
			dataMap.Set("task_name", task.TaskName)
			if task.IsTest == 1 {
				dataMap.Set("outside_call_type", "测试外呼")
			}

			dataMap.Set("break_ignore_regex", promptTemplateCategory.Content.BreakIgnoreRegex)

			// 区号
			var callLine biz.CallLinesDeployEntity
			tp.CommonUsecase.DB().Model(&biz.CallLinesDeployEntity{}).Where("id = ?", detailTask.LineId).First(&callLine)
			if callLine.AreaCode != "" {
				dataMap.Set("call_number", callLine.AreaCode+detailTask.CallNumber)
			}

			dataMap.Set("silenceMs", promptTemplateCategory.Content.SilenceMs)
			dataMap.Set("break_ignore_regex", promptTemplateCategory.Content.BreakIgnoreRegex)

			//	只返回prompt
			if typeMap.GetString("is_only_param") == "1" {
				return dataMap
			}

			bytes, _ := json.Marshal(dataMap)
			tp.log.Info(fmt.Sprintf("呼叫任务号码：%s; 数据：%s", data.CallNumber, string(bytes)))
			var r *string
			// 接口地址
			url := fmt.Sprintf("http://%s/v3/call", tp.conf.CallServer.GetUrl())
			if task.IsTest == 2 && biz.LimitCompany(task.CompanyId) {
				tp.CommonUsecase.Rdb().HIncrBy(&gin.Context{}, fmt.Sprintf("task_%d", task.ID), "caps", -1)
				key := fmt.Sprintf("modelAndLine:{\"bid\":%d,\"line\":%d}", llmConfig.BlendDeployId, dataMap.GetInt("line_id"))
				tp.CommonUsecase.Rdb().HIncrBy(&gin.Context{}, key, fmt.Sprintf("taskSource:%d:caps", task.ID), -1)
			}
			// 发起请求
			r, err := lib.HTTPJsonWithHeaders("POST", url, bytes, nil)
			lib.Try(func() {
				if err != nil {
					// 失败处理机制
					tp.CommonUsecase.DB().Model(&biz.SaleCallTaskDetailEntity{}).Where("id = ?", detailTask.ID).Update("status", 1)
					// 释放资源
					if task.IsTest == 2 && biz.LimitCompany(task.CompanyId) {
						biz.ReleaseSource(tp.CommonUsecase.Rdb(), task, llmConfig.BlendDeployId, dataMap.GetInt("line_id"), tp.log)
					}
					tp.log.Info(fmt.Sprintf("呼叫任务号码：%s; 任务失败：%s; 数据：%s", data.CallNumber, err, lib.InterfaceToString(dataMap)))
					panic("服务暂不可用")
				}
				if r == nil {
					// 失败处理机制
					tp.CommonUsecase.DB().Model(&biz.SaleCallTaskDetailEntity{}).Where("id = ?", detailTask.ID).Update("status", 1)
					// 释放资源
					if task.IsTest == 2 && biz.LimitCompany(task.CompanyId) {
						biz.ReleaseSource(tp.CommonUsecase.Rdb(), task, llmConfig.BlendDeployId, dataMap.GetInt("line_id"), tp.log)
					}
					tp.log.Info(fmt.Sprintf("呼叫任务号码：%s; 任务返回空; 数据：%s", data.CallNumber, lib.InterfaceToString(dataMap)))
					panic("服务暂不可用")
				}

				// 更新call_id
				responseMap := lib.ToTypeMapByString(*r)
				if responseMap.GetInt("code") == 1 {
					tp.log.Info(fmt.Sprintf("呼叫任务号码：%s; 接口调用成功，返回数据：%s", data.CallNumber, *r))
					// 加一条回调记录
					tp.CommonUsecase.DB().Model(&biz.CallbackRecordEntity{}).Create(&biz.CallbackRecordEntity{CallTime: time.Now().Unix(), LineId: dataMap.GetInt("line_id"), BlendDeployId: llmConfig.BlendDeployId, CallType: 1, DetailId: int64(detailTask.ID), ExtendFormat: "{}", CallId: responseMap.GetString("data")})
					tp.CommonUsecase.DB().Model(&biz.SaleCallTaskDetailEntity{}).Where("id = ?", detailTask.ID).Updates(&biz.SaleCallTaskDetailEntity{CallId: responseMap.GetString("data"), Result: lib.InterfaceToString(*r)})
				} else {
					// 释放资源
					if task.IsTest == 2 && biz.LimitCompany(task.CompanyId) {
						biz.ReleaseSource(tp.CommonUsecase.Rdb(), task, llmConfig.BlendDeployId, dataMap.GetInt("line_id"), tp.log)
					}
					tp.log.Info(fmt.Sprintf("呼叫任务号码：%s; 接口调用失败，返回数据：%s", data.CallNumber, *r))
					// 失败处理机制
					tp.CommonUsecase.DB().Model(&biz.SaleCallTaskDetailEntity{}).Where("id = ?", detailTask.ID).Update("status", 1)
				}
			}, func(err interface{}) {
				// 异常重试机制
				tp.log.Info(fmt.Sprintf("呼叫任务号码：%s; 呼叫异常，进行重试 异常%s", data.CallNumber, lib.InterfaceToString(err)))
				tp.CommonUsecase.DB().Model(&biz.SaleCallTaskDetailEntity{}).Where("id = ?", detailTask.ID).Update("status", 1)
			})
			return lib.ToTypeMapByString(lib.InterfaceToString(*r))
		}
		break
	}
	return lib.TypeMap{}
}
