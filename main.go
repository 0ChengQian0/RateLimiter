package main

import (
	"bufio"
	"context"
	"fmt"

	//"monilive/internal/biz"
	"monilive/internal/biz/multi_tasks_system"
	"os"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	// 数据库连接（需要替换为真实的数据库链接）
	dsn := "username:password@tcp(127.0.0.1:3306)/database_name?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(" 连接数据库失败")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 初始化 TaskProcessor（如果不使用 RateLimiter，可以不传）
	taskProcessor := &multi_tasks_system.TaskProcessor{}

	// 启动刷新任务
	taskProcessor.StartWorkerInfoRefresher(ctx, db)

	fmt.Println(" 刷新任务已启动，输入 show 查看数据，stop 停止任务")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())

		switch text {
		case "stop":
			cancel()
			fmt.Println(" 刷新任务已停止，程序退出")
			return

		case "show":
			fmt.Println(" 当前缓存的 Worker 数据：")
			taskProcessor.WorkerInfoMap.Range(func(key, value any) bool {
				if info, ok := value.(*multi_tasks_system.WorkerInfo); ok {
					fmt.Printf("WorkerID: %d, TemplateID: %d, CategoryName: %s, VoiceGender: %s\n",
						info.Worker.ID,
						info.PromptTemplate.ID,
						info.Category.CategoryName,
						info.Voice.Gender,
					)
				}
				return true
			})

		default:
			fmt.Println(" 未知命令，请输入 show 或 stop")
		}
	}
}
