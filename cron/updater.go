package cron

import (
	"time"

	"github.com/open-falcon/aggregator/db"
	"github.com/open-falcon/aggregator/g"
)

// 周期性读取最新cluster items，用于更新worker
func UpdateItems() {
	for {
		updateItems()
		d := time.Duration(g.Config().Database.Interval) * time.Second
		time.Sleep(d)
	}
}

// 读取最新cluster items配置，更新worker
func updateItems() {
	items, err := db.ReadClusterMonitorItems()
	if err != nil {
		return
	}

	// 删除不存在的item对应的worker
	deleteNoUseWorker(items)
	// 添加新增的item对应的worker
	createWorkerIfNeed(items)
}
