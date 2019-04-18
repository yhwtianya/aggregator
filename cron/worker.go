package cron

import (
	"log"
	"time"

	"github.com/open-falcon/aggregator/g"
)

// cluster监控每个item定期轮询结构
type Worker struct {
	Ticker      *time.Ticker
	ClusterItem *g.Cluster
	Quit        chan struct{}
}

// 创建worker
func NewWorker(ci *g.Cluster) Worker {
	w := Worker{}
	w.Ticker = time.NewTicker(time.Duration(ci.Step) * time.Second)
	w.Quit = make(chan struct{})
	w.ClusterItem = ci
	return w
}

// worker周期性轮询cluster item
func (this Worker) Start() {
	go func() {
		for {
			select {
			case <-this.Ticker.C:
				// 定期运行item监控
				WorkerRun(this.ClusterItem)
			case <-this.Quit:
				if g.Config().Debug {
					log.Println("[I] drop worker", this.ClusterItem)
				}
				this.Ticker.Stop()
				return
			}
		}
	}()
}

// 停止轮询
func (this Worker) Drop() {
	close(this.Quit)
}

// 记录所有workers
var Workers = make(map[string]Worker)

// 如果数据库cluster item已经删除或更新，则停止并删除该worker
func deleteNoUseWorker(m map[string]*g.Cluster) {
	del := []string{}
	for key, worker := range Workers {
		if _, ok := m[key]; !ok {
			worker.Drop()
			del = append(del, key)
		}
	}

	for _, key := range del {
		delete(Workers, key)
	}
}

// 如果数据库cluster item有更新或新增，则创建并运行该worker
func createWorkerIfNeed(m map[string]*g.Cluster) {
	for key, item := range m {
		if _, ok := Workers[key]; !ok {
			worker := NewWorker(item)
			Workers[key] = worker
			worker.Start()
		}
	}
}
