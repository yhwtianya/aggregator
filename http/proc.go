package http

import (
	"net/http"

	"github.com/open-falcon/aggregator/db"
)

func configProcRoutes() {
	// 获取cluster表里符合ids的记录，内容为cluster里配置的监控信息
	http.HandleFunc("/items", func(w http.ResponseWriter, r *http.Request) {
		items, err := db.ReadClusterMonitorItems()
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		for _, v := range items {
			w.Write([]byte(v.String()))
			w.Write([]byte("\n"))
		}
	})
}
