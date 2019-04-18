package cron

import (
	"github.com/open-falcon/common/model"
	"github.com/open-falcon/sdk/graph"
)

// 通过query接口查询相关指标最新监控值
func queryCounterLast(numeratorOperands, denominatorOperands, hostnames []string, begin, end int64) (map[string]float64, error) {
	counters := []string{}
	for _, counter := range numeratorOperands {
		counters = append(counters, counter)
	}

	for _, counter := range denominatorOperands {
		counters = append(counters, counter)
	}

	params := []*model.GraphLastParam{}
	counterSize := len(counters)
	hostnameSize := len(hostnames)

	for i := 0; i < counterSize; i++ {
		for j := 0; j < hostnameSize; j++ {
			params = append(params, &model.GraphLastParam{Endpoint: hostnames[j], Counter: counters[i]})
		}
	}

	// sdk通过query接口批量获取指标最新值
	resp, err := graph.Lasts(params)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]float64)
	for _, res := range resp {
		v := res.Value
		if v.Timestamp < begin || v.Timestamp > end {
			continue
		}
		ret[res.Endpoint+res.Counter] = float64(v.Value)
	}

	return ret, nil
}
