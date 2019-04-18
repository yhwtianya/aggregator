package cron

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/open-falcon/aggregator/g"
	"github.com/open-falcon/sdk/portal"
	"github.com/open-falcon/sdk/sender"
)

// 计算cluster item表达式的值，并将数据推送到transfer
func WorkerRun(item *g.Cluster) {
	debug := g.Config().Debug

	// 清除表达式空格
	numeratorStr := cleanParam(item.Numerator)
	denominatorStr := cleanParam(item.Denominator)

	// 表达式合法性检查
	if !expressionValid(numeratorStr) || !expressionValid(denominatorStr) {
		log.Println("[W] invalid numerator or denominator", item)
		return
	}

	// 是否需要计算表达式
	needComputeNumerator := needCompute(numeratorStr)
	needComputeDenominator := needCompute(denominatorStr)

	if !needComputeNumerator && !needComputeDenominator {
		// 分子分母均为纯静态数字，监控没有意义，不向transfer推送数据
		log.Println("[W] no need compute", item)
		return
	}

	// 解析运算项和运算符
	numeratorOperands, numeratorOperators := parse(numeratorStr, needComputeNumerator)
	denominatorOperands, denominatorOperators := parse(denominatorStr, needComputeDenominator)

	// 运算符合法性检查
	if !operatorsValid(numeratorOperators) || !operatorsValid(denominatorOperators) {
		log.Println("[W] operators invalid", item)
		return
	}

	// 获取对应组下的主机列表
	hostnames, err := portal.Hostnames(fmt.Sprintf("%d", item.GroupId))
	if err != nil || len(hostnames) == 0 {
		return
	}

	now := time.Now().Unix()

	// 通过query接口查询相关指标最新监控值
	valueMap, err := queryCounterLast(numeratorOperands, denominatorOperands, hostnames, now-int64(item.Step*2), now)
	if err != nil {
		log.Println("[E]", err, item)
		return
	}

	var numerator, denominator float64
	var validCount int

	// 计算所有主机指标之和
	for _, hostname := range hostnames {
		var numeratorVal, denominatorVal float64
		var (
			numeratorValid   = true
			denominatorValid = true
		)

		if needComputeNumerator {
			// 计算分子值
			numeratorVal, numeratorValid = compute(numeratorOperands, numeratorOperators, hostname, valueMap)
			if !numeratorValid && debug {
				log.Printf("[W] [hostname:%s] [numerator:%s] invalid or not found", hostname, item.Numerator)
			}
		}

		if needComputeDenominator {
			// 计算分子值
			denominatorVal, denominatorValid = compute(denominatorOperands, denominatorOperators, hostname, valueMap)
			if !denominatorValid && debug {
				log.Printf("[W] [hostname:%s] [denominator:%s] invalid or not found", hostname, item.Denominator)
			}
		}

		if numeratorValid && denominatorValid {
			// 对每个主机的值计算和
			numerator += numeratorVal
			denominator += denominatorVal
			validCount += 1
		}
	}

	// 计算$#表达式的值
	if !needComputeNumerator {
		if numeratorStr == "$#" {
			numerator = float64(validCount)
		} else {
			numerator, err = strconv.ParseFloat(numeratorStr, 64)
			if err != nil {
				log.Printf("[E] strconv.ParseFloat(%s) fail %v", numeratorStr, item)
				return
			}
		}
	}

	if !needComputeDenominator {
		if denominatorStr == "$#" {
			denominator = float64(validCount)
		} else {
			denominator, err = strconv.ParseFloat(denominatorStr, 64)
			if err != nil {
				log.Printf("[E] strconv.ParseFloat(%s) fail %v", denominatorStr, item)
				return
			}
		}
	}

	// 除数为0
	if denominator == 0 {
		log.Println("[W] denominator == 0", item)
		return
	}

	// 将cluster监控数据发送到transfer
	sender.Push(item.Endpoint, item.Metric, item.Tags, numerator/denominator, item.DsType, int64(item.Step))
}

// 解析运算项和运算符
func parse(expression string, needCompute bool) (operands []string, operators []uint8) {
	if !needCompute {
		return
	}

	// e.g. $(cpu.busy)+$(cpu.idle)-$(cpu.nice)-$(cpu.guest)
	//      xx          --          --          --         x
	newExpression := expression[2 : len(expression)-1]
	// aar[0]=cpu.busy)+, aar[1]=cpu.idle)+, aar[2]=cpu.nice)+, aar[3]=cpu.guest，注意arr[3]的结尾
	arr := strings.Split(newExpression, "$(")
	count := len(arr)
	if count == 1 {
		// 如果只有一项，则aar[0]=cpu.busy
		operands = append(operands, arr[0])
		return
	}

	if count > 1 {
		for i := 0; i < count; i++ {
			item := arr[i]
			length := len(item)
			if i == count-1 {
				// 最后一项仅包含运算项，不包含运算符
				operands = append(operands, item)
				continue
			}
			operators = append(operators, item[length-1])
			// 不包含右括号
			operands = append(operands, item[0:length-2])
		}
	}

	return
}

// 删除参数中的各种空格换号
func cleanParam(val string) string {
	val = strings.TrimSpace(val)
	val = strings.Replace(val, " ", "", -1)
	val = strings.Replace(val, "\r", "", -1)
	val = strings.Replace(val, "\n", "", -1)
	val = strings.Replace(val, "\t", "", -1)
	return val
}

// 是否需要计算表达式的值
// $#
// 200
// $(cpu.busy) + $(cpu.idle)
func needCompute(val string) bool {
	if strings.Contains(val, "$(") {
		return true
	}

	return false
}

// 表达式合法性检查
func expressionValid(val string) bool {
	// 不能包含中文字符
	// use chinese character?
	if strings.Contains(val, "（") || strings.Contains(val, "）") {
		return false
	}

	return true
}

// 运算符合法性检查
func operatorsValid(ops []uint8) bool {
	count := len(ops)
	for i := 0; i < count; i++ {
		if ops[i] != '+' && ops[i] != '-' {
			return false
		}
	}
	return true
}
