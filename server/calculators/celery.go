package calculators

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"runtime"

	"fxdayu.com/dyupdater/server/celery"
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/schedulers"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/spf13/viper"
)

type celeryCalculatorConfig struct {
	Celery string `mapstructrue:"celery"`
	Task   string `mapstructrue:"task"`
}

// CeleryCalculator 将因子计算任务发送给celery并监控任务运行。
// CeleryCalculator的配置项如下：
//   celery: 对应celery配置中的某个celery实例的名字，如果配置不正确会报错退出。
//   task: celery中用于因子计算的任务名(一般无需改动)
type CeleryCalculator struct {
	common.BaseComponent
	config  *celeryCalculatorConfig
	helper  *celery.CeleryHelper
	queue   *celery.CeleryQueue
	running bool
	limit   chan bool
	out     chan task.TaskResult
}

type asyncApplyResult struct {
	TaskID string `json:"task-id"`
}

func (calculator *CeleryCalculator) Init(config *viper.Viper) {
	calculator.BaseComponent.Init(config)
	calculator.config = &celeryCalculatorConfig{
		Celery: "default",
		Task:   "factor.cal",
	}
	config.Unmarshal(calculator.config)
	calculator.running = true
	calculator.limit = make(chan bool, runtime.NumCPU())
	service := celery.GetCeleryService()
	calculator.helper = service.MustGet(calculator.config.Celery)
	calculator.queue = calculator.helper.GetQueue(calculator.config.Task, "calculator-celery")
	go calculator.handleResult()
}

func (calculator *CeleryCalculator) Cal(id string, factor models.Factor, dateRange models.DateRange) error {
	data := map[string][]interface{}{"args": []interface{}{factor, dateRange.Start, dateRange.End}}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = calculator.queue.Publish(id, jsonData)
	return err
}

func (calculator *CeleryCalculator) release() {
	select {
	case <-calculator.limit:
	default:
	}
}

func (calculator *CeleryCalculator) Subscribe(s schedulers.TaskScheduler) {
	calculator.out = s.GetOutputChan()
	s.AppendSuccessHandler(
		int(task.TaskTypeCal),
		func(task.TaskFuture, task.TaskResult) error {
			calculator.release()
			return nil
		},
	)
	s.AppendFailureHandler(
		int(task.TaskTypeCal),
		func(task.TaskFuture, error) {
			calculator.release()
		},
	)
}

type ResultResult struct {
	TaskID string `json:"task-id"`
	State  string `json:"state"`
	Result string `json:"result"`
}

func parseBody(s string, data *models.FactorValue) error {
	decodeBytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	err = utils.UnpackMsgpackSnappy(decodeBytes, &data.Values)
	if err != nil {
		return err
	}
	date, ok := data.Values["trade_date"]
	if !ok {
		return errors.New("No trade_date in result")
	}
	delete(data.Values, "trade_date")
	data.Datetime = make([]int, len(date))
	for i, v := range date {
		data.Datetime[i] = int(v)
	}
	data.DropNAN()
	return nil
}

func (calculator *CeleryCalculator) handleResult() {
	for celeryResult := range calculator.queue.Output {
		calculator.limit <- true
		rep := celeryResult.Response
		var ret *task.TaskResult
		if rep.Error != "" {
			result := task.CalTaskResult{FactorValue: models.FactorValue{}}
			ret = &task.TaskResult{
				ID:     celeryResult.ID,
				Type:   task.TaskTypeCal,
				Result: result,
				Error:  errors.New(rep.Error),
			}
		} else {
			data := models.FactorValue{}
			err := parseBody(rep.Result, &data)
			result := task.CalTaskResult{FactorValue: data}
			if err != nil {
				log.Error(err)
				ret = &task.TaskResult{
					ID:     celeryResult.ID,
					Type:   task.TaskTypeCal,
					Result: result,
					Error:  err,
				}
			}
			ret = &task.TaskResult{
				ID:     celeryResult.ID,
				Type:   task.TaskTypeCal,
				Result: result,
				Error:  nil,
			}
		}
		calculator.out <- *ret
	}
}

func (calculator *CeleryCalculator) Close() {
	calculator.running = false
	close(calculator.limit)
}
