package calculators

import (
	"encoding/json"
	"errors"

	"fxdayu.com/dyupdater/server/celery"
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/schedulers"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/spf13/viper"
)

type celeryCalculatorConfig struct {
	Celery      string `mapstructrue:"celery"`
	TaskCal     string `mapstructrue:"tasks.cal.name"`
	TaskProcess string `mapstructrue:"tasks.process.name"`
}

// CeleryCalculator 将因子计算任务发送给celery并监控任务运行。
// CeleryCalculator的配置项如下：
//   celery: 对应celery配置中的某个celery实例的名字，如果配置不正确会报错退出。
//   task: celery中用于因子计算的任务名(一般无需改动)
type CeleryCalculator struct {
	common.BaseComponent
	config       *celeryCalculatorConfig
	helper       *celery.CeleryHelper
	queueCal     *celery.CeleryQueue
	queueProcess *celery.CeleryQueue
	running      bool
	outCal       chan task.TaskResult
	outProcess   chan task.TaskResult
}

type asyncApplyResult struct {
	TaskID string `json:"task-id"`
}

func (calculator *CeleryCalculator) Init(config *viper.Viper) {
	calculator.BaseComponent.Init(config)
	calculator.config = &celeryCalculatorConfig{
		Celery:      "default",
		TaskCal:     "factor.cal",
		TaskProcess: "factor.process",
	}
	config.Unmarshal(calculator.config)
	calculator.running = true
	service := celery.GetCeleryService()
	calculator.helper = service.MustGet(calculator.config.Celery)
	calculator.queueCal = calculator.helper.GetQueue(calculator.config.TaskCal, "calculator-celery-cal")
	calculator.queueProcess = calculator.helper.GetQueue(calculator.config.TaskProcess, "calculator-celery-process")
	go calculator.handleResultCal()
	go calculator.handleResultProcess()
}

func (calculator *CeleryCalculator) Cal(id string, factor models.Factor, dateRange models.DateRange) error {
	data := map[string][]interface{}{"args": []interface{}{factor, dateRange.Start, dateRange.End}}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = calculator.queueCal.Publish(id, jsonData)
	return err
}

func (calculator *CeleryCalculator) Process(id string, factor models.Factor, factorValue models.FactorValue, processType task.FactorProcessType) error {
	factorValueStr, err := utils.PackFactorValue(factorValue)
	if err != nil {
		return err
	}
	data := map[string][]interface{}{"args": []interface{}{factor.Name, factorValueStr, string(processType)}}
	jsonData, err := json.Marshal(data)
	_, err = calculator.queueProcess.Publish(id, jsonData)
	return err
}

func (calculator *CeleryCalculator) Subscribe(s schedulers.TaskScheduler) {
	calculator.outCal = s.GetOutputChan(int(task.TaskTypeCal))
	calculator.outProcess = s.GetOutputChan(int(task.TaskTypeProcess))
}

type ResultResult struct {
	TaskID string `json:"task-id"`
	State  string `json:"state"`
	Result string `json:"result"`
}

func (calculator *CeleryCalculator) handleResultCal() {
	for celeryResult := range calculator.queueCal.Output {
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
			err := utils.ParseFactorValue(rep.Result, &data)
			result := task.CalTaskResult{FactorValue: data}
			if err != nil {
				log.Error(err.Error())
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
		calculator.outCal <- *ret
	}
}

func (calculator *CeleryCalculator) handleResultProcess() {
	for celeryResult := range calculator.queueProcess.Output {
		rep := celeryResult.Response
		var ret *task.TaskResult
		if rep.Error != "" {
			result := task.ProcessTaskResult{FactorValue: models.FactorValue{}}
			ret = &task.TaskResult{
				ID:     celeryResult.ID,
				Type:   task.TaskTypeProcess,
				Result: result,
				Error:  errors.New(rep.Error),
			}
		} else {
			data := models.FactorValue{}
			err := utils.ParseFactorValue(rep.Result, &data)
			result := task.ProcessTaskResult{FactorValue: data}
			if err != nil {
				log.Error(err.Error())
				ret = &task.TaskResult{
					ID:     celeryResult.ID,
					Type:   task.TaskTypeProcess,
					Result: result,
					Error:  err,
				}
			}
			ret = &task.TaskResult{
				ID:     celeryResult.ID,
				Type:   task.TaskTypeProcess,
				Result: result,
				Error:  nil,
			}
		}
		calculator.outProcess <- *ret
	}
}

func (calculator *CeleryCalculator) Close() {
	calculator.running = false
}
