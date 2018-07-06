package services

import (
	"time"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/robfig/cron"
	"github.com/spf13/viper"
)

type CronTask struct {
	Rule       string `mapstructure:"rule"`
	Start      int    `mapstructure:"start"`
	End        int    `mapstructure:"end"`
	StartDelta int    `mapstructrue:"startDelta"`
	EndDelta   int    `mapstructrue:"endDelta"`
	Replace    bool   `mapstructure:"replace"`
}

// CronService 是定时任务模块，负责根据配置定时运行检查因子任务,
// 每一个因子检查的任务的配置包含以下几个选项：
//   rule: 字符，定义了运行任务的时间规则，见 https://crontab.guru
//   start: 检查任务的日期范围的开始日期，格式形如"20060102"
//   startDelta: 检查任务的日期范围的开始日期距离当前时间的秒数
//   end: 检查任务的日期范围的结束日期，格式形如"20060102"
//   endDelta: 检查任务的日期范围的结束日期距离当前时间的秒数
// 如果end和endDelta都为0的话，默认检查日期范围截止到昨天。
//
// 定时任务的配置例子如下(yaml格式)：
/*
  cron:
    enabled: true
    tasks:
    # 定时任务everyday，将在每周一至周五的5:00 ~ 8:59分每5分钟运行一次，检查范围为上周到昨天截止
    # crontab的规则定义可以参考 https://crontab.guru
    everyday:
      # At every 5th minute past every hour from 5 through 8 on every day-of-week from Monday through Friday.
      rule: "0 *\/5 5-8 * * 1-5"
      # start time from now , in seconds, 604800 = 7*24*60*60 = 7days.
      startDelta: 604800
    # 定时任务everyweekends，将在每个周末每隔8小时运行一次，检查范围为从20100101开始到昨天截止
    everyweekends:
      # past every 8th hour on Sunday and Saturday.
      rule: "0 0 *\/8 \* \* 0,6"
      # start time in format "20060102"
      start: 20100101
*/
type CronService struct {
	common.BaseComponent
	fs      *FactorServices
	tasks   map[string]*CronTask
	queue   chan *CronTask
	cron    *cron.Cron
	running bool
}

func (cs *CronService) Init(config *viper.Viper) {
	cs.BaseComponent.Init(config)
	cs.tasks = make(map[string]*CronTask)
	if config.IsSet("tasks") {
		tasksConfig := config.Sub("tasks")
		err := tasksConfig.Unmarshal(&cs.tasks)
		if err != nil {
			panic(err)
		}
	}
}

func (cs *CronService) worker() {
	for cs.running {
		select {
		case ct := <-cs.queue:
			{
				now := time.Now()
				dr := models.DateRange{}
				dr.Start = ct.Start
				dr.End = ct.End
				var err error
				if ct.Start == 0 && ct.StartDelta != 0 {
					dr.Start, err = utils.Datetoi(now.Add(-time.Duration(ct.StartDelta) * time.Second))
					if err != nil {
						log.Error(err.Error())
						continue
					}
				}
				if ct.Start == 0 && ct.StartDelta == 0 {
					dr.Start = utils.GetGlobalConfig().GetCalStartDate()
				}
				if ct.End == 0 && ct.EndDelta != 0 {
					dr.End, err = utils.Datetoi(now.Add(-time.Duration(ct.EndDelta) * time.Second))
					if err != nil {
						log.Error(err.Error())
						continue
					}
				}
				cs.fs.CheckAll(dr)
				// cs.fs.Wait(3600)
			}
		case <-time.After(10 * time.Second):
			{
				continue
			}
		default:
		}
	}
}

func (cs *CronService) publish(ct *CronTask) {
	for {
		select {
		case cs.queue <- ct:
			return
		default:
			<-cs.queue
			log.Warning("Cron task queue is full, earlier task was aborded!")
		}
	}
}

func (cs *CronService) Run() {
	cs.running = true
	if !cs.IsEnabled() {
		return
	}
	log.Debugf("Schedule cron tasks.")
	cs.cron = cron.New()
	cs.queue = make(chan *CronTask, 100)
	go cs.worker()
	for k, ct := range cs.tasks {
		func(k string, ct *CronTask) {
			cs.cron.AddFunc(ct.Rule, func() {
				log.Debugf("Publish a cron task: %s.", k)
				cs.publish(ct)
			})
		}(k, ct)
	}
	cs.cron.Start()
}

func (cs *CronService) Close() {
	cs.running = false
	cs.cron.Stop()
	close(cs.queue)
}

func NewCronService(fs *FactorServices) *CronService {
	cs := &CronService{}
	cs.fs = fs
	var config *viper.Viper
	if viper.IsSet("cron") {
		config = viper.Sub("cron")
	} else {
		config = viper.New()
	}
	cs.Init(config)
	return cs
}
