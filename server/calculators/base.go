/*Package calculators 定义了FactorCalculator(因子计算器)接口。

实现了该接口的类可以通过services.RegisterCalculator方法注册，
注册后即可在配置中通过type字段指定。

已有的实现：
  CeleryCalculator,注册名celery
因子计算器的配置的key为calculators，值为一个字典，字典key对应因子计算器的名字(用于区分不同的calculator)
字典的value有以下字段可以配置：
  enabled: bool型，表示该因子计算器是否生效，不生效的计算器中的因子不会接收到计算任务。
  type: 因子计算器类注册的名字
  ...: 对应因子计算器类的其他可配置项，见该因子计算器类的文档。
因子计算器的配置例子(yaml格式):
  celery:
    default:
      host: 127.0.0.1:5555
      backend: redis://127.0.0.1/0
      broker: amqp://guest:guest@127.0.0.1
      queues:
      - factor
      - stores
  calculators:
    default:
      type: celery
      celery: default # 和celery配置中的celery实例相对应
注意：现在还不支持通过calculator名指定计算任务分配给哪一个计算器，默认会全部分配给名为default的计算器。*/
package calculators

import (
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/schedulers"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
)

var log = utils.AppLogger

// FactorCalculator 定义了因子计算器接口
type FactorCalculator interface {
	common.Component
	Cal(id string, factor models.Factor, dateRange models.DateRange) error
	Process(id string, factor models.Factor, factorValue models.FactorValue, processType task.FactorProcessType) error
	Subscribe(schedulers.TaskScheduler)
}

// DefaultCalculator 是默认使用的因子计算器类注册名
var DefaultCalculator = "default"
