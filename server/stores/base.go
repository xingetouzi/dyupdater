/*Package stores 定义了FactorStore(因子存储)接口。

实现了该接口的类可以通过services.RegisterStore方法注册，
注册后即可在配置中通过type字段指定。

已有的实现：
  MongoStore,注册名mongo
  CSVStore,注册名csv
  OracleStore，注册名oracle
  HDF5Store，注册名hdf5
因子存储的配置的key为stores，值为一个字典，字典key对应因子存储的名字(用于区分不同的store)
字典的value有以下字段可以配置：
  enabled: bool型，表示该因子存储类是否生效，不生效的因子存储在服务启动后会实例化但不会接收处理对应任务。
  type: 因子存储类注册的名字
  ...: 对应因子存储类的其他可配置项，见该因子存储类的文档。
因子存储的配置例子(yaml格式):
  stores:
    mongo:
      type: mongo
      enabled: true
      url: mongodb://127.0.0.1
      db: fxdayu_factors
    local:
      type: csv
      enabled: false
      path: $HOME/.fxdayu/factors/
    oracle:
      type: oracle
      enabled: true
      url: FXDAYU/FXDAYU@localhost:1521/xe
    hdf5:
      type: hdf5
      celery: default # 需要在celery中配置对应名字的celery实例
*/
package stores

import (
	"fmt"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
)

var log = utils.AppLogger

// FactorStore define the interface of factor store, which store the factor data
// and can check and update it.
type FactorStore interface {
	common.Component
	Update(models.Factor, models.FactorValue, bool) (int, error)
	Check(models.Factor, []int) ([]int, error)
	Fetch(models.Factor, models.DateRange) (models.FactorValue, error)
}

type BaseFactorStore struct {
}

func (bfs *BaseFactorStore) Fetch(models.Factor, models.DateRange) (models.FactorValue, error) {
	return models.FactorValue{}, fmt.Errorf("FactorStore's Get method is not implement")
}
