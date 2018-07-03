/*Package sources 定义了FactorSource(因子代码源)接口。

实现了该接口的类可以通过services.RegisterSource方法注册，
注册后即可在配置中通过type字段指定。

已有的实现：
  MysqlSource,注册名mysql
  FileSystemSource,注册名filesystem
因子代码源的配置的key为sources，值为一个字典，字典key对应因子代码源的名字(用于区分不同的source)
字典的value有以下字段可以配置：
  enabled: bool型，表示该因子代码源是否生效，不生效的代码源中的因子不会被检查。
  type: 因子代码源类注册的名字
  ...: 对应因子代码源类的其他可配置项，见该因子代码源类的文档。
因子代码源的配置例子(yaml格式):
  sources:
    default:
      type: mysql
      url: root:root@tcp(localhost)/strategymanager
      db: strategymanager
      table: factor_factor
    local:
      type: filesystem
      path: $HOME/.fxdayu/factors-scripts
      regex: ^[A-Z0-9_]*$
*/
package sources

import (
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
)

var log = utils.AppLogger

// FactorSource defined the interface for factors source.
type FactorSource interface {
	common.Component
	Fetch() []models.Factor
}
