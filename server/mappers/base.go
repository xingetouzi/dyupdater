package mappers

import (
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/utils"
)

var log = utils.AppLogger

//FactorNameMapper 负责处理因子源中ID和因子存储ID中的映射。
//FactorNameMapper.Map,接受一个因子在因子源中的ID，返回其在因子存储中对应的ID。
type FactorNameMapper interface {
	common.Configable
	Map(store string, factorID string) string
}
