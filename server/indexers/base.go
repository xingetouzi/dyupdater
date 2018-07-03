package indexers

import (
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
)

var log = utils.AppLogger

//TradingDatetimeIndexer 负责获取交易日索引数据
//TradingDatetimeIndexer.GetIndex方法接受dateRange作为参数,返回一个从小到大排序的整数数组。
//日期到整数的转换规则为：2006年01月02日 -> 20060102
type TradingDatetimeIndexer interface {
	common.Configable
	GetIndex(dateRange models.DateRange) []int
}
