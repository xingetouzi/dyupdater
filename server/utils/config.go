package utils

import (
	"strconv"

	"github.com/spf13/viper"
)

type globalConfigData struct {
	CalStartDate   int `mapstructure:"cal-start-date"`
	MinCalDuration int `mapstructure:"min-cal-duration"`
	MaxCalDuration int `mapstructrue:"max-cal-duration"`
}

// GlobalConfig 是dyupdater全局配置，主要是一些计算相关的参数。
// 配置的位置在整个配置文件中的base项。
// 可配置选项有：
//   cal-start-date # 计算开始时间，可以通过环境变量CAL_START_DATE指定，默认20100101
//   max-cal-duration # 最长计算时间,以秒为单位，计算任务会按时间被切分，最长不超过该值。可以通过环境变量MAX_CAL_DURATION指定，默认5*365*24*60*60=5年
//   min-cal-duration # 最短计算时间，以秒为单位，计算任务会填补到至少有该值的长度，可以通过环境变量MIN_CAL_DURATION指定，默认30*24*60*60=1月
// 配置示例:
/*
 base:
   cal-start-date: 20140101
   max-cal-duration: 157680000
   min-cal-duration: 2592000
*/
type GlobalConfig struct {
	config *globalConfigData
}

var globalConfigInstance *GlobalConfig

func init() {
	globalConfigInstance = &GlobalConfig{config: new(globalConfigData)}
	globalConfigInstance.config.CalStartDate, _ = strconv.Atoi(GetEnv("CAL_START_DATE", strconv.Itoa(20100101)))
	globalConfigInstance.config.MaxCalDuration, _ = strconv.Atoi(GetEnv("MAX_CAL_DURATION", strconv.Itoa(5*365*24*60*60)))
	globalConfigInstance.config.MinCalDuration, _ = strconv.Atoi(GetEnv("MIN_CAL_DURATION", strconv.Itoa(30*24*60*60)))
}

// GetGlobalConfig 用于获取全局配置
func GetGlobalConfig() *GlobalConfig {
	return globalConfigInstance
}

func (gc *GlobalConfig) Init(config *viper.Viper) {
	config.Unmarshal(gc.config)
}

func (gc *GlobalConfig) GetCalStartDate() int {
	return gc.config.CalStartDate
}

func (gc *GlobalConfig) GetMinCalDuration() int {
	return gc.config.MinCalDuration
}

func (gc *GlobalConfig) GetMaxCalDuration() int {
	return gc.config.MaxCalDuration
}

func (gc *GlobalConfig) Close() {

}
