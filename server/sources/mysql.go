package sources

import (
	"fmt"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	_ "github.com/go-sql-driver/mysql" // import mysql sql driver
	"github.com/spf13/viper"
)

type mysqlSourceConfig struct {
	URL   string `mapstructure:"url"`
	Table string `mapstructure:"table"`
}

// MysqlSource 从mysql数据库读取因子.
// 存放因子的表需要有以下表结构：
//   | id | name | code |
// name为字符，将被作为因子ID，code存放因子的代码，只支持单文件代码的因子。
//
// MysqlSourceConfig的配置项如下：
//   url： 数据库URL
//   table： 存放因子的表
type MysqlSource struct {
	common.BaseComponent
	config *mysqlSourceConfig
	helper *utils.SQLConnectHelper
}

// Init the factor source from config, will open mysql connections.
func (source *MysqlSource) Init(config *viper.Viper) {
	source.BaseComponent.Init(config)
	source.config = &mysqlSourceConfig{}
	config.UnmarshalExact(source.config)
	if source.config.URL == "" {
		source.config.URL = utils.GetEnv("MYSQL_URL", "root:@tcp(localhost:3306)/test")
	}
	source.helper = utils.NewSQLConnectHelper("mysql", "mysql", source.config.URL)
	log.Infof("connect to mysql host: %s\n", source.config.URL)
	source.helper.Connect()
}

// Fetch the factors from source.
func (source *MysqlSource) Fetch() []models.Factor {
	source.helper.Connect()
	con := source.helper.DB
	sqlStatement := fmt.Sprintf("SELECT name, code FROM %s order by id", source.config.Table)
	rows, err := con.Query(sqlStatement)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()
	result := make([]models.Factor, 0)

	for rows.Next() {
		factor := models.Factor{}
		err2 := rows.Scan(&factor.ID, &factor.Formula)
		// Exit if we get an error
		if err2 != nil {
			log.Panic(err2)
		}
		result = append(result, factor)
	}
	return result
}

//Close the MysqlSource, will close all mysql connections.
func (source *MysqlSource) Close() {
	source.helper.Close()
}
