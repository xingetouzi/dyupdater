package indexers

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	_ "github.com/alexbrainman/odbc" // import odbc driver
	"github.com/spf13/viper"
)

type windMssqlIndexerConfig struct {
	Server string `mapstructure:"server"`
	Port   string `mapstructure:"port"`
	Db     string `mapstructrue:"db"`
	UID    string `mapstructrue:"uid"`
	Pwd    string `mapstructrue:"pwd"`
}

// WindMssqlIndexer 负责从wind的sqlserver数据库中获取交易日索引
// WindMssqlIndexer
//   server: 所连接sqlserver数据库的host地址，默认localhost
//   port: 所连接数据的端口，默认1521
//   db:  初始连接的数据库。默认master
//   uid: 用户名
//   pwd: 密码
type WindMssqlIndexer struct {
	config        *windMssqlIndexerConfig
	helper        *utils.SQLConnectHelper
	datetimeIndex []int
}

type connParams map[string]string

func defaultDriver() string {
	if runtime.GOOS == "windows" {
		return "sql server"
	} else {
		return "freetds"
	}
}

func newConnParams(config *windMssqlIndexerConfig) connParams {
	params := connParams{
		"driver":   defaultDriver(),
		"server":   config.Server,
		"database": config.Db,
	}
	if params["driver"] == "freetds" {
		params["uid"] = config.UID
		params["pwd"] = config.Pwd
		params["port"] = config.Port
		params["TDS_Version"] = "8.0"
		//params["debugflags"] = "0xffff"
	} else {
		if len(config.UID) == 0 {
			params["trusted_connection"] = "yes"
		} else {
			params["uid"] = config.UID
			params["pwd"] = config.Pwd
		}
	}
	a := strings.SplitN(params["server"], ",", -1)
	if len(a) == 2 {
		params["server"] = a[0]
		params["port"] = a[1]
	}
	return params
}

func (params connParams) makeODBCConnectionString() string {
	if port, ok := params["port"]; ok {
		params["server"] += "," + port
		delete(params, "port")
	}
	var c string
	for n, v := range params {
		c += n + "=" + v + ";"
	}
	return c
}

// Init the MongoIndexer, will create mongodb connect pool.
func (indexer *WindMssqlIndexer) Init(config *viper.Viper) {
	indexer.config = &windMssqlIndexerConfig{
		Server: "localhost",
		Port:   "1521",
		Db:     "master",
		UID:    "",
		Pwd:    "",
	}
	config.Unmarshal(indexer.config)
	params := newConnParams(indexer.config)
	url := params.makeODBCConnectionString()
	log.Infof("Connect to mongo host: %s\n", url)
	indexer.helper = utils.NewSQLConnectHelper("mssql", "odbc", url)
	indexer.helper.Connect()
	indexer.updateTradeDates()
	go func() {
		time.Sleep(24 * time.Hour)
		indexer.updateTradeDates()
	}()
}

func (indexer *WindMssqlIndexer) updateTradeDates() {
	indexer.helper.Connect()
	con := indexer.helper.DB
	sqlStatement := fmt.Sprintf("SELECT DISTINCT TRADE_DAYS FROM \"%s\".dbo.ASHARECALENDAR", indexer.config.Db)
	rows, err := con.Query(sqlStatement)
	if err != nil {
		log.Error(err)
		return
	}
	defer rows.Close()
	index := make([]int, 0)
	for rows.Next() {
		var i int
		err := rows.Scan(&i)
		if err != nil {
			log.Error(err)
			return
		}
		index = append(index, i)
	}
	sort.Ints(index)
	indexer.datetimeIndex = index
	n := len(index)
	if n > 0 {
		log.Infof("Update trade dates from %d to %d", index[0], index[n-1])
	}
}

// GetIndex fetch the trading date index during the given dateRange.
func (indexer *WindMssqlIndexer) GetIndex(dateRange models.DateRange) []int {
	start := dateRange.Start
	end := dateRange.End
	if end <= 0 {
		yesterday := time.Now().Add(-24 * time.Hour)
		end, _ = strconv.Atoi(yesterday.Format("20060102"))
	}
	startIndex := sort.SearchInts(indexer.datetimeIndex, start)
	endIndex := sort.Search(len(indexer.datetimeIndex), func(i int) bool { return indexer.datetimeIndex[i] > end })
	index := indexer.datetimeIndex[startIndex:endIndex]
	return index
}

// Close the MongoIndexer and close all mongo connections.
func (indexer *WindMssqlIndexer) Close() {
	indexer.helper.Close()
}
