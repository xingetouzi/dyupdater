package indexers

import (
	"sort"
	"strconv"
	"time"

	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spf13/viper"
)

type tradeDateRecord struct {
	TradeDate int `bson:"trade_date"`
}

type mongoIndexerConfig struct {
	URL string `mapstructure:"url"`
}

// MongoIndexer is the TradingDatetimeIndexers which get the trading date index from mongodb.
// MongoIndexer的配置项如下：
//   url: 所连接mongodb的url地址，例如mongodb://localhost:27017
type MongoIndexer struct {
	config        *mongoIndexerConfig
	session       *mgo.Session
	datetimeIndex []int
}

// Init the MongoIndexer, will create mongodb connect pool.
func (indexer *MongoIndexer) Init(config *viper.Viper) {
	indexer.config = &mongoIndexerConfig{URL: utils.GetEnv("MONGO_URL", "mongodb://localhost:27017")}
	config.Unmarshal(indexer.config)
	log.Infof("Connect to mongo host: %s\n", indexer.config.URL)
	var err error
	indexer.session, err = mgo.Dial(indexer.config.URL)
	if err != nil {
		panic(err)
	}
	indexer.updateTradeDates()
	go func() {
		time.Sleep(24 * time.Hour)
		indexer.updateTradeDates()
	}()
}

func (indexer *MongoIndexer) updateTradeDates() {
	conn := indexer.session.Clone()
	defer conn.Close()
	datetimeSet := mapset.NewSet()
	var tradeDateRecords []tradeDateRecord
	col := conn.DB("jz").C("secTradeCal")
	col.Find(bson.M{"istradeday": "T"}).Sort("trade_date").All(&tradeDateRecords)
	for _, value := range tradeDateRecords {
		datetimeSet.Add(value.TradeDate)
	}
	index := make([]int, datetimeSet.Cardinality())
	for i, v := range datetimeSet.ToSlice() {
		index[i] = v.(int)
	}
	sort.Ints(index)
	indexer.datetimeIndex = index
	n := len(index)
	if n > 0 {
		log.Infof("Update trade dates from %d to %d", index[0], index[n-1])
	}
}

// GetIndex fetch the trading date index during the given dateRange.
func (indexer *MongoIndexer) GetIndex(dateRange models.DateRange) []int {
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
func (indexer *MongoIndexer) Close() {
	if indexer.session != nil {
		indexer.session.Close()
	}
}
