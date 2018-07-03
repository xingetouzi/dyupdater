package stores

import (
	"math"
	"sort"
	"strings"
	"time"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spf13/viper"
)

type mongoStoreConfig struct {
	URL string `mapstructure:"url"`
	Db  string `mapstructure:"db"`
}

// MongoStore 是依托mongodb的因子存储。
// MongoStore的配置选项有：
//  url: 数据库url, 默认: mongodb://localhost:27017 ,可以参考 https://docs.mongodb.com/manual/reference/connection-string/
//  db: 数据库名，默认: fxdayu_factors
type MongoStore struct {
	common.BaseComponent
	config  *mongoStoreConfig
	session *mgo.Session
}

type factorDatetime struct {
	Datetime time.Time `bson:"datetime"`
}

// Init the MongoStore, will create mongo connection pool.
func (store *MongoStore) Init(config *viper.Viper) {
	store.BaseComponent.Init(config)
	store.config = &mongoStoreConfig{
		URL: utils.GetEnv("MONGO_URL", "mongodb://localhost:27017"),
		Db:  utils.GetEnv("MONGO_DATABASE", "fxdayu_factors"),
	}
	config.Unmarshal(store.config)
	log.Infof("Connect to mongo host: %s/%s\n", store.config.URL, store.config.Db)
	var err error
	store.session, err = mgo.Dial(store.config.URL)
	if err != nil {
		panic(err)
	}
}

func (store *MongoStore) getFactorDateSet(factor models.Factor) mapset.Set {
	conn := store.session.Clone()
	defer conn.Close()
	col := conn.DB(store.config.Db).C(factor.ID)
	var factorDatetimes []factorDatetime
	col.Find(bson.M{}).All(&factorDatetimes)
	dateSet := mapset.NewSet()
	for _, value := range factorDatetimes {
		date, _ := utils.Datetoi(value.Datetime)
		dateSet.Add(date)
	}
	return dateSet
}

// Update factor data of given factor and factor values in MongoStore.
func (store *MongoStore) Update(factor models.Factor, factorValue models.FactorValue, replace bool) (int, error) {
	var err error
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
			log.Error(err)
		}
	}()
	conn := store.session.Clone()
	defer conn.Close()
	col := conn.DB(store.config.Db).C(factor.ID)
	col.EnsureIndexKey("-datetime")
	var dateSet mapset.Set
	length := len(factorValue.Datetime)
	if !replace {
		dateSet = store.getFactorDateSet(factor)
	}
	count := 0
	for i, dateInt := range factorValue.Datetime {
		if !replace && dateSet.Contains(dateInt) {
			continue
		}
		dct := make(map[string]interface{}, length)
		date, _ := utils.ItoDate(dateInt)
		dct["datetime"] = date
		for key := range factorValue.Values {
			value := factorValue.Values[key][i]
			if math.IsNaN(value) || math.IsInf(value, 0) {
				continue
			}
			mgoKey := strings.Split(key, ".")[0]
			if key != "trade_date" {
				dct[mgoKey] = value
			}
		}
		col.Upsert(bson.M{"datetime": dct["datetime"]}, bson.M{"$set": dct})
		count++
	}
	return count, err
}

// Check factor data for given factor and daterange in MongoStore.
func (store *MongoStore) Check(factor models.Factor, index []int) ([]int, error) {
	indexSet := mapset.NewSet()
	for _, v := range index {
		indexSet.Add(interface{}(v))
	}
	var dateToUpdate []int
	for _, v := range indexSet.Difference(store.getFactorDateSet(factor)).ToSlice() {
		dateToUpdate = append(dateToUpdate, v.(int))
	}
	sort.Ints(dateToUpdate)
	return dateToUpdate, nil
}

// Close the MongoStore, will close all mongodb connections.
func (store *MongoStore) Close() {
	store.session.Clone()
}
