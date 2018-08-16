package stores

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"strings"
	"sync"
	"time"

	"fxdayu.com/dyupdater/server/celery"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	_ "github.com/mattn/go-oci8" // import go-oci8 driver
	"github.com/spf13/viper"
)

type oracleStoreConfig struct {
	Celery            string `mapstructure:"celery"`
	URL               string `mapstructure:"url"`
	Db                string `mapstructure:"db"`
	Transaction       bool   `mapstructure:"transaction"`
	TaskUpdate        string `mapstructure:"task.update.name"`
	TaskUpdateTimeout int    `mapstructure:"task.update.timeout"`
}

type oracleFactorValue struct {
	TDATE      int
	SYMBOLCODE string
	RAWVALUE   float64
}

// OracleStore 是依托oracle数据库的因子存储。
//
// 因子数据将存放在与因子ID同名的表中。
//
// 因子数据表有以下格式：
//   | TDATE | SYMBOLCODE | RAWVALUE | PROCESSEDVALUE | PROCESSEDTYPE |
// 建表语句如下：
/*
	CREATE TABLE "{{.username}}"."{{.tablename}}" (
		"TDATE" NUMBER(14) NOT NULL ,
		"SYMBOLCODE" VARCHAR2(12) NOT NUll,
		"RAWVALUE" Number ,
		"PROCESSEDVALUE" Number ,
		"PROCESSEDTYPE" VARCHAR2(1)
	)`)
*/
// OracleStore的配置选项有：
//  url: 数据库url, url格式为：{username}/{password}@{host}:{port}/{sid}?{params},如：hr/hr@localhost:1521/xe?as=sysdba
//  transaction: 是否每次写入都以事务的方式进行，建议开启，默认开启。
type OracleStore struct {
	BaseFactorStore
	common.BaseComponent
	helper           *utils.SQLConnectHelper
	config           *oracleStoreConfig
	taskChan         map[string]chan celeryResult
	taskChanLock     sync.RWMutex
	queueUpdate      *celery.CeleryQueue
	taskUpdatePrefix string
	taskUpdateCount  int
}

// Init the OracleStore, will create connection pool.
func (s *OracleStore) Init(config *viper.Viper) {
	s.BaseComponent.Init(config)
	s.config = &oracleStoreConfig{
		Celery:            "default",
		URL:               utils.GetEnv("ORACLE_URL", "sys/.@?as=sysdba"),
		Transaction:       true,
		TaskUpdate:        "stores.oracle_update",
		TaskUpdateTimeout: 600,
	}
	config.Unmarshal(s.config)
	if s.config.Db == "" {
		s.config.Db = strings.Split(s.config.URL, "/")[0]
	}
	s.helper = utils.NewSQLConnectHelper("oracle", "oci8", s.config.URL)
	service := celery.GetCeleryService()
	celeryHelper := service.MustGet(s.config.Celery)
	s.taskUpdatePrefix = "stores-oracle-update"
	s.queueUpdate = celeryHelper.GetQueue(s.config.TaskUpdate, s.taskUpdatePrefix)
	s.taskChan = make(map[string]chan celeryResult)
	s.taskChanLock = sync.RWMutex{}
	log.Infof("Connect to oracle host: %s", s.config.URL)
	s.helper.Connect()
	go s.handleUpdateResult()
}

func (s *OracleStore) creatTable(name string) error {
	tmpl, err := template.New("createTable").Parse(`
	CREATE TABLE "{{.tablename}}" (
		tdate varchar(20),
		symbolcode varchar(20),
		rawvalue numeric(30,10),
		processedvalue numeric(30,10),
		processedtype varchar(20)
	)`)
	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, map[string]string{"username": s.config.Db, "tablename": s.trimIdentity(name)})
	if err != nil {
		log.Error(err.Error())
		return err
	}
	str := buf.String()
	stmt, err := s.helper.DB.Prepare(str)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		errMsg := err.Error()
		if !strings.HasPrefix(errMsg, "ORA-00955") { // ORA-00955: name is already used by an existing object
			log.Error(err.Error())
			return err
		}
	}
	return nil
}

func (s *OracleStore) trimIdentity(id string) string {
	if len(id) >= 30 {
		return id[:30]
	}
	return id
}

// Check factor data for given factor and daterange in OracleStore.
func (s *OracleStore) Check(factor models.Factor, index []int) ([]int, error) {
	s.helper.Connect()
	indexSet := mapset.NewSet()
	for _, v := range index {
		indexSet.Add(interface{}(v))
	}
	sqlStatement := fmt.Sprintf("SELECT DISTINCT TDATE FROM \"%s\"", s.trimIdentity(factor.ID))
	rows, err := s.helper.DB.Query(sqlStatement)
	empty := false
	if err != nil {
		errMsg := err.Error()
		log.Error(errMsg)
		if strings.HasPrefix(errMsg, "ORA-00942") { // ORA-00942: table or view does not exist
			empty = true
		} else {
			return nil, err
		}
	}
	existSet := mapset.NewSet()
	if !empty {
		for rows.Next() {
			dt := new(int)
			err := rows.Scan(dt)
			if err != nil {
				return nil, err
			}
			existSet.Add(interface{}(*dt))
		}
	}
	lostSet := indexSet.Difference(existSet)
	lost := make([]int, 0, lostSet.Cardinality())
	for v := range lostSet.Iter() {
		lost = append(lost, v.(int))
	}
	return lost, nil
}

func (s *OracleStore) setTaskChan(id string, ch chan celeryResult) {
	s.taskChanLock.Lock()
	defer s.taskChanLock.Unlock()
	s.taskChan[id] = ch
}

func (s *OracleStore) getTaskChan(id string) chan celeryResult {
	s.taskChanLock.RLock()
	defer s.taskChanLock.RUnlock()
	ch, _ := s.taskChan[id]
	return ch
}

func (s *OracleStore) removeTaskChan(id string) {
	s.taskChanLock.Lock()
	defer s.taskChanLock.Unlock()
	ch, ok := s.taskChan[id]
	if ok {
		close(ch)
		delete(s.taskChan, id)
	}
}

func (s *OracleStore) handleUpdateResult() {
	for result := range s.queueUpdate.Output {
		rep := result.Response
		var ret celeryResult
		if rep.Error != "" {
			ret = celeryResult{
				Data:  0,
				Error: errors.New(rep.Error),
			}
		} else {
			var data int
			err := json.Unmarshal([]byte(rep.Result), &data)
			if err != nil {
				log.Error(err.Error())
				ret = celeryResult{
					Data:  0,
					Error: errors.New(rep.Error),
				}
			}
			ret = celeryResult{
				Data:  data,
				Error: nil,
			}
		}
		ch := s.getTaskChan(result.ID)
		if ch != nil {
			ch <- ret
		} else {
			log.Warning("Unrelated celery task %s of %s", result.ID, s.config.TaskUpdate)
		}
	}
}

func (s *OracleStore) Update(factor models.Factor, factorValue models.FactorValue, replace bool) (int, error) {
	factorValueString, err := utils.PackFactorValue(factorValue)
	if err != nil {
		return 0, err
	}
	data := map[string][]interface{}{
		"args": []interface{}{s.trimIdentity(factor.ID), factorValueString},
	}
	jsonData, err := json.Marshal(data)
	s.taskUpdateCount++
	taskID := fmt.Sprintf("%s-%d-%d", s.taskUpdatePrefix, time.Now().Unix(), s.taskUpdateCount)
	_, err = s.queueUpdate.Publish(taskID, jsonData)
	if err != nil {
		return 0, err
	}
	ch := make(chan celeryResult)
	s.setTaskChan(taskID, ch)
	defer s.removeTaskChan(taskID)
	select {
	case r := <-ch:
		if r.Error != nil {
			return 0, r.Error
		}
		result, ok := r.Data.(int)
		if !ok {
			return 0, fmt.Errorf("Invalid update Result: %v", r.Data)
		}
		return result, r.Error
	case <-time.After(time.Duration(s.config.TaskUpdateTimeout) * time.Second):
		return 0, fmt.Errorf("oracle update task timeout after %d seconds", s.config.TaskUpdateTimeout)
	}
}

// func (s *OracleStore) execTransaction(factor models.Factor, start int, end int, rows []oracleFactorValue, replace bool) (int, error) {
// 	timeStart := time.Now()
// 	var tx *sql.Tx
// 	var err error
// 	if s.config.Transaction {
// 		tx, err = s.helper.DB.Begin()
// 		if err != nil {
// 			return 0, err
// 		}
// 		defer tx.Rollback()
// 	}
// 	factorID := s.trimIdentity(factor.ID)
// 	stmtQuery := fmt.Sprintf("SELECT DISTINCT TDATE FROM \"%s\".\"%s\" WHERE TDATE >= %d AND TDATE <= %d", s.config.Db, factorID, start, end)
// 	stmtDelete := fmt.Sprintf("DELETE FROM \"%s\".\"%s\" WHERE TDATE >= %d AND TDATE <= %d", s.config.Db, factorID, start, end)
// 	var queryFunc func(query string, args ...interface{}) (*sql.Rows, error)
// 	var execFunc func(query string, args ...interface{}) (sql.Result, error)
// 	var prepareFunc func(query string) (*sql.Stmt, error)
// 	if s.config.Transaction {
// 		queryFunc = tx.Query
// 		execFunc = tx.Exec
// 		prepareFunc = tx.Prepare
// 	} else {
// 		queryFunc = s.helper.DB.Query
// 		execFunc = s.helper.DB.Exec
// 		prepareFunc = s.helper.DB.Prepare
// 	}
// 	// query
// 	var dateSet mapset.Set
// 	if !replace {
// 		rows, err := queryFunc(stmtQuery)
// 		if err != nil {
// 			return 0, err
// 		}
// 		defer rows.Close()
// 		dateSet = mapset.NewSet()
// 		for rows.Next() {
// 			var v int
// 			rows.Scan(&v)
// 			dateSet.Add(v)
// 		}
// 		rows.Close()
// 	}
// 	// delete
// 	_, err = execFunc(stmtDelete)
// 	log.Debugf("Delete cost %.3f seconds", time.Since(timeStart).Seconds())
// 	if err != nil {
// 		return 0, err
// 	}
// 	// get new rows
// 	newRows := make([]oracleFactorValue, 0, len(rows))
// 	for _, row := range rows {
// 		if !replace && dateSet.Contains(row.TDATE) {
// 			continue
// 		}
// 		newRows = append(newRows, row)
// 	}
// 	timeInsert := time.Now()
// 	// prepare
// 	var stmt *sql.Stmt
// 	stmt, err = prepareFunc(fmt.Sprintf("INSERT INTO \"%s\".\"%s\" (TDATE, SYMBOLCODE, RAWVALUE) VALUES (:tDate, :symbolCode , :rawValue)", s.config.Db, factor.ID))
// 	if err != nil {
// 		return 0, err
// 	}
// 	// insert
// 	for _, row := range newRows {
// 		_, err = stmt.Exec(
// 			sql.Named("tDate", row.TDATE),
// 			sql.Named("symbolCode", row.SYMBOLCODE),
// 			sql.Named("rawValue", row.RAWVALUE),
// 		)
// 		if err != nil {
// 			return 0, err
// 		}
// 	}
// 	err = stmt.Close()
// 	if err != nil {
// 		return 0, err
// 	}
// 	// commit
// 	log.Info("Begin Commit")
// 	if s.config.Transaction {
// 		err = tx.Commit()
// 	}
// 	if err != nil {
// 		return 0, err
// 	}
// 	count := len(newRows)
// 	log.Debugf("Updated %d rows in %s.%s from %d to %d, cost: %.3f seconds", count, s.config.Db, factor.ID, start, end, time.Since(timeInsert).Seconds())
// 	return count, err
// }

// // Update factor data of given factor and factor values in OracleStore.
// func (s *OracleStore) Update(factor models.Factor, factorValue models.FactorValue, replace bool) (int, error) {
// 	if len(factorValue.Datetime) == 0 {
// 		return 0, nil
// 	}
// 	s.helper.Connect()
// 	err := s.creatTable(factor.ID)
// 	if err != nil {
// 		return 0, err
// 	}

// 	maxcap := 100000
// 	count := 0
// 	var rows []oracleFactorValue
// 	var start, end int
// 	var rowTotal int
// 	for i, dt := range factorValue.Datetime {
// 		if count == 0 {
// 			rows = make([]oracleFactorValue, 0, maxcap)
// 			start = dt
// 		}
// 		for symbol, values := range factorValue.Values {
// 			if !(math.IsNaN(values[i]) || math.IsInf(values[i], 0)) {
// 				count++
// 				row := oracleFactorValue{dt, symbol, values[i]}
// 				rows = append(rows, row)
// 			}
// 		}
// 		end = dt
// 		if count >= maxcap {
// 			rowCount, err := s.execTransaction(factor, start, end, rows, replace)
// 			if err != nil {
// 				return 0, err
// 			}
// 			count = 0
// 			rowTotal += rowCount
// 		}
// 	}
// 	if count > 0 {
// 		rowCount, err := s.execTransaction(factor, start, end, rows, replace)
// 		if err != nil {
// 			return 0, err
// 		}
// 		rowTotal += rowCount
// 	}
// 	return rowTotal, err
// }

// Close the MongoStore, will close all connections.
func (s *OracleStore) Close() {
	s.helper.Close()
}
