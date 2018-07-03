package stores

import (
	"bytes"
	"database/sql"
	"fmt"
	"html/template"
	"math"
	"strings"
	"time"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	_ "github.com/mattn/go-oci8" // import go-oci8 driver
	"github.com/spf13/viper"
)

type oracleStoreConfig struct {
	URL         string `mapstructure:"url"`
	Db          string `mapstructure:"db"`
	Transaction bool   `mapstructure:"transaction"`
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
	common.BaseComponent
	helper *utils.SQLConnectHelper
	config *oracleStoreConfig
}

// Init the OracleStore, will create connection pool.
func (s *OracleStore) Init(config *viper.Viper) {
	s.BaseComponent.Init(config)
	s.config = &oracleStoreConfig{
		URL:         utils.GetEnv("ORACLE_URL", "sys/.@?as=sysdba"),
		Transaction: true,
	}
	config.Unmarshal(s.config)
	if s.config.Db == "" {
		s.config.Db = strings.Split(s.config.URL, "/")[0]
	}
	s.helper = utils.NewSQLConnectHelper("oracle", "oci8", s.config.URL)
	log.Infof("Connect to oracle host: %s", s.config.URL)
	s.helper.Connect()
}

func (s *OracleStore) creatTable(name string) error {
	tmpl, err := template.New("createTable").Parse(`
	CREATE TABLE "{{.username}}"."{{.tablename}}" (
		"TDATE" NUMBER(14) NOT NULL ,
		"SYMBOLCODE" VARCHAR2(12) NOT NUll,
		"RAWVALUE" Number ,
		"PROCESSEDVALUE" Number ,
		"PROCESSEDTYPE" VARCHAR2(1)
	)`)
	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, map[string]string{"username": s.config.Db, "tablename": name})
	if err != nil {
		log.Error(err)
		return err
	}
	str := buf.String()
	stmt, err := s.helper.DB.Prepare(str)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		errMsg := err.Error()
		if !strings.HasPrefix(errMsg, "ORA-00955") { // ORA-00955: name is already used by an existing object
			log.Error(err)
			return err
		}
	}
	return nil
}

// Check factor data for given factor and daterange in OracleStore.
func (s *OracleStore) Check(factor models.Factor, index []int) ([]int, error) {
	s.helper.Connect()
	indexSet := mapset.NewSet()
	for _, v := range index {
		indexSet.Add(interface{}(v))
	}
	sqlStatement := fmt.Sprintf("SELECT DISTINCT TDATE FROM %s.%s", s.config.Db, factor.ID)
	rows, err := s.helper.DB.Query(sqlStatement)
	empty := false
	if err != nil {
		errMsg := err.Error()
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
	lost := make([]int, lostSet.Cardinality())
	i := 0
	for v := range lostSet.Iter() {
		lost[i] = v.(int)
		i++
	}
	return lost, nil
}

func (s *OracleStore) execTransaction(factor models.Factor, start int, end int, rows [][]interface{}, replace bool) (int, error) {
	timeStart := time.Now()
	var tx *sql.Tx
	var err error
	if s.config.Transaction {
		tx, err = s.helper.DB.Begin()
		if err != nil {
			return 0, err
		}
		defer tx.Rollback()
	}
	stmtQuery := fmt.Sprintf("SELECT DISTINCT TDATE FROM \"%s\".\"%s\" WHERE TDATE >= %d AND TDATE <= %d", s.config.Db, factor.ID, start, end)
	stmtDelete := fmt.Sprintf("DELETE FROM \"%s\".\"%s\" WHERE TDATE >= %d AND TDATE <= %d", s.config.Db, factor.ID, start, end)
	var queryFunc func(query string, args ...interface{}) (*sql.Rows, error)
	var execFunc func(query string, args ...interface{}) (sql.Result, error)
	var prepareFunc func(query string) (*sql.Stmt, error)
	if s.config.Transaction {
		queryFunc = tx.Query
		execFunc = tx.Exec
		prepareFunc = tx.Prepare
	} else {
		queryFunc = s.helper.DB.Query
		execFunc = s.helper.DB.Exec
		prepareFunc = s.helper.DB.Prepare
	}
	var dateSet mapset.Set
	if !replace {
		rows, err := queryFunc(stmtQuery)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		dateSet = mapset.NewSet()
		for rows.Next() {
			var v int
			rows.Scan(&v)
			dateSet.Add(v)
		}
		rows.Close()
	}
	_, err = execFunc(stmtDelete)
	log.Debugf("Delete cost %.3f seconds", time.Since(timeStart).Seconds())
	if err != nil {
		return 0, err
	}
	var stmt *sql.Stmt
	stmt, err = prepareFunc(fmt.Sprintf("INSERT INTO \"%s\".\"%s\" (TDATE, SYMBOLCODE, RAWVALUE) VALUES (:tDate, :symbolCode , :rawValue)", s.config.Db, factor.ID))
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	for _, row := range rows {
		if !replace && dateSet.Contains(rows[0]) {
			continue
		}
		_, err = stmt.Exec(
			sql.Named("tDate", row[0]),
			sql.Named("symbolCode", row[1]),
			sql.Named("rawValue", row[2]),
		)
	}
	if err != nil {
		return 0, err
	}
	if s.config.Transaction {
		err = tx.Commit()
	}
	log.Debugf("Updated %d rows in %s.%s from %d to %d, cost: %.3f seconds", len(rows), s.config.Db, factor.ID, start, end, time.Since(timeStart).Seconds())
	return len(rows), err
}

// Update factor data of given factor and factor values in OracleStore.
func (s *OracleStore) Update(factor models.Factor, factorValue models.FactorValue, replace bool) (int, error) {
	if len(factorValue.Datetime) == 0 {
		return 0, nil
	}
	s.helper.Connect()
	err := s.creatTable(factor.ID)
	if err != nil {
		return 0, err
	}

	n := len(factorValue.Values)
	maxcap := 100000/n*n + n
	count := 0
	var rows [][]interface{}
	var start, end int
	var rowTotal int
	for i, dt := range factorValue.Datetime {
		if count == 0 {
			rows = make([][]interface{}, 0, maxcap)
			start = dt
		}
		for symbol, values := range factorValue.Values {
			if !(math.IsNaN(values[i]) || math.IsInf(values[i], 0)) {
				count++
				row := []interface{}{dt, symbol, values[i]}
				rows = append(rows, row)
			}
		}
		end = dt
		if count >= maxcap {
			rowCount, err := s.execTransaction(factor, start, end, rows, replace)
			if err != nil {
				return 0, err
			}
			count = 0
			rowTotal += rowCount
		}
	}
	if count > 0 {
		rowCount, err := s.execTransaction(factor, start, end, rows, replace)
		if err != nil {
			return 0, err
		}
		rowTotal += rowCount
	}
	return rowTotal, err
}

// Close the MongoStore, will close all connections.
func (s *OracleStore) Close() {
	s.helper.Close()
}
