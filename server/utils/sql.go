package utils

import (
	"database/sql"
	"sync"
	"time"
)

var log = AppLogger

// SQLConnectHelper help manage connection to a sql.
type SQLConnectHelper struct {
	name      string
	driver    string
	url       string
	lock      sync.Mutex
	DB        *sql.DB
	Connected bool
	running   bool
}

// Connect get the connection of a sql, autoretry.
func (helper *SQLConnectHelper) Connect() {
	helper.lock.Lock()
	defer helper.lock.Unlock()
	if helper.DB != nil {
		if helper.DB.Ping() == nil {
			return
		}
		log.Infof("Connection lost of %s host: %s", helper.name, helper.url)
		helper.DB.Close()
	}
	helper.Connected = false
	for retry := 2; helper.running; {
		db, err := sql.Open(helper.driver, helper.url)
		if err != nil {
			log.Infof("Connect failed to %s host: %s", helper.name, helper.url)
			log.Error(err)
		}
		err = db.Ping()
		if err != nil {
			log.Infof("Connect failed to %s host: %s", helper.name, helper.url)
			log.Error(err)
			db.Close()
		} else {
			log.Infof("Connect success to %s host: %s", helper.name, helper.url)
			helper.DB = db
			helper.Connected = true
			break
		}
		log.Infof("Retry connect to %s host: %s after %d seconds...", helper.name, helper.url, retry)
		time.Sleep(time.Duration(retry) * time.Second)
		retry *= 2
	}
}

func (helper *SQLConnectHelper) Close() {
	helper.running = false
	if helper.DB != nil {
		helper.DB.Close()
	}
}

// NewSQLConnectHelper create a new SQLConnectHelper
func NewSQLConnectHelper(name, driver, url string) (helper *SQLConnectHelper) {
	helper = &SQLConnectHelper{name: name, driver: driver, url: url}
	helper.lock = sync.Mutex{}
	helper.running = true
	return
}
