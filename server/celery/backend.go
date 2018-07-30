package celery

import (
	"bytes"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gocelery/gocelery"
)

type CustomRedisCeleryBackend struct {
	*gocelery.RedisCeleryBackend
	subscribers []chan interface{}
	db          string
}

func NewRedisPoolWithOptions(host, pass string, options ...redis.DialOption) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, options...)
			if err != nil {
				return nil, err
			}
			if pass != "" {
				if _, err = c.Do("AUTH", pass); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func NewRedisCeleryBackendWithOptions(host, pass string, options ...redis.DialOption) *gocelery.RedisCeleryBackend {
	return &gocelery.RedisCeleryBackend{
		Pool: NewRedisPoolWithOptions(host, pass, options...),
	}
}

func NewCustomRedisCeleryBackend(s string) *CustomRedisCeleryBackend {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	path := strings.TrimPrefix(u.Path, "/")
	if path == "" {
		path = "0"
	}
	dbInt, err := strconv.Atoi(path)
	if err != nil {
		log.Error(err.Error())
	}
	db := redis.DialDatabase(dbInt)
	host = fmt.Sprintf("%s:%s", host, port)
	var pass = ""
	if u.User != nil {
		pass, _ = u.User.Password()
	}
	log.Infof("Connect to redis celery backend: %s/%s", host, path)
	return &CustomRedisCeleryBackend{
		RedisCeleryBackend: NewRedisCeleryBackendWithOptions(host, pass, db),
		subscribers:        make([]chan interface{}, 0),
		db:                 path,
	}
}

func (crb *CustomRedisCeleryBackend) GetTaskIDs() ([]string, error) {
	conn := crb.Get()
	defer conn.Close()
	values, err := redis.Values(conn.Do("KEYS", "celery-task-meta-*"))
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for _, v := range values {
		ids = append(ids, strings.TrimPrefix(string(v.([]byte)), "celery-task-meta-"))
	}
	if err != nil {
		return nil, err
	}
	return ids, nil
}

var abortMessage = "{\"status\": \"ABORTED\", \"result\": null, \"traceback\": null, \"children\": null}"

func (crb *CustomRedisCeleryBackend) AbortTask(taskID string) error {
	conn := crb.Get()
	defer conn.Close()
	_, err := conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, []byte(abortMessage))
	return err
}

func (crb *CustomRedisCeleryBackend) GetResult(taskID string) (*gocelery.ResultMessage, error) {
	res, err := crb.RedisCeleryBackend.GetResult(taskID)
	if res != nil {
		res.ID = taskID
	}
	return res, err
}

func (crb *CustomRedisCeleryBackend) DeleteResult(taskID string) error {
	conn := crb.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", fmt.Sprintf("celery-task-meta-%s", taskID))
	return err
}

func (crb *CustomRedisCeleryBackend) sendToSubscribers(v interface{}) {
	for _, ch := range crb.subscribers {
		select {
		case ch <- v:
			continue
		default:
			continue
		}
	}
}

func (crb *CustomRedisCeleryBackend) handleSubscribe(psc *redis.PubSubConn) error {
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			key := bytes.NewBuffer(v.Data).String()
			if strings.HasPrefix(key, "celery-task-meta-") {
				taskID := strings.TrimPrefix(key, "celery-task-meta-")
				crb.sendToSubscribers(taskID)
			}
		case redis.Subscription:
			log.Debugf("Subscribe success, channel: %s, kind: %s, count: %d.", v.Channel, v.Kind, v.Count)
			crb.sendToSubscribers(v)
		case error:
			log.Error(v.Error())
			return v
		}
	}
}

func (crb *CustomRedisCeleryBackend) checkConfig() error {
	conn := crb.Get()
	res, err := conn.Do("CONFIG", "GET", "notify-keyspace-events")
	if err != nil {
		return err
	}
	val, ok := res.([]interface{})
	if !ok {
		return fmt.Errorf("unknown redis config info of notify-keyspace-events")
	}
	redisConfig := map[string]string{}
	for i := 0; i < len(val); i++ {
		key, ok := val[i].([]byte)
		if !ok {
			return fmt.Errorf("unknown redis config info of notify-keyspace-events")
		}
		i++
		value, ok := val[i].([]byte)
		if !ok {
			return fmt.Errorf("unknown redis config info of notify-keyspace-events")
		}
		redisConfig[bytes.NewBuffer(key).String()] = bytes.NewBuffer(value).String()
	}
	notifyConf, _ := redisConfig["notify-keyspace-events"]
	if !(strings.Contains(notifyConf, "E") && strings.Contains(notifyConf, "A") || strings.Contains(notifyConf, "E") && strings.Contains(notifyConf, "$")) {
		return fmt.Errorf("redis server notify-keyspace-events improperly configured: %q", notifyConf)
	}
	return nil
}

func (crb *CustomRedisCeleryBackend) startSubscribe() error {
	channelName := fmt.Sprintf("__keyevent@%s__:set", crb.db)
	log.Debugf("subscribing channel: %s", channelName)
	if err := crb.checkConfig(); err != nil {
		return err
	}
	go func() {
		failture := 0
		for {
			if failture > 0 {
				wait := int(math.Pow(2, float64(failture)))
				if wait > 64 {
					wait = 64
				}
				log.Infof("Retry subscribe redis backend after %d seconds", wait)
				time.Sleep(time.Duration(wait) * time.Second)
			}
			conn := crb.Get()
			psc := &redis.PubSubConn{Conn: conn}
			err := psc.Subscribe(channelName)
			if err != nil {
				log.Error(err.Error())
			} else {
				failture = 0
				crb.handleSubscribe(psc)
			}
			failture++
		}
	}()
	return nil
}

func (crb *CustomRedisCeleryBackend) Subscribe(ch chan interface{}) error {
	if len(crb.subscribers) == 0 {
		if err := crb.startSubscribe(); err != nil {
			return err
		}
	}
	crb.subscribers = append(crb.subscribers, ch)
	return nil
}
