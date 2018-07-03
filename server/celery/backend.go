package celery

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/gocelery/gocelery"
)

type CustomRedisCeleryBackend struct {
	*gocelery.RedisCeleryBackend
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
	host = fmt.Sprintf("%s:%s", host, port)
	var pass = ""
	if u.User != nil {
		pass, _ = u.User.Password()
	}
	return &CustomRedisCeleryBackend{
		RedisCeleryBackend: gocelery.NewRedisCeleryBackend(host, pass),
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

func (crb *CustomRedisCeleryBackend) DeleteResult(task string) error {
	conn := crb.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", fmt.Sprintf("celery-task-meta-%s", task))
	return err
}
