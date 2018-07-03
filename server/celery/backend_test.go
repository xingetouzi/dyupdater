package celery

import (
	"testing"

	"fxdayu.com/dyupdater/server/utils"
)

func TestCeleryBackend(t *testing.T) {
	s := utils.GetEnv("REDIS_URL", "redis://127.0.0.1/0")
	backend := NewCustomRedisCeleryBackend(s)
	ids, err := backend.GetTaskIDs()
	if err != nil {
		t.Error(err.Error())
	} else {
		t.Log(ids)
	}
}
