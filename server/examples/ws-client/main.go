package main

import (
	"net/url"

	"github.com/op/go-logging"

	"fxdayu.com/dyupdater/server/calculators"
	"fxdayu.com/dyupdater/server/utils"
)

var log = logging.MustGetLogger("")

func main() {
	host := utils.GetEnv("FLOWER_HOST", "localhost:5555")
	url := url.URL{Scheme: "ws", Host: host, Path: "/api/task/events/task-succeeded/"}
	read := make(chan *calculators.FlowerEvent, 20)
	err := calculators.OpenWs(read, url.String())
	if err != nil {
		log.Error(err)
	}
	for m := range read {
		log.Info(*m)
	}
}
