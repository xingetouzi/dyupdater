package main

import (
	"fmt"
	"log"

	"fxdayu.com/dyupdater/server/task"

	"fxdayu.com/dyupdater/server/stores"

	"github.com/spf13/viper"

	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
)

func main() {
	utils.ConfigLog(nil, true)
	config := viper.New()
	store := new(stores.OracleStore)
	store.Init(config)
	factor := models.Factor{ID: "test", Formula: ""}
	lost, err := store.Check(factor, task.ProcessTypeNone, []int{20160101})
	if err != nil {
		log.Panic(err)
	} else {
		log.Println(lost)
	}
	symbols := []string{"000001.SZ", "600001.SH"}
	factorValues := models.FactorValue{}
	factorValues.Datetime = lost
	factorValues.Values = make(map[string][]float64, 2)
	for _, s := range symbols {
		factorValues.Values[s] = make([]float64, len(lost))
	}
	count, err := store.Update(factor, task.ProcessTypeNone, factorValues, false)
	fmt.Println(count)
}
