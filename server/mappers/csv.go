package mappers

import (
	"errors"
	"fmt"
	"os"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/gocarina/gocsv"
	"github.com/spf13/viper"
	fsnotify "gopkg.in/fsnotify.v1"
)

type csvMappingRecord struct {
	Source string `csv:"source"`
	Store  string `csv:"store"`
}

type csvMappingRecords []*csvMappingRecord

type csvMapperConfig struct {
	File  string `mapstructure:"file"`
	Watch bool   `mapstructure:"watch"`
}

type CSVMapper struct {
	common.BaseComponent
	config    csvMapperConfig
	factorMap map[string]string
}

func (mapper *CSVMapper) Init(config *viper.Viper) {
	mapper.BaseComponent.Init(config)
	mapper.config = csvMapperConfig{Watch: true}
	config.Unmarshal(&mapper.config)
	if mapper.config.File == "" {
		panic(errors.New("CSVMapper's mapping file should be specify"))
	} else if !utils.IsExist(mapper.config.File) {
		panic(errors.New("CSVMapper's mapping file not exists"))
	}
	mapper.factorMap = make(map[string]string)
	mapper.load()
	if mapper.config.Watch {
		go mapper.watch()
	}
}

func (mapper *CSVMapper) watch() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()
	err = watcher.Add(mapper.config.File)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Infof("CSVMapper's mapping file changed, reload it.")
					mapper.load()
				}
			case err := <-watcher.Errors:
				if err != nil {
					log.Error(err.Error())
				}
			}
		}
	}()
}

func (mapper *CSVMapper) read() (map[string]string, error) {
	var mapping map[string]string
	records := csvMappingRecords{}
	file, err := os.OpenFile(mapper.config.File, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := gocsv.UnmarshalFile(file, &records); err != nil {
		return nil, err
	}
	mapping = make(map[string]string)
	for _, record := range records {
		if record.Source != "" && record.Store != "" {
			if _, ok := mapping[record.Source]; ok {
				return nil, fmt.Errorf("duplicated mapping at key: %s", record.Source)
			}
			mapping[record.Source] = record.Store
		}
	}
	return mapping, nil
}

func (mapper *CSVMapper) load() {
	mapping, err := mapper.read()
	if err != nil {
		log.Errorf("CSVMapper read mapping file failed: %s", err.Error())
	} else {
		mapper.factorMap = mapping
	}
}

func (mapper *CSVMapper) Map(factorID string) string {
	mapped, ok := mapper.factorMap[factorID]
	if !ok {
		return factorID
	} else {
		return mapped
	}
}
