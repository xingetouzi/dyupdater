package mappers

import (
	"errors"
	"fmt"
	"os"

	"github.com/deckarep/golang-set"

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
	File   string        `mapstructure:"file"`
	Watch  bool          `mapstructure:"watch"`
	Stores []interface{} `mapstructure:"stores"`
}

// CSVMapper 从CSV文件中读取因子名映射表，如某因子名出现在表中，则对其进行映射，否则则保持原因子名。
// CSV映射表格式如下：
//   | source | store |
// source列表示映射前的因子名，store列表示映射后的因子名。
//
// CSVMapper的配置项如下：
//   file: 对应CSV映射表文件的位置。
//   stores: array of string, 表示哪些要对哪些stores启用映射，如果不指定，则默认对所有的stores都启用映射。
type CSVMapper struct {
	common.BaseComponent
	config    csvMapperConfig
	factorMap map[string]string
	stores    mapset.Set
}

func (mapper *CSVMapper) Init(config *viper.Viper) {
	mapper.BaseComponent.Init(config)
	mapper.config = csvMapperConfig{
		Watch: true,
	}
	config.Unmarshal(&mapper.config)
	mapper.stores = mapset.NewSetFromSlice(mapper.config.Stores)
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

func (mapper *CSVMapper) Map(store string, factorID string) string {
	stores := mapper.stores
	if stores.Cardinality() > 0 && !stores.Contains(store) {
		return factorID
	}
	mapped, ok := mapper.factorMap[factorID]
	if !ok {
		return factorID
	} else {
		return mapped
	}
}
