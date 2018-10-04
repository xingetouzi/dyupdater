package services

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"fxdayu.com/dyupdater/server/calculators"
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/indexers"
	"fxdayu.com/dyupdater/server/mappers"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/schedulers"
	"fxdayu.com/dyupdater/server/sources"
	"fxdayu.com/dyupdater/server/stores"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/spf13/viper"
)

var log = utils.AppLogger

type FactorTaskHandler interface {
	GetTaskType() task.TaskType
	Handle(tf *task.TaskFuture) error
	OnSuccess(tf task.TaskFuture, r task.TaskResult) error
	OnFailed(tf task.TaskFuture, err error)
}

type FactorServices struct {
	sources     map[string]sources.FactorSource
	calculators map[string]calculators.FactorCalculator
	stores      map[string]stores.FactorStore
	count       sync.Map
	scheduler   schedulers.TaskScheduler
	indexer     indexers.TradingDatetimeIndexer
	mapper      mappers.FactorNameMapper
}

func (service *FactorServices) RegisterHandler(handler FactorTaskHandler) {
	service.scheduler.AppendHandler(int(handler.GetTaskType()), handler.Handle)
	service.scheduler.AppendSuccessHandler(int(handler.GetTaskType()), handler.OnSuccess)
	service.scheduler.AppendFailureHandler(int(handler.GetTaskType()), handler.OnFailed)
}

func (service *FactorServices) RegisterSource(name string, source sources.FactorSource) {
	service.sources[name] = source
}

func (service *FactorServices) RegisterCalculator(name string, calculator calculators.FactorCalculator) {
	service.calculators[name] = calculator
	calculator.Subscribe(service.scheduler)
}

func (service *FactorServices) RegisterStore(name string, store stores.FactorStore) {
	service.stores[name] = store
}

func (service *FactorServices) SetIndexer(indexer indexers.TradingDatetimeIndexer) {
	service.indexer = indexer
}

func (service *FactorServices) SetMapper(mapper mappers.FactorNameMapper) {
	service.mapper = mapper
}

func (service *FactorServices) GetScheduler() schedulers.TaskScheduler {
	return service.scheduler
}

func (service *FactorServices) mapFactor(store string, factor models.Factor) (newFactor models.Factor) {
	newFactor = factor
	if service.mapper != nil {
		newFactor.ID = service.mapper.Map(store, newFactor.ID)
	}
	return
}

func (service *FactorServices) Check(factor models.Factor, dateRange models.DateRange) *task.TaskFuture {
	stores := make([]string, 0)
	for k, v := range service.stores {
		if v.IsEnabled() {
			stores = append(stores, k)
		}
	}
	ti := task.NewCheckTaskInput(stores, factor, dateRange, task.ProcessTypeNone)
	tf := service.scheduler.Publish(nil, ti)
	return tf
}

func (service *FactorServices) CheckWithLock(factor models.Factor, dateRange models.DateRange) *task.TaskFuture {
	if val, ok := service.count.Load(factor.ID); ok {
		count := val.(int)
		if count > 0 {
			return nil
		}
	}
	stores := make([]string, 0)
	for k, v := range service.stores {
		if v.IsEnabled() {
			stores = append(stores, k)
		}
	}
	ti := task.NewCheckTaskInput(stores, factor, dateRange, task.ProcessTypeNone)
	tf := service.scheduler.Publish(nil, ti)
	if tf != nil {
		service.count.Store(factor.ID, 1)
	}
	return tf
}

func (service *FactorServices) CheckAll(dateRange models.DateRange) []*task.TaskFuture {
	tfs := make([]*task.TaskFuture, 0)
	for _, source := range service.sources {
		if source.IsEnabled() {
			factors := source.Fetch()
			ch := make(chan models.Factor, 10000)
			count := len(factors)
			go func(factors []models.Factor) {
				for _, factor := range factors {
					ch <- factor
				}
			}(factors)
			for factor := range ch {
				var tf *task.TaskFuture
				tf = service.Check(factor, dateRange)
				if tf != nil {
					tfs = append(tfs, tf)
					count--
					if count == 0 {
						break
					}
				} else {
					select {
					case ch <- factor:
						continue
					default:
						go func(factor models.Factor) {
							ch <- factor
						}(factor)
					}
				}
			}
			close(ch)
		}
	}
	return tfs
}

func (service *FactorServices) Wait(timeout int) {
	ch := make(chan bool)
	service.scheduler.Wait(ch)
	if timeout == 0 {
		<-ch
	} else {
		select {
		case <-ch:
		case <-time.After(time.Duration(timeout) * time.Second):
		}
	}
}

func NewFactorServices(s schedulers.TaskScheduler) *FactorServices {
	fs := &FactorServices{}
	fs.scheduler = s
	fs.sources = make(map[string]sources.FactorSource)
	fs.calculators = make(map[string]calculators.FactorCalculator)
	fs.stores = make(map[string]stores.FactorStore)
	s.RegisterTaskType(int(task.TaskTypeCal), 0, runtime.NumCPU())
	s.RegisterTaskType(int(task.TaskTypeCheck), 0, 0)
	s.RegisterTaskType(int(task.TaskTypeUpdate), runtime.NumCPU(), 0)
	s.RegisterTaskType(int(task.TaskTypeProcess), runtime.NumCPU(), runtime.NumCPU())
	fs.RegisterHandler(newCalTaskHandler(fs))
	fs.RegisterHandler(newCheckTaskHandler(fs))
	fs.RegisterHandler(newUpdateTaskHandler(fs))
	fs.RegisterHandler(newProcessTaskHandler(fs))
	return fs
}

var sourceMap = map[string]reflect.Type{
	"mysql":      reflect.TypeOf(sources.MysqlSource{}),
	"filesystem": reflect.TypeOf(sources.FileSystemSource{}),
}

var calculatorMap = map[string]reflect.Type{
	"celery": reflect.TypeOf(calculators.CeleryCalculator{}),
}

var storeMap = map[string]reflect.Type{
	"mongo":  reflect.TypeOf(stores.MongoStore{}),
	"csv":    reflect.TypeOf(stores.CSVStore{}),
	"oracle": reflect.TypeOf(stores.OracleStore{}),
	"hdf5":   reflect.TypeOf(stores.HDF5Store{}),
}

var indexerMap = map[string]reflect.Type{
	"mongo": reflect.TypeOf(indexers.MongoIndexer{}),
	"wind":  reflect.TypeOf(indexers.WindMssqlIndexer{}),
}

var mapperMap = map[string]reflect.Type{
	"csv": reflect.TypeOf(mappers.CSVMapper{}),
}

func RegisterSourceType(name string, t reflect.Type) {
	sourceMap[name] = t
}

func RegisterCalculatorType(name string, t reflect.Type) {
	calculatorMap[name] = t
}

func RegisterStoreType(name string, t reflect.Type) {
	storeMap[name] = t
}

func RegisterIndexType(name string, t reflect.Type) {
	indexerMap[name] = t
}

func RegisterMapperType(name string, t reflect.Type) {
	mapperMap[name] = t
}

func getConfig() {
	viper.SetConfigName("dyupdater")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.fxdayu")
	viper.AddConfigPath("/etc/fxdayu/")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error in config file: %s", err))
	}
}

func getConfigFromPath(p string) {
	file, err := os.OpenFile(p, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	viper.SetConfigName("dyupdater")
	viper.SetConfigType("yaml")
	err = viper.ReadConfig(file)
	if err != nil {
		panic(err)
	}
}

func NewFactorServiceFromConfig(p string) *FactorServices {
	s := new(schedulers.InMemoryTaskScheduler)
	s.Init(nil)
	fs := NewFactorServices(s)
	if p == "" {
		getConfig()
	} else {
		getConfigFromPath(p)
	}
	components := make(map[common.Configable]*viper.Viper)
	// parse global config
	if viper.IsSet("base") {
		config := viper.Sub("base")
		utils.GetGlobalConfig().Init(config)
	}
	// parse sources config
	for name := range viper.GetStringMap("sources") {
		config := viper.Sub("sources." + name)
		if config.IsSet("type") {
			tName := config.GetString("type")
			t, ok := sourceMap[tName]
			if ok {
				source, ok := reflect.New(t).Interface().(sources.FactorSource)
				if !ok {
					panic(fmt.Errorf("source %s with type %s is not valid FactorSource", name, t))
				}
				fs.RegisterSource(name, source)
				components[source] = config
			} else {
				panic(fmt.Errorf("source %s with unknown type %s", name, tName))
			}
		} else {
			panic(fmt.Errorf("source %s with undefine type", name))
		}
	}
	// parsse calculators config
	for name := range viper.GetStringMap("calculators") {
		config := viper.Sub("calculators." + name)
		if config.IsSet("type") {
			tName := config.GetString("type")
			t, ok := calculatorMap[tName]
			if ok {
				calculator, ok := reflect.New(t).Interface().(calculators.FactorCalculator)
				if !ok {
					panic(fmt.Errorf("calculator %s with type %s is not a valid FactorCalculator", name, t))
				}
				fs.RegisterCalculator(name, calculator)
				components[calculator] = config
			} else {
				panic(fmt.Errorf("calculator %s with unknown type %s", name, tName))
			}
		} else {
			panic(fmt.Errorf("calculator %s with undefine type", name))
		}
	}
	// parse stores config
	for name := range viper.GetStringMap("stores") {
		config := viper.Sub("stores." + name)
		if config.IsSet("type") {
			tName := config.GetString("type")
			t, ok := storeMap[tName]
			if ok {
				store, ok := reflect.New(t).Interface().(stores.FactorStore)
				if !ok {
					panic(fmt.Errorf("store %s with type %s is not a valid FactorStore", name, t))
				}
				fs.RegisterStore(name, store)
				components[store] = config
			} else {
				panic(fmt.Errorf("store %s with unknown type %s", name, tName))
			}
		} else {
			panic(fmt.Errorf("store %s with undefine type", name))
		}
	}
	// parse indexer config
	if viper.IsSet("indexer") {
		config := viper.Sub("indexer")
		if config.IsSet("type") {
			tName := config.GetString("type")
			t, ok := indexerMap[tName]
			if ok {
				indexer, ok := reflect.New(t).Interface().(indexers.TradingDatetimeIndexer)
				if !ok {
					panic(fmt.Errorf("indexer with type %s is not a valid TradingDatetimeIndexer", t))
				}
				fs.SetIndexer(indexer)
				components[indexer] = config
			}
		} else {
			panic(fmt.Errorf("indexer with undefine type"))
		}
	} else {
		panic(errors.New("no indexer config is provided"))
	}

	// parse mapper config
	if viper.IsSet("mapper") {
		config := viper.Sub("mapper")
		if config.IsSet("type") {
			tName := config.GetString("type")
			t, ok := mapperMap[tName]
			if ok {
				mapper, ok := reflect.New(t).Interface().(mappers.FactorNameMapper)
				if !ok {
					panic(fmt.Errorf("mapper with type %s is not a valid FactorNameMapper", t))
				}
				fs.SetMapper(mapper)
				components[mapper] = config
			}
		} else {
			log.Warning(errors.New("no mapper config is provided").Error())
		}
	}
	// init all components
	for v, c := range components {
		v.Init(c)
	}
	return fs
}
