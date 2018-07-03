package services

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"time"

	"fxdayu.com/dyupdater/server/calculators"
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/indexers"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/schedulers"
	"fxdayu.com/dyupdater/server/sources"
	"fxdayu.com/dyupdater/server/stores"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	"github.com/spf13/viper"
)

var log = utils.AppLogger

type FactorServices struct {
	sources     map[string]sources.FactorSource
	calculators map[string]calculators.FactorCalculator
	stores      map[string]stores.FactorStore
	scheduler   schedulers.TaskScheduler
	indexer     indexers.TradingDatetimeIndexer
}

func (this *FactorServices) RegisterSource(name string, source sources.FactorSource) {
	this.sources[name] = source
}

func (this *FactorServices) RegisterCalculator(name string, calculator calculators.FactorCalculator) {
	this.calculators[name] = calculator
	calculator.Subscribe(this.scheduler)
}

func (this *FactorServices) RegisterStore(name string, store stores.FactorStore) {
	this.stores[name] = store
}

func (this *FactorServices) SetIndexer(indexer indexers.TradingDatetimeIndexer) {
	this.indexer = indexer
}

func (this *FactorServices) GetScheduler() schedulers.TaskScheduler {
	return this.scheduler
}

func (this *FactorServices) Check(factor models.Factor, dateRange models.DateRange) *task.TaskFuture {
	stores := make([]string, 0)
	for k, v := range this.stores {
		if v.IsEnabled() {
			stores = append(stores, k)
		}
	}
	data := task.CheckTaskPayload{Stores: stores, Factor: factor, DateRange: dateRange}
	t := task.TaskInput{Type: task.TaskTypeCheck, Payload: data}
	tf := this.scheduler.Publish(nil, t)
	return tf
}

func (service *FactorServices) CheckAll(dateRange models.DateRange) []*task.TaskFuture {
	tfs := make([]*task.TaskFuture, 0)
	for _, source := range service.sources {
		if source.IsEnabled() {
			factors := source.Fetch()
			for _, factor := range factors {
				var tf *task.TaskFuture
				for i := 0; i <= 3; i++ {
					tf = service.Check(factor, dateRange)
					if tf == nil {
						time.Sleep(time.Duration(i) * 2 * time.Second)
					} else {
						break
					}
				}
				if tf != nil {
					tfs = append(tfs, tf)
				}
			}
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

func (service *FactorServices) onCheckSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.CheckTaskResult)
	if !ok {
		return errors.New("Unvalid check result")
	}
	data := tf.Input.Payload.(task.CheckTaskPayload)
	log.Infof("(Task %s) {%s} [ %d , %d ] Check finish.", r.ID, data.Factor.ID, data.DateRange.Start, data.DateRange.End)
	maxCalDuration := utils.GetGlobalConfig().GetMaxCalDuration()
	minCalDuration := utils.GetGlobalConfig().GetMinCalDuration()
	if len(result.Datetimes) > 0 {
		var ranges []models.DateRange
		var current *models.DateRange
		for _, v := range result.Datetimes {
			if current == nil {
				current = &models.DateRange{Start: v, End: v}
			} else {
				current.End = v
				s, _ := utils.ItoDate(current.Start)
				e, _ := utils.ItoDate(current.End)
				if int(e.Sub(s).Seconds()) <= maxCalDuration {
					continue
				} else {
					ranges = append(ranges, *current)
					current = nil
				}
			}
		}
		if current != nil {
			s, _ := utils.ItoDate(current.Start)
			e, _ := utils.ItoDate(current.End)
			if int(e.Sub(s).Seconds()) <= minCalDuration {
				newS := e.Add(-time.Duration(minCalDuration) * time.Second)
				current.Start, _ = utils.Datetoi(newS)
			}
			ranges = append(ranges, *current)
		}
		for _, dateRange := range ranges {
			calData := task.CalTaskPayload{Calculator: calculators.DefaultCalculator, Factor: data.Factor, DateRange: dateRange}
			calInput := task.TaskInput{Type: task.TaskTypeCal, Payload: calData}
			service.scheduler.Publish(&tf, calInput)
		}
	}
	return nil
}

func (service *FactorServices) onCalSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.CalTaskResult)
	if !ok {
		return errors.New("Unvalid cal result")
	}
	data := tf.Input.Payload.(task.CalTaskPayload)
	log.Infof("(Task %s) { %s } [ %d , %d ] Cal finish.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	// trunc cal result
	calStartDate := utils.GetGlobalConfig().GetCalStartDate()
	factorValue := models.FactorValue{}
	if data.DateRange.Start > calStartDate {
		calStartDate = data.DateRange.Start
	}
	startIndex := sort.SearchInts(result.FactorValue.Datetime, calStartDate)
	endIndex := sort.Search(len(result.FactorValue.Datetime), func(i int) bool { return result.FactorValue.Datetime[i] > data.DateRange.End })
	if startIndex >= len(result.FactorValue.Datetime) {
		log.Infof("(Task %s) { %s } [ %d , %d ] No data to update.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
		return nil
	}
	factorValue.Datetime = result.FactorValue.Datetime[startIndex:endIndex]
	factorValue.Values = map[string][]float64{}
	for k, v := range result.FactorValue.Values {
		factorValue.Values[k] = v[startIndex:endIndex]
	}
	// update cal result
	for name, store := range service.stores {
		if store.IsEnabled() {
			payload := task.UpdateTaskPayload{Store: name, Factor: data.Factor, FactorValue: factorValue}
			input := task.TaskInput{Type: task.TaskTypeUpdate, Payload: payload}
			service.scheduler.Publish(&tf, input)
		}
	}
	return nil
}

func (service *FactorServices) onCalFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.CalTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Cal failed: %s", tf.ID, data.Factor.ID, data.DateRange.Start, data.DateRange.End, err)
}

func (this *FactorServices) onCheckFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.CalTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Check failed: %s", tf.ID, data.Factor.ID, data.DateRange.Start, data.DateRange.End, err)
}

func (this *FactorServices) handleCheck(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.CheckTaskPayload)
	if !ok {
		return errors.New("Unvalid check task")
	}
	dateSet := mapset.NewSet()
	index := this.indexer.GetIndex(data.DateRange)
	log.Infof("(Task %s) { %s } [ %d , %d ] Check begin.", tf.ID, data.Factor.ID, data.DateRange.Start, data.DateRange.End)
	for _, name := range data.Stores {
		store, ok := this.stores[name]
		if !ok {
			continue
		}
		dates, err := store.Check(data.Factor, index)
		if err != nil {
			log.Warningf("(Task %s) { %s } [ %d , %d ] Store[%s] check failed: %s.", tf.ID, data.GetFactorID(),
				data.GetStartTime(), data.GetEndTime(), name, err.Error())
		}
		newSet := mapset.NewSet()
		for _, v := range dates {
			newSet.Add(v)
		}
		dateSet = dateSet.Union(newSet)
	}
	allDates := make([]int, dateSet.Cardinality())
	for i, v := range dateSet.ToSlice() {
		allDates[i] = v.(int)
	}
	sort.Ints(allDates)
	startIndex := sort.SearchInts(allDates, data.DateRange.Start)
	var endIndex int
	if data.DateRange.End <= 0 {
		endIndex = len(allDates)
	} else {
		endIndex = sort.Search(len(allDates), func(i int) bool { return allDates[i] > data.DateRange.End })
	}
	output := new(task.TaskResult)
	result := task.CheckTaskResult{Datetimes: allDates[startIndex:endIndex]}
	output.ID = tf.ID
	output.Type = task.TaskTypeCheck
	output.Result = result
	out := this.scheduler.GetOutputChan()
	out <- *output
	return nil
}

func (this *FactorServices) handleCal(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.CalTaskPayload)
	if !ok {
		return errors.New("Unvalid cal task")
	}
	calculator, ok := this.calculators[data.Calculator]
	if !ok {
		return fmt.Errorf("Calculator not Found: %s", data.Calculator)
	}
	log.Infof(
		"(Task %s) { %s }  [ %d , %d ] Cal begin.",
		tf.ID, data.Factor.ID, data.DateRange.Start, data.DateRange.End,
	)
	err := calculator.Cal(tf.ID, data.Factor, data.DateRange)
	if err != nil {
		return err
	}
	return nil
}

func (service *FactorServices) handleUpdate(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.UpdateTaskPayload)
	if !ok {
		return errors.New("Unvalid update task")
	}
	log.Infof(
		"(Task %s) { %s }  [ %d , %d ] Update begin.",
		tf.ID, data.Factor.ID, data.GetStartTime(), data.GetEndTime(),
	)
	store, ok := service.stores[data.Store]
	if !ok {
		return fmt.Errorf("Store not Found: %s", data.Store)
	}
	count, err := store.Update(data.Factor, data.FactorValue, false)
	if err != nil {
		return err
	}
	output := new(task.TaskResult)
	result := task.UpdateTaskResult{Count: count}
	output.ID = tf.ID
	output.Type = task.TaskTypeCheck
	output.Result = result
	out := service.scheduler.GetOutputChan()
	out <- *output
	return nil
}

func (service *FactorServices) onUpdateSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.UpdateTaskResult)
	if !ok {
		return errors.New("Unvalid cal result")
	}
	data := tf.Input.Payload.(task.UpdateTaskPayload)
	log.Infof("(Task %s) { %s } [ %d , %d ] Update finish. %d record was updated.", r.ID, data.Factor.ID, data.GetStartTime(), data.GetEndTime(), result.Count)
	return nil
}

func (service *FactorServices) onUpdateFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.UpdateTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Update failed in store %s: %s", tf.ID, data.Factor.ID, data.GetStartTime(), data.GetEndTime(), data.Store, err)
}

func NewFactorServices(s schedulers.TaskScheduler) *FactorServices {
	fs := &FactorServices{}
	fs.scheduler = s
	fs.sources = make(map[string]sources.FactorSource)
	fs.calculators = make(map[string]calculators.FactorCalculator)
	fs.stores = make(map[string]stores.FactorStore)
	s.AppendHandler(int(task.TaskTypeCal), fs.handleCal)
	s.AppendHandler(int(task.TaskTypeCheck), fs.handleCheck)
	s.AppendHandler(int(task.TaskTypeUpdate), fs.handleUpdate)
	s.AppendSuccessHandler(int(task.TaskTypeCal), fs.onCalSuccess)
	s.AppendSuccessHandler(int(task.TaskTypeCheck), fs.onCheckSuccess)
	s.AppendSuccessHandler(int(task.TaskTypeUpdate), fs.onUpdateSuccess)
	s.AppendFailureHandler(int(task.TaskTypeCal), fs.onCalFailed)
	s.AppendFailureHandler(int(task.TaskTypeCheck), fs.onCheckFailed)
	s.AppendFailureHandler(int(task.TaskTypeUpdate), fs.onUpdateFailed)
	s.SetOutLimit(int(task.TaskTypeCal), runtime.NumCPU())
	s.SetInLimit(int(task.TaskTypeCal), runtime.NumCPU()*2)
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

func getConfig() {
	viper.SetConfigName("dyupdater")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.fxdayu")
	viper.AddConfigPath("/etc/fxdayu/")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error in config file: %s\n", err))
	}
}

func getConfigFromPath(p string) {
	file, err := os.OpenFile(p, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	viper.ReadConfig(file)
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
					panic(fmt.Errorf("Source %s with type %s is not valid factor sources!", name, t))
				}
				fs.RegisterSource(name, source)
				components[source] = config
			} else {
				panic(fmt.Errorf("Source %s with unknown type %s!", name, tName))
			}
		} else {
			panic(fmt.Errorf("Source %s with undefine type!", name))
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
					panic(fmt.Errorf("Calculator %s with type %s is not a valid factor calculator!", name, t))
				}
				fs.RegisterCalculator(name, calculator)
				components[calculator] = config
			} else {
				panic(fmt.Errorf("Calculator %s with unknown type %s!", name, tName))
			}
		} else {
			panic(fmt.Errorf("Calculator %s with undefine type!", name))
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
					panic(fmt.Errorf("Store %s with type %s is not a valid factor store!", name, t))
				}
				fs.RegisterStore(name, store)
				components[store] = config
			} else {
				panic(fmt.Errorf("Store %s with unknown type %s!", name, tName))
			}
		} else {
			panic(fmt.Errorf("Store %s with undefine type!", name))
		}
	}
	// parse indexer config
	if !viper.IsSet("indexer") {
		panic(errors.New("no indexer config is provided\n"))
	}
	config := viper.Sub("indexer")
	if config.IsSet("type") {
		tName := config.GetString("type")
		t, ok := indexerMap[tName]
		if ok {
			indexer, ok := reflect.New(t).Interface().(indexers.TradingDatetimeIndexer)
			if !ok {
				panic(fmt.Errorf("Indexer with type %s is not a valid factor store!", t))
			}
			fs.SetIndexer(indexer)
			components[indexer] = config
		}
	} else {
		panic(fmt.Errorf("Indexer with undefine type!"))
	}
	for v, c := range components {
		v.Init(c)
	}
	return fs
}
