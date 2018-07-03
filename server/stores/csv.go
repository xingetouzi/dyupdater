package stores

import (
	"math"
	"os"
	"path"
	"runtime"
	"sort"
	"sync"

	"fxdayu.com/dyupdater/server/common"

	"fxdayu.com/dyupdater/server/models"
	"github.com/deckarep/golang-set"
	"github.com/gocarina/gocsv"
	"github.com/spf13/viper"
)

type factorValueRecord struct {
	TDate      int     `csv:"TDATE"`
	SymbolCode string  `csv:"SYMBOLCODE"`
	RawValue   float64 `csv:"RAWVALUE"`
}

type factorValueRecords []*factorValueRecord

func (records factorValueRecords) Len() int {
	return len(records)
}

func (records factorValueRecords) Less(i, j int) bool {
	return records[i].TDate < records[j].TDate
}

func (records factorValueRecords) Swap(i, j int) {
	tmp := records[i]
	records[i] = records[j]
	records[j] = tmp
}

type csvStoreConfig struct {
	common.BaseConfig
	Path string `mapstructure:"path"`
}

// CSVStore 是将因子数据存为csv文件的因子存储
// CSVStore 效率比较低下，不建议使用。
//
// CSVStore 的配置项有：
//   path: csv文件将存放在该目录下，不存在会自动创建，每一个因子对应一个同名csv文件。
type CSVStore struct {
	common.BaseComponent
	config *csvStoreConfig
	locks  map[string]*sync.RWMutex
	limit  chan bool
}

// Init the CSVStore, will create the root directory.
func (s *CSVStore) Init(config *viper.Viper) {
	s.BaseComponent.Init(config)
	s.config = &csvStoreConfig{Path: "."}
	config.Unmarshal(s.config)
	s.locks = make(map[string]*sync.RWMutex)
	s.limit = make(chan bool, runtime.NumCPU())
	s.config.Path = os.ExpandEnv(s.config.Path)
	os.MkdirAll(s.config.Path, os.ModePerm)
}

func (s *CSVStore) getCSVPath(factor models.Factor) string {
	p := path.Join(s.config.Path, factor.ID+".csv")
	return p
}

func (s *CSVStore) getLock(factor models.Factor) *sync.RWMutex {
	lock, ok := s.locks[factor.ID]
	if !ok {
		lock = &sync.RWMutex{}
		s.locks[factor.ID] = lock
	}
	return lock
}

func (s *CSVStore) release() {
	select {
	case <-s.limit:
	default:
	}
}

func (s *CSVStore) readFactor(factor models.Factor) factorValueRecords {
	lock := s.getLock(factor)
	p := s.getCSVPath(factor)
	records := factorValueRecords{}
	lock.RLock()
	defer lock.RUnlock()
	file, err := os.OpenFile(p, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return records
	}
	defer file.Close()
	if err := gocsv.UnmarshalFile(file, &records); err != nil {
		log.Error(err)
		return factorValueRecords{}
	}
	return records
}

func (s *CSVStore) writeFactor(factor models.Factor, recordsArray ...factorValueRecords) error {
	if len(recordsArray) == 0 {
		return nil
	}
	lock := s.getLock(factor)
	p := s.getCSVPath(factor)
	tmp := p + ".tmp"
	lock.Lock()
	defer lock.Unlock()
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	headered := false
	for _, records := range recordsArray {
		if len(records) == 0 {
			continue
		}
		if !headered {
			err = gocsv.MarshalFile(records, file)
			headered = true
		} else {
			err = gocsv.MarshalWithoutHeaders(records, file)
		}
		if err != nil {
			return err
		}
	}
	file.Close()
	return os.Rename(tmp, p)
}

// Check factor data for given factor and daterange in CSVStore.
func (s *CSVStore) Check(factor models.Factor, index []int) ([]int, error) {
	s.limit <- true
	defer s.release()
	indexSet := mapset.NewSet()
	for _, v := range index {
		indexSet.Add(interface{}(v))
	}
	records := s.readFactor(factor)
	existSet := mapset.NewSet()
	for _, record := range records {
		existSet.Add(record.TDate)
	}
	lostSet := indexSet.Difference(existSet)
	lost := make([]int, lostSet.Cardinality())
	i := 0
	for v := range lostSet.Iter() {
		lost[i] = v.(int)
		i++
	}
	return lost, nil
}

// Update factor data of given factor and factor values in CSVStore.
func (s *CSVStore) Update(factor models.Factor, factorValue models.FactorValue, replace bool) (int, error) { // TODO replace params
	if len(factorValue.Datetime) == 0 {
		return 0, nil
	}
	s.limit <- true
	defer s.release()
	records := s.readFactor(factor)
	sort.Sort(records)
	newRecords := []*factorValueRecord{}
	for i, dt := range factorValue.Datetime {
		for symbol, values := range factorValue.Values {
			if !(math.IsNaN(values[i]) || math.IsInf(values[i], 0)) {
				newRecords = append(newRecords, &factorValueRecord{TDate: dt, SymbolCode: symbol, RawValue: values[i]})
			}
		}
	}
	if !replace {
		dateSet := mapset.NewSet()
		for _, v := range records {
			dateSet.Add(v.TDate)
		}
		var l, r int
		for l = 0; l < len(newRecords); l++ {
			if !dateSet.Contains(newRecords[l].TDate) {
				break
			}
		}
		if l == len(newRecords) {
			newRecords = []*factorValueRecord{}
		} else {
			for r = len(newRecords) - 1; l >= 0; l-- {
				if !dateSet.Contains(newRecords[r].TDate) {
					break
				}
			}
			newRecords = newRecords[l : r+1]
		}
	}
	if len(newRecords) == 0 {
		log.Debugf("CSV data of %s no need to be update.", factor.ID)
		return 0, nil
	}
	start := newRecords[0].TDate
	end := newRecords[len(newRecords)-1].TDate
	startIndex := sort.Search(len(records), func(i int) bool { return records[i].TDate >= start })
	endIndex := sort.Search(len(records), func(i int) bool { return records[i].TDate > end })
	s1 := records[:startIndex]
	s2 := newRecords
	s3 := records[endIndex:]
	log.Debugf("Update CSV data of %s in [%d, %d]", factor.ID, newRecords[0].TDate, newRecords[len(newRecords)-1].TDate)
	err := s.writeFactor(factor, s1, s2, s3)
	if err != nil {
		return 0, err
	}
	return len(s2), nil
}
