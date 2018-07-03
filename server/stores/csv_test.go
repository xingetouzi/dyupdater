package stores

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"fxdayu.com/dyupdater/server/models"
	"github.com/spf13/viper"
)

var store *CSVStore
var factor models.Factor
var root string

func initFile() {
	data := `TDATE,SYMBOLCODE,RAWVALUE
20100101,600261.SH,0
20100102,600261.SH,0
20100103,600261.SH,0
20100104,600261.SH,0
20100105,600261.SH,0
20100107,600261.SH,0
20100108,600261.SH,0
20100109,600261.SH,0
20100110,600261.SH,0
20100111,600261.SH,0
`
	err := ioutil.WriteFile(path.Join(root, factor.ID+".csv"), []byte(data), os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func init() {
	var err error
	root, err = ioutil.TempDir("", "csv_test")
	if err != nil {
		panic(err)
	}
	factorName := "test"
	factor = models.Factor{ID: factorName}
	config := viper.New()
	config.SetDefault("path", root)
	store = &CSVStore{}
	store.Init(config)
}

func TestCSVRead(t *testing.T) {
	ioutil.WriteFile(path.Join(root, "test.csv"), []byte{}, os.ModePerm)
	records := store.readFactor(factor)
	if len(records) != 0 {
		t.Errorf("Length of records dismatch! Desired: 0, Given: %d", len(records))
	}
	ioutil.WriteFile(path.Join(root, "test.csv"), []byte("TDATE,SYMBOLCODE,RAWVALUE"), os.ModePerm)
	records = store.readFactor(factor)
	if len(records) != 0 {
		t.Errorf("Length of records dismatch! Desired: 0, Given: %d", len(records))
	}
	initFile()
	records = store.readFactor(factor)
	if len(records) != 10 {
		t.Errorf("Length of records dismatch! Desired: 10, Given: %d", len(records))
	}
}

func checkFileContent(t *testing.T, filename string, content string, step int) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
		return
	}
	filecontent := string(b)
	if strings.Compare(filecontent, content) != 0 {
		t.Logf("File:\n%s", filecontent)
		t.Logf("Content:\n%s", content)
		t.Fatalf("Content missmatch in step %d", step)
	}
}

func TestCSVUpdate(t *testing.T) {
	data := []models.FactorValue{}
	content := []string{}
	data = append(data, models.FactorValue{
		Datetime: []int{20100106},
		Values:   map[string][]float64{"600261.SH": []float64{0}},
	})
	content = append(content, `TDATE,SYMBOLCODE,RAWVALUE
20100101,600261.SH,0
20100102,600261.SH,0
20100103,600261.SH,0
20100104,600261.SH,0
20100105,600261.SH,0
20100106,600261.SH,0
20100107,600261.SH,0
20100108,600261.SH,0
20100109,600261.SH,0
20100110,600261.SH,0
20100111,600261.SH,0
`)
	data = append(data, models.FactorValue{
		Datetime: []int{20091231},
		Values:   map[string][]float64{"600261.SH": []float64{0}},
	})
	content = append(content, `TDATE,SYMBOLCODE,RAWVALUE
20091231,600261.SH,0
20100101,600261.SH,0
20100102,600261.SH,0
20100103,600261.SH,0
20100104,600261.SH,0
20100105,600261.SH,0
20100106,600261.SH,0
20100107,600261.SH,0
20100108,600261.SH,0
20100109,600261.SH,0
20100110,600261.SH,0
20100111,600261.SH,0
`)
	data = append(data, models.FactorValue{
		Datetime: []int{20100112},
		Values:   map[string][]float64{"600261.SH": []float64{0}},
	})
	content = append(content, `TDATE,SYMBOLCODE,RAWVALUE
20091231,600261.SH,0
20100101,600261.SH,0
20100102,600261.SH,0
20100103,600261.SH,0
20100104,600261.SH,0
20100105,600261.SH,0
20100106,600261.SH,0
20100107,600261.SH,0
20100108,600261.SH,0
20100109,600261.SH,0
20100110,600261.SH,0
20100111,600261.SH,0
20100112,600261.SH,0
`)
	data = append(data, models.FactorValue{
		Datetime: []int{20100104, 20100105, 20100106, 20100107, 20100108},
		Values:   map[string][]float64{"600261.SH": []float64{1, 1, 1, 1, 1}},
	})
	content = append(content, `TDATE,SYMBOLCODE,RAWVALUE
20091231,600261.SH,0
20100101,600261.SH,0
20100102,600261.SH,0
20100103,600261.SH,0
20100104,600261.SH,1
20100105,600261.SH,1
20100106,600261.SH,1
20100107,600261.SH,1
20100108,600261.SH,1
20100109,600261.SH,0
20100110,600261.SH,0
20100111,600261.SH,0
20100112,600261.SH,0
`)
	data = append(data, models.FactorValue{
		Datetime: []int{20091231, 20100101, 20100102, 20100103, 20100104},
		Values:   map[string][]float64{"600261.SH": []float64{2, 2, 2, 2, 2}},
	})
	content = append(content, `TDATE,SYMBOLCODE,RAWVALUE
20091231,600261.SH,2
20100101,600261.SH,2
20100102,600261.SH,2
20100103,600261.SH,2
20100104,600261.SH,2
20100105,600261.SH,1
20100106,600261.SH,1
20100107,600261.SH,1
20100108,600261.SH,1
20100109,600261.SH,0
20100110,600261.SH,0
20100111,600261.SH,0
20100112,600261.SH,0
`)
	data = append(data, models.FactorValue{
		Datetime: []int{20100108, 20100109, 20100110, 20100111, 20100112},
		Values:   map[string][]float64{"600261.SH": []float64{3, 3, 3, 3, 3}},
	})
	content = append(content, `TDATE,SYMBOLCODE,RAWVALUE
20091231,600261.SH,2
20100101,600261.SH,2
20100102,600261.SH,2
20100103,600261.SH,2
20100104,600261.SH,2
20100105,600261.SH,1
20100106,600261.SH,1
20100107,600261.SH,1
20100108,600261.SH,3
20100109,600261.SH,3
20100110,600261.SH,3
20100111,600261.SH,3
20100112,600261.SH,3
`)
	initFile()
	filename := store.getCSVPath(factor)
	for i, v := range data {
		store.Update(factor, v, false)
		checkFileContent(t, filename, content[i], i)
	}
}
