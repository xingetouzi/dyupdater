package sources

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fxdayu.com/dyupdater/server/models"
	"github.com/spf13/viper"
)

func getFactorsMap(factors []models.Factor) map[string]*models.Factor {
	ret := make(map[string]*models.Factor)
	for _, v := range factors {
		factor := v
		ret[v.ID] = &factor
	}
	fmt.Println(ret)
	return ret
}

var aaaContent = "\"Are you OK~!\""
var ccccContent = "\"Rrrrrrrr U OK\""
var dddContent = "\"I am fine\""
var ddd2Content = "\"I am not fine\""
var eeeeContent = "\"Thank you~\""

func TestFileSource(t *testing.T) {
	config := viper.New()
	path, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	config.Set("enabled", true)
	config.Set("path", filepath.Join(path, "file_test_case"))
	source := &FileSystemSource{}
	source.Init(config)
	factors := getFactorsMap(source.Fetch())
	checkFactor := func(factors map[string]*models.Factor, name string, content string, exist bool) {
		factor, ok := factors[name]
		if ok != exist {
			t.Errorf("Exist status factor %s should be %t, but %t", name, exist, ok)
			return
		}
		if !ok {
			return
		}
		if strings.Compare(factor.Formula, content) != 0 {
			t.Errorf("Contents of factor %s are not consistent:\n%s\n%s", name, factor.Formula, content)
		}
	}
	checkFactor(factors, "aaa", aaaContent, true)
	checkFactor(factors, "bbb", "", false)
	checkFactor(factors, "bb", "", false)
	checkFactor(factors, "cccc", ccccContent, true)
	checkFactor(factors, "ddd", dddContent, true)
	checkFactor(factors, "ddd-不同", ddd2Content, true)
	checkFactor(factors, "eeee", eeeeContent, true)
	config.Set("regex", "^\\w{0,3}$")
	source = &FileSystemSource{}
	source.Init(config)
	factors = getFactorsMap(source.Fetch())
	checkFactor(factors, "aaa", aaaContent, true)
	checkFactor(factors, "bbb", "", false)
	checkFactor(factors, "bb", "", false)
	checkFactor(factors, "cccc", "", false)
	checkFactor(factors, "ddd", dddContent, true)
	checkFactor(factors, "ddd-不同", "", false)
	checkFactor(factors, "eeee", "", false)
}
