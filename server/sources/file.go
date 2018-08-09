package sources

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/spf13/viper"
)

type fileSystemSourceConfig struct {
	Path  string `mapstructure:"path"`
	Regex string `mapstructure:"regex"`
}

/*FileSystemSource 从文件系统读取因子脚本.
因子脚本支持两种格式：
  -单文件(*.py)：文件名即代表因子名。如A00001.py代表ID为A00001的因子的脚本
  -项目式：将因子运行所需的文件放在同一个文件夹中，文件夹名即为因子名。文件夹下同名.py文件为因子的入口脚本
FileSystemSource的配置项如下：
   path: 文件系统路径,支持环境变量展开
   regex: 正则表达式，如果该项不为空，只会读取因子ID满足该正则的因子。
*/
type FileSystemSource struct {
	common.BaseComponent
	config *fileSystemSourceConfig
}

// Init the factor source from config, will open mysql connections.
func (source *FileSystemSource) Init(config *viper.Viper) {
	source.BaseComponent.Init(config)
	source.config = &fileSystemSourceConfig{Path: os.ExpandEnv("~/factors")}
	config.UnmarshalExact(source.config)
}

// Fetch the factors from source.
func (source *FileSystemSource) Fetch() []models.Factor {
	fullPath, err := filepath.Abs(source.config.Path)
	if err != nil {
		log.Errorf("Filepath error: %s", err.Error())
		return []models.Factor{}
	}
	stat, err := os.Stat(fullPath)
	if err != nil {
		log.Errorf("Filepath error: %s", err.Error())
		return []models.Factor{}
	}
	if !stat.IsDir() {
		log.Errorf("Filepath %s is not a directory", fullPath)
		return []models.Factor{}
	}
	factors := []models.Factor{}
	f := func(path string, info os.FileInfo, err error) error {
		depth := strings.Count(path, string(os.PathSeparator)) - strings.Count(fullPath, string(os.PathSeparator))
		var ret error
		if depth > 1 {
			return filepath.SkipDir
		} else if depth == 1 && info.IsDir() {
			ret = filepath.SkipDir
		} else if depth == 0 {
			return nil
		}
		var filename string
		if info.IsDir() {
			filename = filepath.Join(path, info.Name()+".py")
		} else {
			filename = path
		}
		if !utils.IsExist(filename) || filepath.Ext(filename) != ".py" {
			return ret
		}
		_, factorName := filepath.Split(filename)
		factorName = strings.TrimSuffix(factorName, ".py")
		if source.config.Regex != "" {
			match, _ := regexp.MatchString(source.config.Regex, factorName)
			if !match {
				return ret
			}
		}
		factor := models.Factor{}
		var fileContent []byte
		if info.IsDir() {
			fileContent, err = utils.ZipDirectory(filepath.Dir(filename))
		} else {
			fileContent, err = utils.ZipFile(filename)
		}
		if err != nil {
			log.Error(err.Error())
			return ret
		}
		factor.Archive = base64.StdEncoding.EncodeToString(fileContent)
		factor.ID = factorName
		factors = append(factors, factor)
		return ret
	}
	filepath.Walk(fullPath, f)
	return factors
}
