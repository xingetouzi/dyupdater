package utils

import (
	"bytes"
	"os"
	"strconv"
	"time"
)

func CheckError(err interface{}) {
	if err != nil {
		panic(err)
	}
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func ItoDate(value int) (time.Time, error) {
	return time.Parse("20060102", strconv.Itoa(value))
}

func Datetoi(value time.Time) (int, error) {
	tmp := value.Format("20060102")
	return strconv.Atoi(tmp)
}

func MaxInt(array []int) int {
	var max int = array[0]
	for _, value := range array {
		if max < value {
			max = value
		}
	}
	return max
}

func MinInt(array []int) int {
	var min int = array[0]
	for _, value := range array {
		if min > value {
			min = value
		}
	}
	return min
}

var bom = "\xef\xbb\xbf"

func StripBOM(fileBytes []byte) []byte {
	trimmedBytes := bytes.Trim(fileBytes, bom)
	return trimmedBytes
}

func IsExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
