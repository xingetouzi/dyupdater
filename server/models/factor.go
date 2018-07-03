package models

import (
	"math"
)

type Factor struct {
	ID      string `json:"name"`
	Formula string `json:"formula"`
}

type FactorValue struct {
	Datetime []int
	Values   map[string][]float64
}

func (value *FactorValue) Check() bool {
	n := len(value.Datetime)
	for _, v := range value.Values {
		if len(v) != n {
			return false
		}
	}
	return true
}

func (value *FactorValue) DropNAN() {
	var datetime []int
	var values map[string][]float64
	if !value.Check() {
		datetime = []int{}
		values = map[string][]float64{}
	} else {
		datetime = make([]int, 0, len(value.Datetime))
		values = make(map[string][]float64)
		for k := range value.Values {
			values[k] = make([]float64, 0, (len(value.Datetime)))
		}
		for i, dt := range value.Datetime {
			allNAN := true
			for _, v := range value.Values {
				if math.IsNaN(v[i]) || math.IsInf(v[i], 0) {
					continue
				}
				allNAN = false
				break
			}
			if !allNAN {
				datetime = append(datetime, dt)
				for k := range value.Values {
					values[k] = append(values[k], value.Values[k][i])
				}
			}
		}
	}
	value.Datetime = datetime
	value.Values = values
}
