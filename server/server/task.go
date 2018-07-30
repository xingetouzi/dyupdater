package server

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"fxdayu.com/dyupdater/server/services"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
)

type taskInfo struct {
	utils.TaskArchiveRecord
	Children []*taskInfo `json:"children,omitempty"`
}

var taskFilterFields = map[string]string{
	"type":   "TaskType",
	"factor": "Factor",
	"status": "Status",
}

func lessFuncInt(ai, aj reflect.Value) bool {
	return ai.Int() < aj.Int()
}

func lessFuncString(ai, aj reflect.Value) bool {
	si := ai.String()
	sj := aj.String()
	return len(si) < len(sj) || (len(si) == len(sj) && si < sj)
}

var taskSorterFields = map[string]string{
	"id":        "ID",
	"parent":    "ParentID",
	"factor":    "Factor",
	"published": "Published",
	"updated":   "Updated",
}

var taskSorterFuncs = map[string]func(ai, aj reflect.Value) bool{
	"id":        lessFuncString,
	"parent":    lessFuncString,
	"factor":    lessFuncString,
	"published": lessFuncString,
	"updated":   lessFuncString,
}

type sortHelper struct {
	key        string
	typ        reflect.Type
	descending bool
	arr        []interface{}
	less       func(ai, aj reflect.Value) bool
}

func (helper *sortHelper) Len() int {
	return len(helper.arr)
}

func (helper *sortHelper) Less(i, j int) (ret bool) {
	ai := reflect.ValueOf(helper.arr[i]).FieldByName(helper.key)
	aj := reflect.ValueOf(helper.arr[j]).FieldByName(helper.key)
	ret = helper.less(ai, aj)
	if helper.descending {
		ret = !ret
	}
	return
}

func (helper *sortHelper) Swap(i, j int) {
	helper.arr[i], helper.arr[j] = helper.arr[j], helper.arr[i]
}

func (helper *sortHelper) Sort(key string, descending bool, less func(ai, aj reflect.Value) bool) {
	helper.key = key
	helper.descending = descending
	helper.less = less
	sort.Sort(helper)
}

func newSortHelper(arr ...interface{}) (helper *sortHelper) {
	helper = &sortHelper{}
	helper.arr = arr
	return
}

func getDefaultQueryInt(c *gin.Context, key string, def int) int {
	if val, ok := c.GetQuery(key); ok {
		i, err := strconv.Atoi(val)
		if err == nil {
			return i
		}
	}
	return def
}

func handleTasks(fs *services.FactorServices) func(*gin.Context) {
	return func(c *gin.Context) {
		tfs := utils.GetTaskArchive().All()
		infos := make([]*taskInfo, 0, len(tfs))
		for _, v := range tfs {
			ti := &taskInfo{TaskArchiveRecord: v, Children: nil}
			infos = append(infos, ti)
		}
		// filter
		for queryKey, itemKey := range taskFilterFields {
			filterValueStr, ok := c.GetQuery(queryKey)
			if ok && len(filterValueStr) > 0 {
				filterValues := strings.Split(filterValueStr, ",")
				filterSet := mapset.NewSet()
				for _, v := range filterValues {
					filterSet.Add(v)
				}
				tmp := make([]*taskInfo, 0, len(tfs))
				for _, item := range infos {
					v := reflect.ValueOf(*item)
					itemValue := fmt.Sprint(v.FieldByName(itemKey).Interface())
					if filterSet.Contains(itemValue) {
						tmp = append(tmp, item)
					}
				}
				infos = tmp
			}
		}
		// sort
		sorter, ok := c.GetQuery("sorter")
		sorterKey := taskSorterFields["id"]
		sorterFunc := taskSorterFuncs["id"]
		descending := false
		if ok {
			for k, v := range taskSorterFields {
				switch sorter {
				case k + "_ascend":
					{
						sorterKey = v
						sorterFunc = taskSorterFuncs[k]
						descending = false
					}
				case k + "_descend":
					{
						sorterKey = v
						sorterFunc = taskSorterFuncs[k]
						descending = true
					}
				}
			}
		}
		infoInterfaces := make([]interface{}, len(infos))
		for i, v := range infos {
			infoInterfaces[i] = interface{}(*v)
		}
		helper := newSortHelper(infoInterfaces...)
		helper.Sort(sorterKey, descending, sorterFunc)
		// recursive
		recursive, ok := c.GetQuery("recursive")
		if recursive != "" && recursive != "false" {
			rootNodeIDs := make([]string, 0, len(infoInterfaces))
			nodes := make(map[string]*taskInfo)
			for _, item := range infoInterfaces {
				v := item.(taskInfo)
				v.Children = []*taskInfo{}
				nodes[v.ID] = &v
			}
			for _, item := range infoInterfaces {
				v := item.(taskInfo)
				if v.ParentID == "" {
					rootNodeIDs = append(rootNodeIDs, v.ID)
				} else {
					parent, ok := nodes[v.ParentID]
					if ok {
						parent.Children = append(parent.Children, nodes[v.ID])
						continue
					}
					rootNodeIDs = append(rootNodeIDs, v.ID)
				}
			}
			tmp := make([]interface{}, len(rootNodeIDs))
			for i, ID := range rootNodeIDs {
				tmp[i] = interface{}(*nodes[ID])
			}
			infoInterfaces = tmp
		}
		// pagination
		currentPage := getDefaultQueryInt(c, "currentPage", 1)
		pageSize := getDefaultQueryInt(c, "pageSize", 0)
		total := len(infoInterfaces)
		if pageSize > 0 && total > 0 {
			start := (currentPage - 1) * pageSize
			if start >= total {
				start = total - 1
			}
			end := currentPage * pageSize
			if end >= total {
				end = total
			}
			infoInterfaces = infoInterfaces[start:end]
		}
		c.JSON(200, gin.H{"list": infoInterfaces, "pagination": gin.H{
			"total":       total,
			"currentPage": currentPage,
			"pageSize":    pageSize,
		}})
	}
}
