package services

import (
	"errors"
	"sort"
	"time"

	"fxdayu.com/dyupdater/server/calculators"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
)

type checkTaskHandler struct {
	service *FactorServices
}

func (handler *checkTaskHandler) Handle(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.CheckTaskPayload)
	if !ok {
		return errors.New("Unvalid check task")
	}
	dateSet := mapset.NewSet()
	index := handler.service.indexer.GetIndex(data.DateRange)
	log.Infof("(Task %s) { %s } [ %d , %d ] Check begin.", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	syncFrom := utils.GetGlobalConfig().GetSyncFrom()
	for _, name := range data.Stores {
		store, ok := handler.service.stores[name]
		newFactor := handler.service.mapFactor(name, data.Factor)
		if !ok || name == syncFrom {
			continue
		}
		dates, err := store.Check(newFactor, data.ProcessType, index)
		if err != nil {
			log.Warningf("(Task %s) { %s } [ %d , %d ] Store[%s] check failed: %s.", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), name, err.Error())
			return err
		}
		if dates != nil {
			newSet := mapset.NewSet()
			for _, v := range dates {
				newSet.Add(v)
			}
			dateSet = dateSet.Union(newSet)
		}
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
	out := handler.service.scheduler.GetOutputChan(int(output.Type))
	out <- *output
	return nil
}

func (handler *checkTaskHandler) OnSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.CheckTaskResult)
	if !ok {
		return errors.New("invalid check result")
	}
	service := handler.service
	data := tf.Input.Payload.(task.CheckTaskPayload)
	var first, last int
	l := len(result.Datetimes)
	if l > 0 {
		first = result.Datetimes[0]
		last = result.Datetimes[l-1]
		log.Infof("(Task %s) { %s } [ %d , %d ] Check finish, missing data in (%d ... %d).", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), first, last)
		maxCalDuration := utils.GetGlobalConfig().GetMaxCalDuration()
		minCalDuration := utils.GetGlobalConfig().GetMinCalDuration()
		calStartDate := utils.GetGlobalConfig().GetCalStartDate()
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
						if calStartDate >= current.Start {
							current.Start = calStartDate
						}
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
				if calStartDate >= current.Start {
					current.Start = calStartDate
				}
				ranges = append(ranges, *current)
			}
			if data.ProcessType == task.ProcessTypeNone {
				// cal
				for _, dateRange := range ranges {
					calData := task.CalTaskPayload{Calculator: calculators.DefaultCalculator, Factor: data.Factor, DateRange: dateRange}
					calInput := task.TaskInput{Type: task.TaskTypeCal, Payload: calData}
					service.scheduler.Publish(&tf, calInput)
				}
			} else {
				// process
				syncFrom := utils.GetGlobalConfig().GetSyncFrom()
				processFrom := utils.GetGlobalConfig().GetProcessFrom()
				var name string
				if syncFrom != "" {
					name = syncFrom
				} else if processFrom != "" {
					name = processFrom
				}
				for _, dateRange := range ranges {
					var factorValue models.FactorValue
					if name != "" {
						var err error
						store, _ := service.stores[name]
						index := service.indexer.GetIndex(dateRange)
						factorValue, err = store.Fetch(service.mapFactor(name, data.Factor), models.DateRange{Start: index[0], End: index[len(index)-1]})
						if err != nil {
							return err
						}
					} else {
						factorValue.Datetime = []int{dateRange.Start, dateRange.End}
					}
					ti := task.NewProcessTaskInput(calculators.DefaultCalculator, data.Factor, factorValue, data.ProcessType)
					service.scheduler.Publish(&tf, ti)
				}
			}
		}
	} else {
		log.Infof("(Task %s) { %s } [ %d , %d ] Check finish, no missing data found.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
		if data.ProcessType == task.ProcessTypeNone {
			processType := task.ProcessTypeAll // TODO: configable
			ti := task.NewCheckTaskInput(data.Stores, data.Factor, data.DateRange, processType)
			service.scheduler.Publish(&tf, ti)
		}
	}
	return nil
}

func (handler *checkTaskHandler) OnFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.CalTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Check failed: %s", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), err)
}

func (handler *checkTaskHandler) GetTaskType() task.TaskType {
	return task.TaskTypeCheck
}

func newCheckTaskHandler(service *FactorServices) *checkTaskHandler {
	return &checkTaskHandler{service: service}
}
