package main

import (
	"fmt"
	"os"

	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/server"
	"fxdayu.com/dyupdater/server/services"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	flag "github.com/spf13/pflag"
)

var logPath string
var accessLogPath string
var configPath string
var debug bool
var host string
var port int
var initCheck bool
var startTime, endTime int
var f1, f2 *flag.FlagSet

func init() {
	f1 = flag.NewFlagSet("dyupdater run", flag.ContinueOnError)
	f1.StringVarP(&logPath, "logfile", "l", "dyupdater.log", "Set the updater's logfile path.")
	f1.StringVarP(&accessLogPath, "accessLogFile", "L", "gin.log", "Set the dashbord's access logfile path.")
	f1.StringVarP(&configPath, "config", "c", "", "Using the given config file.")
	f1.BoolVarP(&debug, "debug", "d", false, "Run at debug mode.")
	f1.StringVarP(&host, "host", "H", "127.0.0.1", "Dashboard's host.")
	f1.IntVarP(&port, "port", "p", 19328, "Dashboard's port.")
	f1.BoolVarP(&initCheck, "check", "C", false, "Whether to do a initial checking after run.")
	f1.SortFlags = false
	f2 = flag.NewFlagSet("dyupdater check", flag.ContinueOnError)
	f2.StringVarP(&logPath, "logfile", "l", "./dyupdater.log", "Set the logfile path.")
	f2.StringVarP(&configPath, "config", "c", "", "Using the given config file.")
	f2.BoolVarP(&debug, "debug", "d", false, "Run at debug mode.")
	f2.IntVarP(&startTime, "start", "s", 0, "The check's start date in format \"20060102\", default will be cal-start-date in configation.")
	f2.IntVarP(&endTime, "end", "e", 0, "The check's end date in format \"20060102\", 0 means up to today.")
	f2.SortFlags = false
}

func configLog() {
	logFile, _ := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModeType)
	utils.ConfigLog(logFile, debug)
}

func main() {
	n := len(os.Args)
	if n > 1 {
		switch os.Args[1] {
		case "run":
			if err := f1.Parse(os.Args[2:]); err == nil {
				configLog()
				accessLogFile, err := os.OpenFile(accessLogPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModeType)
				if err != nil {
					panic(err)
				}
				addrs := []string{fmt.Sprintf("%s:%d", host, port)}
				fs := services.NewFactorServiceFromConfig(configPath)
				archive := utils.GetTaskArchive()
				scheduler := fs.GetScheduler()
				archiveHandler := func(tf *task.TaskFuture) error {
					tr := utils.NewTaskArchiveRecord(tf)
					archive.Set(tr)
					return nil
				}
				taskTypes := []int{int(task.TaskTypeCheck), int(task.TaskTypeCal), int(task.TaskTypeUpdate)}
				for _, t := range taskTypes {
					scheduler.PrependHandler(t, archiveHandler)
					scheduler.AppendPostSuccessHandler(t, func(tf task.TaskFuture, r task.TaskResult) {
						archiveHandler(&tf)
					})
					scheduler.AppendAbortedHandler(t, func(tf task.TaskFuture) {
						archiveHandler(&tf)
					})
				}
				cs := services.NewCronService(fs)
				if startTime == 0 {
					startTime = utils.GetGlobalConfig().GetCalStartDate()
				}
				if initCheck {
					go fs.CheckAll(models.DateRange{Start: startTime, End: endTime})
				}
				cs.Run()
				server.Serve(debug, accessLogFile, fs, addrs...)
			}
			return
		case "check":
			if err := f2.Parse(os.Args[2:]); err == nil {
				configLog()
				fs := services.NewFactorServiceFromConfig(configPath)
				fs.CheckAll(models.DateRange{Start: startTime, End: endTime})
				fs.Wait(0)
			}
			return
		}
	}
	fmt.Println(`Usage of dyupdater:
  run       Run the dyupdater with web UI, run tasks which is in cron settings or sending by web UI.
  check     Run checking and updating for once without web UI.`)
}
