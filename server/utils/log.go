package utils

import (
	"os"

	logging "github.com/op/go-logging"
)

var AppLogger = logging.MustGetLogger("dyupdater")

func ConfigLog(logfile *os.File, debug bool) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	var format logging.Formatter
	if debug {
		format = logging.MustStringFormatter(
			`%{time:2006/01/02 15:04:05.000} [%{level:.4s}] %{longfile}:%{shortfunc} | %{message}`,
		)
	} else {
		format = logging.MustStringFormatter(
			`%{time:2006/01/02 15:04:05.000} [%{level:.4s}] %{shortfile}:%{shortfunc} | %{message}`,
		)
	}
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	if logfile != nil {
		backendF := logging.NewLogBackend(logfile, "", 0)
		backendFormatterF := logging.NewBackendFormatter(backendF, format)
		backendLeveledF := logging.AddModuleLevel(backendFormatterF)
		backendLeveledF.SetLevel(logging.DEBUG, "")
		AppLogger.SetBackend(logging.MultiLogger(backendLeveled, backendLeveledF))
	} else {
		AppLogger.SetBackend(backendLeveled)
	}
}
