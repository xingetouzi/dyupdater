package server

import (
	"io"
	"io/ioutil"
	"os"

	"fxdayu.com/dyupdater/server/services"
	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

func handleConfig(c *gin.Context) {
	viper := viper.GetViper()
	file := viper.ConfigFileUsed()
	content, err := ioutil.ReadFile(file)
	if err != nil {
		c.JSON(500, gin.H{"message": err.Error()})
	}
	c.JSON(200, gin.H{"data": content})
}

func handleCurrentUser(c *gin.Context) {
	c.JSON(200, gin.H{})
}

func Serve(debug bool, logfile *os.File, fs *services.FactorServices, addr ...string) {
	if logfile != nil {
		gin.DisableConsoleColor()
		gin.DefaultWriter = io.MultiWriter(logfile)
	}
	if !debug {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()
	r.Use(static.Serve("/", BinaryFileSystem("")))
	r.GET("/api/tasks", handleTasks(fs))
	r.GET("/api/config", handleConfig)
	r.GET("/api/currentUser", handleCurrentUser)
	r.Run(addr...) // listen and serve on 127.0.0.1:8080
}
