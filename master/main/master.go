package main

import (
	"flag"
	"fmt"
	"github.com/smartrui/crontab/master"
	"runtime"
)


var (
	confFile string
)


func initArgs() {
	var defaultFile = "/Users/liangchen/Documents/mylearn/go/src/github.com/smartrui/crontab/master/main/master.json"
	flag.StringVar(&confFile, "config", defaultFile,"指定master.json")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}


func main(){
	var (
		err  error
	)

	//初始化参数
	initArgs()

	//初始化线程
	initEnv()

	//加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	//初始化etcd
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	//初始化http服务器
	if err = master.InitServer(); err != nil {
		goto ERR
	}
	select{}
	//正常退出
	return

ERR:
	fmt.Println(err)
}