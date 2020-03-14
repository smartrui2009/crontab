package main

import (
	"flag"
	"fmt"
	"github.com/smartrui/crontab/worker"
	"runtime"
)


var (
	confFile string
)


func initArgs() {
	var defaultFile = "/Users/liangchen/Documents/mylearn/go/src/github.com/smartrui/crontab/worker/main/worker.json"
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

	fmt.Println("开始加载配置")
	//加载配置
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	fmt.Println("开始初始化调度器")
	//初始化调度器
	if err = worker.InitSchedule(); err != nil {
		goto ERR
	}

	fmt.Println("开始初始化执行器")
	//初始化调度器
	if err = worker.InitExecuter(); err != nil {
		goto ERR
	}

	fmt.Println("开始初始化etcd")
	//初始化etcd
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}


	fmt.Println("结束所有初始化工作，运行了")
	select{}
	//正常退出
	return

ERR:
	fmt.Println(err)
}