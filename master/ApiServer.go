package master

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/smartrui/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

//单例
var (
	G_ApiServer *ApiServer
)

//初始化服务
func  InitServer()(err error) {

	var (
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
	)

	//配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)
	mux.HandleFunc("/job/kill",handleJobKill)
	mux.HandleFunc("/job/detail",handleJobDetail)



	//启动tcp监听
	if listener,err = net.Listen("tcp",":" + strconv.Itoa(G_Config.ApiPort)); err != nil {
		fmt.Println(err)
		return
	}

	httpServer = &http.Server{
		ReadTimeout: time.Duration(G_Config.APiReadTimeOut) * time.Millisecond,
		WriteTimeout: time.Duration(G_Config.ApiWriteTimeOut) * time.Microsecond,
		Handler:mux,
	}

	G_ApiServer = &ApiServer{
		httpServer:httpServer,
	}

	go httpServer.Serve(listener)

	return
}

//保存任务接口
func handleJobSave(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	postJob = req.PostForm.Get("job")

	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	if oldJob, err = G_JobMgr.SaveJob(&job) ; err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return //需要return,否则会继续走到 ERR处

 ERR:
    if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}


//删除任务接口
func handleJobDelete(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		oldJob *common.Job
		bytes []byte
		name string
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.PostForm.Get("job")
 	if name == ""|| len(name) == 0 {
 		goto ERR
	}

	if oldJob, err = G_JobMgr.DeleteJob(name) ; err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}

}

//任务列表
func handleJobList(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		jobList []*common.Job
		bytes []byte
	)
	if jobList, err = G_JobMgr.ListJob(); err != nil {
		goto  ERR
	}
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//杀死任务
func handleJobKill(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		name string
		bytes []byte
	)


	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.PostForm.Get("job")
	if name == ""|| len(name) == 0 {
		err = errors.New("name不能为空") //覆盖err
		goto ERR
	}

	if err = G_JobMgr.KillJob(name); err != nil {
		goto  ERR
	}

	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		resp.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//获取单条任务
func handleJobDetail(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		job *common.Job
		bytes []byte
		name string
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.PostForm.Get("job")
	if name == ""|| len(name) == 0 {
		err = errors.New("name不能为空") //覆盖err
		goto ERR
	}

	if job, err = G_JobMgr.GetJob(name); err != nil {
		goto  ERR
	}
	if bytes, err = common.BuildResponse(0, "success", job); err == nil {
		resp.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}