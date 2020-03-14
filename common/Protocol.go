package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//定义任务
type Job struct {
	Name string `json:"name"`
	Command string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

//任务执行计划表
type JobSchedulePlan struct {
	Job *Job
	Expr *cronexpr.Expression
	NextTime time.Time
}

//任务执行状态
type JobExecuteInfo struct{
	Job *Job
	PlanTime time.Time //理论执行时间
	RealTime time.Time //真实执行时间
	CancelCtx context.Context
	CancelFunc context.CancelFunc
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

//变化事件
type JobEvent struct {
	EventType int
	Job *Job
}


//http返回接口
type Response struct {
	Error int `json:"error"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

//应答方法
func BuildResponse(error int, msg string, data interface{})(resp []byte, err error){
	var (
		response Response
	)
	response.Error = error
	response.Msg   = msg
	response.Data  = data

	resp ,err = json.Marshal(response)
	return
}

func UnpackJob(value []byte)(ret *Job, err error){
	var (
		job *Job
	)
	job = &Job{}
	if err = json.Unmarshal(value, &job); err != nil {
		return
	}
	ret = job
	return
}


// 从etcd的key中提取任务名
// /cron/jobs/job10抹掉/cron/jobs/
func ExtractJobName(jobKey string) (string) {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

func ExtractKillerName(jobKey string) (string) {
	return strings.TrimPrefix(jobKey, JOB_KILL_DIR)
}

// 任务变化事件有2种：1）更新任务 2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job: job,
	}
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	jobSchedulePlan = &JobSchedulePlan{
		Job:job,
		Expr:expr,
		NextTime:expr.Next(time.Now()),
	}

	return
}

// 构造任务执行状态
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {

	jobExecuteInfo = &JobExecuteInfo{
		 Job:  jobSchedulePlan.Job,
		 PlanTime:jobSchedulePlan.NextTime,
		 RealTime:time.Now(),
	}

	jobExecuteInfo.CancelCtx,jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())

	return
}