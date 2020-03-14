package worker

import (
	"fmt"
	"github.com/smartrui/crontab/common"
	"time"
)

type Schedule struct {
	jobEventChan chan *common.JobEvent
	jobPlanTable map[string]*common.JobSchedulePlan
	jobExecutingTable map[string]*common.JobExecuteInfo
	jobResultChan chan *common.JobExecuteResult
}


var (
	G_scheduler  *Schedule
)


//初始化调度器
func InitSchedule()(err error){
	G_scheduler = &Schedule{
		jobEventChan: make(chan *common.JobEvent,100),
		jobPlanTable: make(map[string]*common.JobSchedulePlan,1000),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo,1000),
		jobResultChan:make(chan *common.JobExecuteResult,100),
	}



	go G_scheduler.ScheduleLoop()

	return
}

//启动调度协程
func(schedule *Schedule) ScheduleLoop() {
	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	scheduleAfter = schedule.TrySchedule()

	scheduleTimer = time.NewTimer(scheduleAfter)

	for {

		select{
		case jobEvent = <-schedule.jobEventChan:
		    schedule.handleJobEvent(jobEvent)
		case <-scheduleTimer.C:  // 最近的任务到期了
		case jobResult = <-schedule.jobResultChan:
			schedule.handleJobResult(jobResult)
		}

		scheduleAfter = schedule.TrySchedule()


		scheduleTimer.Reset(scheduleAfter)
	}


}



// 处理任务事件
func (scheduler *Schedule) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		err error
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo *common.JobExecuteInfo
		jobExisted bool
		jobExecuting bool
	)

	switch jobEvent.EventType{
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
		if jobSchedulePlan,jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted {
			delete(scheduler.jobPlanTable,jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name];jobExecuting {
			jobExecuteInfo.CancelFunc()
		}
	}
}


// 处理任务结果
func (scheduler *Schedule) handleJobResult(result *common.JobExecuteResult) {

	//删除执行状态
	delete(scheduler.jobExecutingTable,result.ExecuteInfo.Job.Name)

	//todo,写入日志到mongodb或其它


	fmt.Println("任务执行完成:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

// 重新计算任务调度状态
func (scheduler *Schedule) TrySchedule() (scheduleAfter time.Duration) {

	var (
		now time.Time
		nearTime *time.Time
		jobPlan *common.JobSchedulePlan
	)
	//如果任务列表为空,修眠1秒
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	now = time.Now()

	for _,jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//执行任务
			scheduler.TryStartJob(jobPlan)

			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}

		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}

	// 下次调度间隔（最近要执行的任务调度时间 - 当前时间）

	scheduleAfter = (*nearTime).Sub(now)

	return
}


// 尝试执行任务
func (scheduler *Schedule) TryStartJob(jobSchedulePlan *common.JobSchedulePlan) {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobSchedulePlan.Job.Name];jobExecuting {
		return
	}

	jobExecuteInfo = common.BuildJobExecuteInfo(jobSchedulePlan)

	//保存执行状态
	scheduler.jobExecutingTable[jobSchedulePlan.Job.Name] = jobExecuteInfo

	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)

	G_executor.ExecuteJob(jobExecuteInfo)
}

//推送任务事件
func (scheduler *Schedule) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 回传任务执行结果
func (scheduler *Schedule) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}