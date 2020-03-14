package worker

import (
	"github.com/smartrui/crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executer struct{

}

var (

	G_executor *Executer
)

//初始化执行器
func InitExecuter() (err error){

	G_executor = &Executer{}
	return
}

func(executer *Executer) ExecuteJob(info *common.JobExecuteInfo) {
	go func(){
		var (
			result *common.JobExecuteResult
			cmd *exec.Cmd
			output []byte
			err error
			jobLock *JobLock
		)

		jobLock = G_JobMgr.CreateJobLock(info.Job.Name)

		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo:info,
			Output:make([]byte,0),
		}

		result.StartTime = time.Now()

		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		//尝试加锁
		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			 result.Err = err
			 result.EndTime = time.Now()
		}else{

			result.StartTime = time.Now()

			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)
			output, err = cmd.CombinedOutput()

			result.Output = output
			result.Err = err
			result.EndTime = time.Now()
		}

		// 任务执行完成后，把执行的结果返回给Scheduler，Scheduler会从executingTable中删除掉执行记录
		G_scheduler.PushJobResult(result)

	}()
}