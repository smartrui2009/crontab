package worker

import (
	"context"
	"fmt"
	"github.com/smartrui/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobMgr struct {
	Client *clientv3.Client
	Kv clientv3.KV
	Lease clientv3.Lease
	Watcher clientv3.Watcher
}

var (
	G_JobMgr *JobMgr
)



//初始化任务管理器
func InitJobMgr()(err error) {

	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)
	config = clientv3.Config{
		Endpoints:G_Config.EtcdEndPoints,
		DialTimeout:10 * time.Second,
	}

	if client,err = clientv3.New(config); err != nil {
		fmt.Println(err)
		return
	}

	kv    = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_JobMgr = &JobMgr{
		Client :client,
		Kv:kv,
		Lease:lease,
		Watcher:watcher,
	}

	//启动监听任务
	G_JobMgr.watchJobs()

	// 启动监听killer
	G_JobMgr.watchKiller()

	return
}

func(jobMgr *JobMgr) watchJobs()(err error){
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		jobEvent *common.JobEvent
		jobName string
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResponse clientv3.WatchResponse
		watchEvent *clientv3.Event
	)

	if getResp, err = jobMgr.Kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		if job, err  = common.UnpackJob(kvpair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	go func(){
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = jobMgr.Watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix(),clientv3.WithRev(watchStartRevision))

		for watchResponse = range watchChan {
			 for _, watchEvent = range watchResponse.Events {
			 	switch watchEvent.Type{
				case mvccpb.PUT:
					  if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
							continue
					  }

					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name:jobName,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
					G_scheduler.PushJobEvent(jobEvent)
				}


			 }
		}
	}()


	return
}


func(jobMgr *JobMgr) watchKiller()(err error){

	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
		job *common.Job
	)

	// 监听/cron/killer目录
	go func() { // 监听协程
		// 监听/cron/killer/目录的变化
		watchChan = jobMgr.Watcher.Watch(context.TODO(), common.JOB_KILL_DIR, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死任务事件
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// 事件推给scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期, 被自动删除
				}
			}
		}
	}()

	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock){
	jobLock = InitJobLock(jobName, jobMgr. Kv, jobMgr.Lease)
	return
}