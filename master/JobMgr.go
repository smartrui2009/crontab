package master

import (
	"context"
	"encoding/json"
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

	G_JobMgr = &JobMgr{
		Client :client,
		Kv:kv,
		Lease:lease,
	}

	return
}

//保存任务
func(jobMgr *JobMgr) SaveJob(job *common.Job)(oldJob *common.Job, err error){
	var (
		jobKey string
		jobValue []byte
		putResp * clientv3.PutResponse
		oldJobObj common.Job
	)

	//任务key
	jobKey = common.JOB_SAVE_DIR + job.Name

	if jobValue, err = json.Marshal(job) ;err != nil {
		return
	}

	if putResp,err = jobMgr.Kv.Put(context.TODO(),jobKey, string(jobValue),clientv3.WithPrevKV()); err != nil {
		return
	}

	//如果能找到原先的，则反序列化一下
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

//删除任务
func(jobMgr *JobMgr) DeleteJob(name string)(oldJob *common.Job, err error){
	var (
		jobKey string
		deleteResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	jobKey = common.JOB_SAVE_DIR + name

	if deleteResp, err = jobMgr.Kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV()); err != nil {
		return
	}
	if len(deleteResp.PrevKvs) != 0{
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
		}
		oldJob = &oldJobObj
	}

	return
}

//任务列表
func(jobMgr *JobMgr) ListJob()(jobList []*common.Job, err error){
	var(
		getResp *clientv3.GetResponse
		kvPairs *mvccpb.KeyValue
		job *common.Job
	)
	if getResp, err = jobMgr.Kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix());err != nil {
		return
	}

	jobList = make([]*common.Job, 0)

	if len(getResp.Kvs) != 0 {
		for _,kvPairs = range getResp.Kvs {
			job = &common.Job{} //这个初始化要放在循环内，
			//fmt.Println(k ,string(v.Key), ":",string(v.Value))
			if err =json.Unmarshal(kvPairs.Value, &job) ;err != nil {
				err = nil
				continue
			}
			jobList = append(jobList,job)
		}
	}
	return
}

// 杀死任务
func(jobMgr *JobMgr) KillJob(name string)(err error){
	//更新一个key /cron/killer/任务名
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	killerKey = common.JOB_KILL_DIR + name

	//创建一个租约，一段时间后，让key自动删除，不要占用etcd空间。
	if leaseGrantResp, err = jobMgr.Lease.Grant(context.TODO(),1); err != nil {
		return
	}

	leaseId = leaseGrantResp.ID

	if  _, err = jobMgr.Kv.Put(context.TODO(), killerKey,"", clientv3.WithLease(leaseId)); err != nil {
		fmt.Println(err)
		return
	}

	return
}

//获取单条任务
func(jobMgr *JobMgr) GetJob(name string)(job *common.Job, err error){
	var (
		jobKey string
		getResp *clientv3.GetResponse
	)

	jobKey = common.JOB_SAVE_DIR + name

	if getResp, err = jobMgr.Kv.Get(context.TODO(), jobKey); err != nil {
		return
	}
	if len(getResp.Kvs) != 0 {
		if err = json.Unmarshal(getResp.Kvs[0].Value,&job) ;err != nil {
			return
		}
	}
	return
}