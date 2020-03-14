package common

const (

	JOB_SAVE_DIR =  "/cron/jobs/"  //任务存储到etcd的key前缀

	JOB_KILL_DIR =  "/cron/killer/"  //杀死任务存储到etcd的key前缀

	// 任务锁目录
	JOB_LOCK_DIR = "/cron/lock/"


	// 保存任务事件
	JOB_EVENT_SAVE   = 1

	// 删除任务事件
	JOB_EVENT_DELETE = 2

	// 强杀任务事件
	JOB_EVENT_KILL   = 3

)
