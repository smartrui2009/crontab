package worker

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct{
	ApiPort int `json:"apiPort"`
	APiReadTimeOut int `json:"apiReadTimeOut"`
	ApiWriteTimeOut int `json:"apiWriteTimeOut"`
	EtcdEndPoints []string `josn:"etcdEndPoints"`
}

var (
	G_Config *Config
)

func InitConfig(filename string)(err error){
	var (
		content []byte
		conf Config
	)
	if content,err  = ioutil.ReadFile(filename); err != nil {
		return
	}

	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	G_Config = &conf

	return
}