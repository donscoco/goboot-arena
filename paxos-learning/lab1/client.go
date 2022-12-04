package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func main() {

	//doSet("9090", "key-5", "key-5") // 集群测试
	//doSet("9090", "key-5", "key-6") // 集群测试
	doSet("9090", "key-5", "key-7") // 集群测试
	//doGet("9092") // 集群测试

	//doSet("9093") // 单节点加入后测试

}

type ItemReq struct {
	Key      string
	Proposal int64       // 提案编号
	Value    interface{} // 在prepare请求中为nil,共用下请求体
}
type ItemReply struct {
	IsSuccess bool
	Err       error

	// [k,n,v]
	Key      string
	Proposal int64
	Value    interface{}
}

func doGet(port string) {
	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		log.Println(err)
	}
	//defer client.Close()
	req := ItemReq{
		Key: "key-1",
		//Val:   "val-1",
	}
	reply := ItemReply{}
	err = client.Call("ServerApi.Get", req, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(reply)
}
func doSet(port string, k, v string) {
	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		log.Println(err)
	}
	//defer client.Close()
	req := ItemReq{
		Key:   k,
		Value: v,
	}
	reply := ItemReply{}
	err = client.Call("ServerApi.Set", req, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(reply)

}
