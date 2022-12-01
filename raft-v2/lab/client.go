package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func main() {

	//doSet("9090") // 集群测试
	//doGet("9092") // 集群测试

	doSet("9093") // 单节点加入后测试

}

type ItemReq struct {
	Key   string
	Val   interface{}
	Level int // 同步级别  0-异步;1-半同步;2-同步
	Node  int // 要同步的节点数
}
type ItemReply struct {
	IsSuccess bool

	Val interface{}
	Err error
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
		Level: 0,
		Node:  0,
	}
	reply := ItemReply{}
	err = client.Call("ServerApi.Get", req, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(reply)
}
func doSet(port string) {
	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		log.Println(err)
	}
	//defer client.Close()
	req := ItemReq{
		Key:   "key-5",
		Val:   "val-5",
		Level: 0,
		Node:  0,
	}
	reply := ItemReply{}
	err = client.Call("ServerApi.Set", req, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(reply)

}
