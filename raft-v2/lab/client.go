package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func main() {

	doSet()
	//doGet()

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

func doGet() {
	client, err := rpc.Dial("tcp", "localhost:9092")
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
func doSet() {
	client, err := rpc.Dial("tcp", "localhost:9091")
	if err != nil {
		log.Println(err)
	}
	//defer client.Close()
	req := ItemReq{
		Key:   "key-4",
		Val:   "val-4",
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
