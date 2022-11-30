package main

import (
	"fmt"
	"net/rpc"
)

type Obj struct {
	Key     string
	Value   interface{}
	Version int64
}
type QuorumReply struct {
	Obj
	IsSuccess bool
	Err       error
}

func main() {

	fmt.Print(3 / 2)

	client, err := rpc.Dial("tcp", "localhost:9091")
	if err != nil {
		return
	}
	req := Obj{
		Key:   "key-1",
		Value: "val-1",
	}
	reply := QuorumReply{}

	//err = client.Call("Api.SetQuorumW", req, &reply)
	err = client.Call("Api.GetQuorumR", req, &reply)

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%+v", reply)
	client.Close()

}
