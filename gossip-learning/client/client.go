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

func main() {

	client, err := rpc.Dial("tcp", "localhost:9090")
	if err != nil {
		return
	}
	req := Obj{
		Key:   "key-1",
		Value: "val-1",
	}
	reply := Obj{}

	//err = client.Call("Api.Set", req, &reply)
	err = client.Call("Api.Get", req, &reply)

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%+v", reply)
	client.Close()

}
