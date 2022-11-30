package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

const (
	Rumor1 = "Api.Rumor1"
	Rumor2 = "Api.Rumor2"

	Set = "Api.Set"
	Get = "Api.Get"
)

var Node *ServerNode

var addr string

func init() {
	flag.StringVar(&addr, "addr", "localhost:9092", "addr ")
}

func main() {

	flag.Parse()

	//////  临时配置
	var ClusterMember = []string{
		"localhost:9090",
		"localhost:9091",
		"localhost:9092",
	}

	/////  初始化
	Node = new(ServerNode)
	Node.ClusterMember = ClusterMember
	Node.context, Node.cancel = context.WithCancel(context.TODO())
	Node.Data = make(map[string]Obj, 16)
	Node.Addr = addr

	/////  启动服务
	Node.Start()
	log.Println("启动服务")

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	Node.Stop()
}

type Obj struct {
	Key     string
	Value   interface{}
	Version int64
}

func (o *Obj) String() string {
	return o.Value.(string)
}

type ServerNode struct {
	Addr          string
	ClusterMember []string

	Data map[string]Obj

	Listener net.Listener

	context context.Context
	cancel  context.CancelFunc

	sync.WaitGroup
	sync.Mutex
}

func Checksum(arr []string) string {
	md5Ctx := md5.New()
	sort.Strings(arr)
	for _, s := range arr {
		md5Ctx.Write([]byte(s))
	}
	cipherStr := md5Ctx.Sum(nil)
	return hex.EncodeToString(cipherStr)
}
func (s *ServerNode) AntiEntropy() {

	arr := make([]string, 0, len(s.Data))
	s.Lock()
	for k, v := range s.Data {
		arr = append(arr, k+v.String())
	}
	s.Unlock()
	checksum := Checksum(arr)

	// 遍历自己要Rumor传播的节点（目前先源节点传播。传给所有节点）
	for _, addr := range s.ClusterMember {

		if addr == s.Addr { // 不用发给自己
			continue
		}

		client, err := rpc.Dial("tcp", addr) // todo timeout
		if err != nil {                      // 可能有宕机的
			log.Printf("%s unreachable \n", addr)
			continue
		}

		req := RumorReq{
			//Addr:    s.Addr,
			Checksum: checksum,
		}
		resp := RumorReply{}

		select {
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("timeout")
			client.Close()
			continue
		case call := <-client.Go(Rumor1, req, &resp, nil).Done:
			err = call.Error
		}

		if err != nil {
			client.Close()
			log.Println(err)
			continue
		}

		if !resp.IsChanged {
			//log.Println("反熵：无变化")
			client.Close()
			continue
		}

		// 改变了，需要同步
		req.Data = s.Data
		select {
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("timeout")
			client.Close()
			continue
		case call := <-client.Go(Rumor2, req, &resp, nil).Done:
			err = call.Error
		}
		if err != nil {
			client.Close()
			log.Println(err)
			continue
		}
		log.Printf("反熵 更新:%+v\n", resp)
		s.Data = resp.Data
		client.Close()
	}
}
func (s *ServerNode) Start() {

	// rpc 接口
	go func() {
		log.Println("启动 rpc 服务")
		s.Add(1)
		s.Worker1()
		s.Done()
		log.Println("关闭 rpc 服务")
	}()
	// 定期反熵
	go func() {
		log.Println("启动 反熵 服务")
		s.Add(1)
		s.Worker2()
		s.Done()
		log.Println("关闭 反熵 服务")
	}()
}
func (s *ServerNode) Stop() {

	log.Println("退出")
	s.cancel()

	s.Listener.Close()

	s.Wait()
	log.Println("安全退出")

}
func (s *ServerNode) Worker2() {

	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-s.context.Done():
			// 关闭下游
			return
		case <-ticker.C:
			//log.Println("反熵开始")
			fmt.Printf("debug %+v\n", Node.Data)
			//s.AntiEntropy() // 测试quorumNWR,禁止反熵,节点之间不同步数据,观察GetquorumR情况
		}
	}
}
func (s *ServerNode) Worker1() error {

	/////  通信角色
	c := new(Api)
	err := rpc.Register(c)
	if err != nil {
		return err
	}

	_, port, _ := net.SplitHostPort(s.Addr)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}
	s.Listener = listener

	var tempDelay time.Duration

	log.Println("启动 rpc服务 监听 端口 ", s.Addr)

	for {
		conn, err := s.Listener.Accept()
		// 直接仿照 net/http 包的异常处理
		if err != nil {
			select {
			case <-s.context.Done():
				return err
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		go s.serve(conn)
	}
}
func (s *ServerNode) serve(conn net.Conn) {
	s.Add(1)
	rpc.ServeConn(conn)
	s.Done()
}

type Api struct {
}

type RumorReq struct {
	Checksum string
	Data     map[string]Obj
}
type RumorReply struct {
	IsChanged bool
	Data      map[string]Obj // todo 改成需要处理的行为，减少通讯成本
}

func (c *Api) Rumor1(req RumorReq, reply *RumorReply) error {
	// 检查 校验和 (Checksum)等机制
	checksumR := req.Checksum

	arr2 := make([]string, 0, len(Node.Data))
	Node.Lock()
	for k, v := range Node.Data {
		arr2 = append(arr2, k+v.String())
	}
	Node.Unlock()
	checksumN := Checksum(arr2)
	if checksumN != checksumR {
		reply.IsChanged = true
	}
	return nil
}
func (c *Api) Rumor2(req RumorReq, reply *RumorReply) error {
	log.Println("Rumor2")
	Node.Lock()
	defer Node.Unlock()
	// 同步req中的数据和本服务中的数据
	for k, rv := range req.Data {
		nv, ok := Node.Data[k]
		if !ok {
			Node.Data[k] = rv
			continue
		}
		if rv.Version > nv.Version {
			Node.Data[k] = rv
		} else {
			Node.Data[k] = nv
		}
	}
	reply.Data = Node.Data
	return nil
}

// 异步 set
func (c *Api) Set(req Obj, reply *Obj) error {
	Node.Lock()
	defer Node.Unlock()
	Node.Data[req.Key] = Obj{Value: req.Value, Version: time.Now().UnixNano()}
	return nil
}
func (c *Api) Get(req Obj, reply *Obj) error {
	Node.Lock()
	defer Node.Unlock()
	obj, ok := Node.Data[req.Key]
	if !ok {
		return nil
	}
	reply.Key = obj.Key
	reply.Value = obj.Value
	reply.Version = obj.Version

	return nil
}

//	func (c *Api) Del() error {
//		return nil
//	}
func (c *Api) NewMember() error {
	// todo
	return nil
}

type QuorumReply struct {
	Obj
	IsSuccess bool
	Err       error
}

func (c *Api) SetQuorumW(req Obj, reply *QuorumReply) error {
	Node.Lock()
	defer Node.Unlock()

	// todo 校验

	nodeNum := len(Node.ClusterMember)
	req.Version = time.Now().UnixNano()
	successNum := 0

	for _, addr := range Node.ClusterMember {

		if addr == Node.Addr { // 不用发给自己
			successNum++
			Node.Data[req.Key] = req
			continue
		}

		client, err := rpc.Dial("tcp", addr) // todo timeout
		if err != nil {                      // 可能有宕机的
			log.Printf("%s unreachable \n", addr)
			continue
		}

		resp := Obj{}

		select {
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("timeout")
			client.Close()
			continue
		case call := <-client.Go("Api.SetForQuorum", req, &resp, nil).Done:
			err = call.Error
		}

		if err != nil {
			client.Close()
			log.Println(err)
			continue
		}

		successNum++
		if successNum > nodeNum/2 {
			log.Println("设置成功", successNum)
			client.Close()
			break
		}

		client.Close()
	}

	if successNum > nodeNum/2 {
		reply.IsSuccess = true
	}

	return nil
}

func (c *Api) GetQuorumR(req Obj, reply *QuorumReply) error {
	Node.Lock()
	defer Node.Unlock()

	//todo 校验

	nodeNum := len(Node.ClusterMember)
	req.Version = time.Now().UnixNano()
	successObj := make([]*Obj, 0, nodeNum)
	successNum := 0

	for _, addr := range Node.ClusterMember {

		if addr == Node.Addr {
			obj, _ := Node.Data[req.Key]

			successNum++
			successObj = append(successObj, &obj)

			continue
		}

		client, err := rpc.Dial("tcp", addr) // todo timeout
		if err != nil {                      // 可能有宕机的
			log.Printf("%s unreachable \n", addr)
			continue
		}

		resp := Obj{}

		select {
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("timeout")
			client.Close()
			continue
		case call := <-client.Go(Get, req, &resp, nil).Done:
			err = call.Error
		}

		if err != nil {
			client.Close()
			log.Println(err)
			continue
		}

		successNum++
		successObj = append(successObj, &resp)
		if successNum > nodeNum/2 {
			client.Close()
			break
		}

		client.Close()
	}

	if successNum > nodeNum/2 {
		reply.IsSuccess = true

		target := &Obj{}
		for _, o := range successObj {
			if o == nil {
				continue
			}
			if o.Version > target.Version {
				target = o
			}
		}

		reply.Key = target.Key
		reply.Value = target.Value
		reply.Version = target.Version
	}

	return nil

}

// 用于测试quorum，使用quorum发来的版本号
func (c *Api) SetForQuorum(req Obj, reply *Obj) error {
	Node.Lock()
	defer Node.Unlock()
	Node.Data[req.Key] = Obj{Value: req.Value, Version: req.Version}
	return nil
}
