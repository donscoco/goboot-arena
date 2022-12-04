package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var addrP string

func init() {
	flag.StringVar(&addrP, "addr", "localhost:9090", "input addr")
}

var srv Server

func main() {
	flag.Parse()

	//var err error
	//////  临时配置
	var ClusterMember = []string{
		"localhost:9090",
		"localhost:9091",
		"localhost:9092",
	}

	srv.Events = make(map[string]interface{}) // todo 检查读写
	srv.Addr = addrP
	srv.ClusterMember = ClusterMember
	srv.Proposal = 1

	srv.context, srv.cancel = context.WithCancel(context.TODO())

	srv.Start()
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Stop()
}

const (
	ApiPrepare = "ServerApi.Prepare"
	ApiAccept  = "ServerApi.Accept"
	ApiSync    = "ServerApi.Sync"
	ApiLearn   = "ServerApi.Learn"
)

type Server struct {
	ClusterMember []string
	Addr          string
	Events        map[string]interface{} // 确定后不可改变的 key value

	Proposal int64 //每个节点已知的最大提案编号。

	Api      *ServerApi
	Listener net.Listener

	context context.Context
	cancel  context.CancelFunc

	sync.WaitGroup
	sync.Mutex
}

func (s *Server) Start() error {
	// 启动api
	go func() {
		log.Println("启动rpc服务")
		s.Add(1)
		s.RpcServer()
		s.Done()
		log.Println("关闭rpc服务")
	}()

	//for test
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			<-ticker.C
			s.Lock()
			log.Printf("events:%+v \n", s.Events)
			log.Printf("sever:%+v \n", s.Proposal)
			s.Unlock()
		}
	}()
	return nil
}
func (s *Server) Stop() error {
	s.cancel()
	s.Listener.Close()
	s.Wait()
	log.Println("安全退出")
	return nil
}
func (s *Server) RpcServer() (err error) {
	// rpc服务
	api := new(ServerApi)
	api.server = s
	s.Api = api

	err = rpc.Register(api)
	if err != nil {
		return
	}

	_, port, _ := net.SplitHostPort(s.Addr)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return
	}

	log.Println("listen on", s.Addr)
	s.Listener = listener
	var tempDelay time.Duration

	for {
		conn, err := s.Listener.Accept()
		// 直接仿照 net/http 包的异常处理
		if err != nil {
			select {
			case <-s.context.Done():
				//log.Println(err)
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
			//log.Println(err)
			return err
		}

		go s.serve(conn)
	}

	return nil

}
func (s *Server) serve(conn net.Conn) {
	s.Add(1)
	rpc.ServeConn(conn)
	s.Done()
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

/*
发起提议
接收提议
接收learner的同步信息
接收proposser的提议通过信息（收到信息的节点成为learner），然后通知同步各个acceptor
*/
type ServerApi struct {
	server *Server
}

// acceptor Job：接收提议 （准备阶段）
func (s *ServerApi) Prepare(req ItemReq, reply *ItemReply) (err error) {
	log.Printf("调用 Prepare %+v", req)
	// todo 检查
	server := s.server
	server.Lock()
	defer server.Unlock()

	if req.Proposal <= server.Proposal {
		log.Println("提案编号过小不处理")
		reply.IsSuccess = false
		reply.Err = fmt.Errorf("提案编号过小不处理")
		// todo 是否需要把 最大的proposal同步回去？避免让proposer做无用功
		reply.Proposal = server.Proposal
		return
	}

	val := server.Events[req.Key]
	// todo 是否要对存在与否进行判断，还是不管

	//[n,v]
	reply.IsSuccess = true
	reply.Proposal = server.Proposal
	reply.Value = val

	server.Proposal = req.Proposal // 节点 承诺处理的提案编号，只有更大的编号过来才能让节点改变
	return
}

// acceptor Job：接收提交 （提交阶段）
func (s *ServerApi) Accept(req ItemReq, reply *ItemReply) (err error) {
	log.Printf("调用 Accept %+v", req)
	// todo 检查
	server := s.server
	server.Lock()
	defer server.Unlock()

	if req.Proposal != server.Proposal {
		log.Println("提案编号不等于提议阶段的编号,需要先进行提议")
		reply.IsSuccess = false
		reply.Err = fmt.Errorf("提案编号不等于提议阶段的编号")
		return
	}

	server.Events[req.Key] = req.Value
	reply.IsSuccess = true
	//reply.Key = req.Key
	//reply.Proposal = req.Proposal
	//reply.Value = req.Value

	return
}

// acceptor Job ：接收leaner的同步信息（learner的信息是代表集群议案通过的。需要无条件接收）
func (s *ServerApi) Sync(req ItemReq, reply *ItemReply) (err error) {
	log.Println("调用Sync")
	// todo 检查
	server := s.server

	if req.Proposal < server.Proposal {
		return
	}
	server.Lock()
	defer server.Unlock()

	server.Events[req.Key] = req.Value
	reply.IsSuccess = true
	return
}

// learner Job ：被proposer指定为learner了。负责同步提案信息给所有节点。
func (s *ServerApi) Learn(req ItemReq, reply *ItemReply) (err error) {
	log.Println("调用Learn")
	// todo 检查
	server := s.server

	var wg sync.WaitGroup
	for i, addr := range server.ClusterMember {
		if addr == s.server.Addr {
			s.Sync(ItemReq{
				Key:      req.Key,
				Value:    req.Value,
				Proposal: req.Proposal,
			}, &ItemReply{})
			continue
		}
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			var e error
			client, e := rpc.Dial("tcp", addr)
			if e != nil {
				log.Println(e)
				return
			}
			req := ItemReq{
				Key:      req.Key,
				Value:    req.Value,
				Proposal: req.Proposal,
			}
			reply := &ItemReply{}
			select {
			case <-time.After(1 * time.Second):
				e = fmt.Errorf("call timeout")
			case call := <-client.Go(ApiSync, req, reply, nil).Done:
				e = call.Error
			}
			if e != nil {
				client.Close()
			}
			client.Close()
			// 不管同步成不成功，都直接不处理了
		}(i, addr)
	}
	wg.Wait()
	return
}

func (s *ServerApi) Get(req ItemReq, reply *ItemReply) (err error) {
	log.Println("调用GET")
	// todo 检查
	server := s.server
	server.Lock()
	defer server.Unlock()

	val, ok := server.Events[req.Key]
	if !ok {
		err = fmt.Errorf("empty key")
		return
	}
	reply.IsSuccess = true
	reply.Key = req.Key
	reply.Proposal = server.Proposal
	reply.Value = val
	return
}

// 改变节点状态 proposer
// 发起提议
// 根据提议情况
// 进行提交
// 根据提交情况
// 提交成功后挑选一个learner节点
func (s *ServerApi) Set(req ItemReq, reply *ItemReply) (errs error) {
	log.Println("调用SET")
	// todo 检查
	server := s.server
	//log.Println(req)

	server.Lock()
	server.Proposal = server.Proposal + 1
	req.Proposal = server.Proposal
	server.Unlock()

	// 发起提议
	log.Println("发起提议准备")
	prepareReplys := make([]*ItemReply, len(server.ClusterMember))
	var wg sync.WaitGroup
	for i, addr := range server.ClusterMember {
		if addr == s.server.Addr {
			// todo
			continue
		}
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			var e error
			client, e := rpc.Dial("tcp", addr)
			if e != nil {
				log.Println(e)
				return
			}
			proposeReq := ItemReq{
				Key:      req.Key,
				Value:    req.Value,
				Proposal: req.Proposal,
			}
			proposeReply := &ItemReply{}
			select {
			case <-time.After(1 * time.Second):
				e = fmt.Errorf("call timeout")
			case call := <-client.Go(ApiPrepare, proposeReq, proposeReply, nil).Done:
				e = call.Error
			}
			if e != nil {
				log.Printf("err :%+v", e)
				client.Close()
			}
			prepareReplys[i] = proposeReply
			client.Close()
		}(i, addr)
	}
	wg.Wait()

	// 判断
	var proposal int64
	var value interface{} = nil
	acceptCount := 0
	for _, r := range prepareReplys {
		if r == nil {
			continue
		}
		if r.IsSuccess {
			acceptCount++
		}
		if r.Value != nil && r.Proposal > proposal { // 使用响应中最大的提案编号的值
			proposal = r.Proposal
			value = r.Value
		}
	}
	if acceptCount <= len(server.ClusterMember)/2 {
		//reply.
		reply.Err = fmt.Errorf("提议未经半数节点以上通过")
		return
	}
	if value == nil { //说明上面所有节点都没有值,使用自己的值
		//proposal = req.Proposal
		value = req.Value
	}

	// 发起提交
	log.Println("发起提交")
	acceptReplys := make([]*ItemReply, len(server.ClusterMember))

	for i, addr := range server.ClusterMember {
		if addr == s.server.Addr {
			// todo
			continue
		}
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			var e error
			client, e := rpc.Dial("tcp", addr)
			if e != nil {
				log.Println(e)
				return
			}
			req := ItemReq{
				Key:      req.Key,
				Proposal: req.Proposal,
				Value:    value,
			}
			reply := &ItemReply{}
			select {
			case <-time.After(1 * time.Second):
				e = fmt.Errorf("call timeout")
			case call := <-client.Go(ApiAccept, req, reply, nil).Done:
				e = call.Error
			}
			if e != nil {
				client.Close()
			}
			acceptReplys[i] = reply
			client.Close()
		}(i, addr)
	}
	wg.Wait()

	// 判断，超过一半提交成功就可以确认同步给集群
	successCount := 0
	for _, r := range acceptReplys {
		if r == nil {
			continue
		}
		if r.IsSuccess {
			successCount++
		}
	}

	if successCount <= len(server.ClusterMember)/2 {
		//reply.
		reply.Err = fmt.Errorf("提交未经半数节点以上通过")
		return
	}

	server.Lock()
	server.Events[req.Key] = value
	server.Unlock()

	reply.IsSuccess = true
	reply.Key = req.Key
	reply.Proposal = server.Proposal
	reply.Value = value

	//return
	// 选一个learner去同步集群,这里直接先用自己的leaner
	//server.ClusterMember[]
	log.Println("指定节点去学习")
	client, err := rpc.Dial("tcp", server.Addr)
	if err != nil {
		reply.Err = err
	}
	learnReq := ItemReq{
		Key:   req.Key,
		Value: value,
		//Version: 0,
	}
	learnReply := &ItemReply{}
	select {
	case <-time.After(1 * time.Second):
		err = fmt.Errorf("call timeout")
	case call := <-client.Go(ApiLearn, learnReq, learnReply, nil).Done:
		err = call.Error
	}
	if err != nil {
		log.Println(err)
		client.Close()
		return
	}
	client.Close()

	return
}
