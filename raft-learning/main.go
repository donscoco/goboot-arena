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

/*
学习 Raft 时，写一个小的demo，帮助自己理解这个大致流程，ps：实际的raft更完整。以下只是一个流程，并非生产环境
*/

var Node *StateInof

const (
	// 通信接口
	ElectionWork  = "Communicator.Election"
	HeartbeatWork = "Communicator.Heartbeat"
)
const (
	// 节点状态/角色
	Follower = iota
	Candidate
	Leader
)

var addr string

func init() {
	flag.StringVar(&addr, "addr", ":9090", "addr ")
}

func main() {

	flag.Parse()

	var err error

	//////  临时配置
	var ClusterMember = []string{
		"localhost:9090",
		"localhost:9091",
		"localhost:9092",
	}
	var HeartbeatTimeout = int64(3) //单位秒

	/////  初始化
	Node = new(StateInof)
	Node.ClusterMember = ClusterMember
	Node.HeartbeatTimeout = HeartbeatTimeout
	Node.Heartbeat = time.Now().Unix()
	Node.context, Node.cancel = context.WithCancel(context.TODO())
	Node.State = Follower
	Node.Version = 0
	Node.BiggestVersion = 0
	//Node.Addr = "localhost" + addr
	Node.Addr = addr

	/////  通信角色
	c := new(Communicator)
	err = rpc.Register(c)
	if err != nil {
		log.Fatalln(err)
	}

	/////  启动服务
	Node.Start()
	log.Println("启动服务")

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	Node.Stop()

}

type StateInof struct {

	// 每个进程服务要有一个状态   leader，candidate，follower
	State int
	// 每个进程服务要记录一个版本号
	Version int64
	// 每个进程服务要负责监听心跳/发送心跳
	Heartbeat        int64 // 最新的心跳时间
	HeartbeatTimeout int64

	// 要维护集群成员表
	ClusterMember []string

	BiggestVersion int64  // 节点已知的最高版本
	VoteTo         string // 可能有多个相同的最高版本，只会投给其中一个

	Listener net.Listener
	Addr     string

	context context.Context
	cancel  context.CancelFunc
	sync.Mutex
	sync.WaitGroup
}

func (s *StateInof) Start() {

	// 监听处理communicator
	go func() {
		log.Println("启动 rpc服务")
		s.Add(1)
		s.Worker1()
		s.Done()
		log.Println("退出 rpc服务")
	}()

	// 监听leader的心跳
	go func() {
		log.Println("启动 心跳检测")
		s.Add(1)
		s.Worker2()
		s.Done()
		log.Println("退出 心跳检测")
	}()
}
func (s *StateInof) Stop() {
	log.Println("退出")
	s.cancel()
	s.Listener.Close()
	log.Println("关闭等待")
	s.Wait()
	log.Println("退出成功")
}
func (s *StateInof) Worker1() (err error) {
	_, port, _ := net.SplitHostPort(s.Addr)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}
	s.Listener = listener

	var tempDelay time.Duration

	log.Println("启动 rpc服务 监听 端口 ", s.Addr)

	for {
		conn, err := listener.Accept()
		//log.Println("收到rpc请求")
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
func (s *StateInof) serve(conn net.Conn) {
	s.Add(1)
	rpc.ServeConn(conn)
	s.Done()
}
func (s *StateInof) Worker2() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			//
			log.Printf("state %+v, version %+v, BiggestVersion %+v Heartbeat %+v\n", s.State, s.Version, s.BiggestVersion, s.Heartbeat)

			switch s.State {
			case Leader:
				// 发送心跳
				s.LeaderJob()
			case Follower:
				// 检查心跳
				if (s.Heartbeat + s.HeartbeatTimeout) > time.Now().Unix() {
					continue
				}
				log.Println("心跳超时")
				s.State = Candidate
				s.Version = s.Version + 1
			case Candidate:
				// 发起选举
				if s.ElectionJob() {
					log.Println("选举成功")
					s.State = Leader
				} else {
					log.Println("选举失败")
				}
			}

		case <-s.context.Done():
			// 收到退出通知,关闭下游
			return
		}
	}
}

func (s *StateInof) ElectionJob() (isSuccess bool) {
	log.Println("开始选举")
	s.BiggestVersion = s.Version
	count := 0
	for _, addr := range s.ClusterMember {
		req := ElectionReq{
			Addr:    s.Addr,
			Version: s.BiggestVersion,
		}
		resp := ElectionReply{}
		client, err := rpc.Dial("tcp", addr) // todo timeout
		if err != nil {                      // 可能有宕机的，出错可以先不管，只考虑能收到的选票
			log.Printf("%s unreachable \n", addr)
			continue
		}
		// todo 超时
		select {
		case <-time.After(1 * time.Second):
			log.Printf("%s call timeout \n", addr)
			err = fmt.Errorf("call timeout")
			continue
		case call := <-client.Go(ElectionWork, req, &resp, nil).Done:
			err = call.Error
		}
		if err != nil {
			log.Println(err)
			continue
		}
		if resp.IsVote {
			count++
		}
		client.Close()
	}
	log.Println("获得选票", count)
	if count*2 > len(s.ClusterMember) {
		return true
	}
	return false

}

func (s *StateInof) LeaderJob() {
	for _, addr := range s.ClusterMember {
		req := HeartbeatReq{
			Addr:      s.Addr,
			Heartbeat: time.Now().Unix(),
			Version:   s.Version,
		}
		resp := HeartbeatReply{}
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil { //有节点宕机了。先不管
			log.Println(err)
			continue
		}
		client := rpc.NewClient(conn)
		select {
		case <-time.After(time.Second):
			log.Printf("%s call timeout \n", addr)
			err = fmt.Errorf("call timeout")
			continue
		case call := <-client.Go(HeartbeatWork, req, &resp, nil).Done:
			err = call.Error
		}
		if err != nil {
			log.Println(err)
			// 不管。leader 只管发心跳
			continue
		}
		client.Close()
	}
}

type HeartbeatReq struct {
	Addr      string
	Heartbeat int64
	Version   int64
}
type HeartbeatReply struct {
}
type ElectionReq struct {
	Addr    string
	Version int64
}
type ElectionReply struct {
	IsVote bool
}
type Communicator struct{}

// 接收leader的心跳
func (c *Communicator) Heartbeat(req HeartbeatReq, reply *HeartbeatReply) error {

	// 检查心跳的版本和自己的版本
	// 同步到心跳节点
	if req.Version >= Node.Version {
		Node.Heartbeat = req.Heartbeat
		Node.Version = req.Version

		if len(Node.VoteTo) > 0 {
			Node.VoteTo = ""
		}

		// leader给自己发心跳不用改
		if Node.Addr != req.Addr {
			Node.State = Follower
		}

		log.Printf("communicate 同步心跳时间 %+v\n", Node.Heartbeat)
	} else {
		//版本 小于自己的心跳不管
	}
	return nil
}

// 接收别人的选举，负责根据选举版本返回投票
func (c *Communicator) Election(req ElectionReq, reply *ElectionReply) error {
	log.Println("communicate 收到选举请求")

	if req.Version > Node.BiggestVersion {
		Node.VoteTo = req.Addr
		Node.BiggestVersion = req.Version
		reply.IsVote = true
		return nil
	} else if req.Version == Node.BiggestVersion {
		if len(Node.VoteTo) == 0 { //还没投给别人
			Node.VoteTo = req.Addr
			Node.BiggestVersion = req.Version
			reply.IsVote = true
			return nil
		} else {
			if Node.VoteTo == req.Addr {
				Node.VoteTo = req.Addr
				Node.BiggestVersion = req.Version
				reply.IsVote = true
				return nil
			}
		}
	}

	return nil
}
