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
	"strings"
	"sync"
	"syscall"
	"time"
)

/*
 */
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
	var HeartbeatTimeout = int64(3) //单位秒

	srv.Data = make(map[string]interface{})
	srv.Term = 0
	srv.State = Follower
	srv.Addr = addrP
	srv.ClusterMember = ClusterMember
	srv.HeartbeatTimeout = HeartbeatTimeout
	srv.LogFilePath = "/tmp/log-" + addrP
	srv.context, srv.cancel = context.WithCancel(context.TODO())

	srv.Start()
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Stop()

}

const (
	Follower = iota
	Candidate
	Leader
	Nobody // Candidate 日志落后，竞选失败 啥也不是，等待leader 发心跳过来，转成follower
)
const (
	ApiGet = "ServerApi.Get"
	ApiSet = "ServerApi.Set"
)
const (
	CommunicateHeartbeat = "ServerApi.Heartbeat"
	CommunicateElection  = "ServerApi.Election"
	CommunicateSyncLog   = "ServerApi.SyncLog"
)

/*
server
*/
type Server struct {
	Api    *ServerApi
	Logger *ServerLog

	ClusterMember []string
	Addr          string
	State         int // 角色

	Leader           string
	Heartbeat        int64
	HeartbeatTimeout int64
	Term             int64

	VoteTerm int64 // 投票给的那个任期；本节点已知的最大的任期
	VoteTo   string

	Data map[string]interface{}

	Listener    net.Listener
	LogFile     *os.File
	LogFilePath string

	context context.Context
	cancel  context.CancelFunc

	sync.WaitGroup
	sync.Mutex
}

func (s *Server) Start() error {

	// 启动日志
	//go func() {
	log.Println("启动日志服务")
	//	s.Add(1)
	s.LogServer()
	//	s.Done()
	//	log.Println("关闭日志服务")
	//}()

	// 启动api
	go func() {
		log.Println("启动rpc服务")
		s.Add(1)
		s.RpcServer()
		s.Done()
		log.Println("关闭rpc服务")
	}()

	// 启动角色工作，心跳，选举
	go func() {
		log.Println("启动角色服务")
		s.Add(1)
		s.Work()
		s.Done()
		log.Println("关闭角色服务")
	}()

	return nil
}
func (s *Server) Stop() error {
	s.cancel()
	s.Listener.Close()
	s.Logger.Close()
	s.Wait()
	log.Println("安全退出")
	return nil
}
func (s *Server) Work() (err error) {
	// 各个角色要做的事情
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-s.context.Done():
			// 收到退出通知，关闭下游
			return
		case <-ticker.C:
			switch s.State {
			case Follower:
				// 检查心跳
				s.followerJob()
			case Candidate:
				// 发起选举, 检查选举资格是否被否决
				s.electionJob()
			case Leader:
				// 发送心跳
				s.leaderJob()
			case Nobody:
				// 啥也不做，等待leader产生
				time.Sleep(time.Second)
			}
		}
	}
	return nil
}
func (s *Server) leaderJob() (err error) {
	for _, addr := range s.ClusterMember {
		req := HeartbeatReq{
			Addr:      s.Addr,
			Heartbeat: time.Now().Unix(),
			Term:      s.Term,
			Offset:    s.Logger.Offset(),
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
		case call := <-client.Go(CommunicateHeartbeat, req, &resp, nil).Done:
			err = call.Error
		}
		if err != nil {
			log.Println(err)
			// 不管。leader 只管发心跳
			client.Close()
			continue
		}

		if resp.Deviation == 0 { // 日志没有偏差，继续发下一个节点
			client.Close()
			continue
		}

		buff, err := s.Logger.ReadItem(resp.Deviation)
		if err != nil {
			return err
		}
		logItemReq := LogItemReq{Data: buff}
		logItemReply := LogItemReply{}
		select {
		case <-time.After(time.Second):
			log.Printf("%s call timeout \n", addr)
			err = fmt.Errorf("call timeout")
		case call := <-client.Go(CommunicateSyncLog, logItemReq, &logItemReply, nil).Done:
			err = call.Error
		}
		if err != nil {
			log.Println(err)
			// 不管。// todo
			client.Close()
			continue
		}
		if logItemReply.IsSuccess {
			client.Close()
		} else {
			//todo 日志同步失败处理
		}

	}
	return
}
func (s *Server) electionJob() (err error) {
	log.Println("开始选举")
	s.VoteTerm = s.Term
	count := 0
	for _, addr := range s.ClusterMember {
		req := ElectionReq{
			Addr:   s.Addr,
			Term:   s.Term,
			Offset: s.Logger.Offset(),
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
		case call := <-client.Go(CommunicateElection, req, &resp, nil).Done:
			err = call.Error
		}
		if err != nil {
			log.Println(err)
			continue
		}
		if resp.IsVote {
			count++
		}
		if resp.IsRejected {
			s.State = Nobody
			break
		}
		client.Close()
	}
	if s.State == Nobody {
		log.Println("日志落后，成为 nobody")
		return
	}
	log.Println("获得选票", count)
	if count > len(s.ClusterMember)/2 {
		log.Println("选举成功")
		s.State = Leader
		return
	}
	log.Println("选举失败") // 选举失败的不要马上发起下一次选举
	time.Sleep(time.Second)
	return
}
func (s *Server) followerJob() (err error) {
	if (s.Heartbeat + s.HeartbeatTimeout) > time.Now().Unix() {
		return
	}
	log.Println("心跳超时")
	s.State = Candidate // 转换角色
	s.Term = s.Term + 1
	return
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
func (s *Server) LogServer() (err error) {

	// 检查
	//_,err := os.Stat(s.LogFilePath)

	// 日志服务
	f, err := os.OpenFile(s.LogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	serverLog := new(ServerLog)
	s.Logger = serverLog
	s.Logger.File = f
	s.LogFile = f

	// 每次重起要读log文件重放到 Data中
	err = s.Logger.Replay(s.Data)
	if err != nil {
		// todo
		return
	}

	// todo 不应该直接让server操作文件。应该加一个中间件。但是简单做直接写文件吧
	// 启动循环定期更新日志到磁盘中
	return nil
}

/*
api
*/
type ServerApi struct {
	server *Server
}

/*
conmunicate
*/
type HeartbeatReq struct {
	Addr      string
	Heartbeat int64
	Term      int64

	Offset int64
}
type HeartbeatReply struct {
	Deviation int64 // 数据偏差
}
type ElectionReq struct {
	Addr   string // 发起选举的候选人
	Term   int64  // 任期
	Offset int64  // 日志偏移量
}
type ElectionReply struct {
	IsRejected bool
	IsVote     bool
}

func (s *ServerApi) Heartbeat(req HeartbeatReq, reply *HeartbeatReply) (err error) {
	if req.Term < s.server.Term { // 旧leader 发的，通知他转为follower，或者不管，这样旧leader会等到新leader给他发的心跳，自己转为follower
		return
	}

	// req.Term >= s.server.Term
	// 设置心跳
	s.server.Term = req.Term
	s.server.Heartbeat = req.Heartbeat
	s.server.Leader = req.Addr
	// 极端情况下，可能会有一个和leader 相同term的candidate
	if req.Addr != s.server.Addr { //一旦收到新的leader，leader以外的所有人都转为follower
		s.server.State = Follower
	}
	// 新的leader已经产生，重制投票，为下次投票准备
	s.server.VoteTo = ""

	// 判断log offset
	if s.server.State == Leader { // 如果自己是leader 就不用去同步log了
		return
	}
	if req.Offset == s.server.Logger.Offset() {
		return
	}
	if req.Offset > s.server.Logger.Offset() {
		log.Println("发现日志偏移")
		reply.Deviation = s.server.Logger.Offset() - req.Offset
	}
	return
}
func (s *ServerApi) Election(req ElectionReq, reply *ElectionReply) (err error) {

	log.Println("选举投票")

	// 先比较日志，是否比自己新或者等于自己
	if req.Offset < s.server.Logger.Offset() {
		reply.IsRejected = true    // 有这一票直接禁止选举
		s.server.State = Candidate // 自己的日志更新，自己来当候选人
		return
	}

	// 再比较版本，是否大于等于自己投的那个版本
	if req.Term < s.server.VoteTerm {
		return
	}
	if req.Term > s.server.VoteTerm {
		// 任期更大，直接投给这个人，记录以下投给了谁
		s.server.VoteTo = req.Addr
		s.server.VoteTerm = req.Term
		reply.IsVote = true
		return nil
	}
	// req.Term == s.server.VoteTerm
	// 再判断自己是否投票了
	if len(s.server.VoteTo) == 0 { //还没投给别人
		s.server.VoteTo = req.Addr
		s.server.VoteTerm = req.Term
		reply.IsVote = true
		return nil
	}
	if s.server.VoteTo == req.Addr { // 之前投过的人，继续来找我们确认选票了
		s.server.VoteTo = req.Addr
		s.server.VoteTerm = req.Term
		reply.IsVote = true
		return nil
	}
	return
}

type LogItemReq struct {
	Data []byte
}
type LogItemReply struct {
	IsSuccess bool
	Err       error
}

func (s *ServerApi) SyncLog(req LogItemReq, reply *LogItemReply) (err error) {
	log.Println("同步日志")

	_, err = s.server.LogFile.Write(req.Data)
	if err != nil {
		reply.IsSuccess = false
		reply.Err = err
		return
	}
	err = s.server.Logger.Load(s.server.Data, req.Data)
	if err != nil {
		reply.IsSuccess = false
		reply.Err = err
		return
	}
	reply.IsSuccess = true
	return
}

/*
store
*/
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

func (s *ServerApi) Set(req ItemReq, reply *ItemReply) (err error) {

	server := s.server
	if server.State != Leader {
		log.Println("请求到了follower,代理写请求")
		// todo 根据当前是否leader 进行转发处理，非leader不处理写请求，有两种处理方式，1.代转发和2.直接返回给client leader地址让client自己写
		client, err := rpc.Dial("tcp", server.Leader)
		if err != nil {
			reply.Err = err
			return err
		}

		select {
		case <-time.After(1 * time.Second):
			reply.Err = fmt.Errorf("call timeout")
			err = fmt.Errorf("call timeout")
		case call := <-client.Go(ApiSet, req, reply, nil).Done:
			reply.Err = call.Error
			err = call.Error
		}
		if err != nil {
			client.Close()
			return err
		}
		client.Close()
		return err
	}
	// todo  根据 同步级别 和 指定同步节点个数 记录日志，这里简单做就先做 [半同步,1个节点] ps:详见blog
	err = server.Logger.Record(req.Key, req.Val, "U")
	if err != nil {
		return
	}
	reply.IsSuccess = true
	server.Lock()
	defer server.Unlock()

	server.Data[req.Key] = req.Val
	return nil
}
func (s *ServerApi) Get(req ItemReq, reply *ItemReply) (err error) {
	server := s.server
	server.Lock()
	defer server.Unlock()

	v, ok := server.Data[req.Key]
	if !ok {
		return fmt.Errorf("not exist key")
	}
	reply.IsSuccess = true
	reply.Val = v
	return
}

/*
log
*/
type ServerLog struct {
	File *os.File
}

func (s *ServerLog) Offset() int64 {
	ret, _ := s.File.Seek(0, 2)
	return ret

}
func (s *ServerLog) Sync() {
	s.File.Sync()
}
func (s *ServerLog) ReadItem(deviation int64) (buff []byte, err error) {
	log.Println("读取同步日志")

	offset, err := s.File.Seek(deviation, 2)
	if err != nil {
		return nil, err
	}
	buff = make([]byte, -deviation)
	_, err = s.File.ReadAt(buff, offset)
	if err != nil {
		return nil, err
	}
	return

}
func (s *ServerLog) Record(key string, val interface{}, action string) (err error) {
	log.Println("记录日志数据")

	_, err = s.File.Write([]byte(action + ":" + key + ":" + val.(string) + "\n"))
	return
}
func (s *ServerLog) Replay(data map[string]interface{}) (err error) {

	log.Println("加载日志数据")

	size, err := s.File.Seek(0, 2)
	if err != nil {
		return
	}
	buff := make([]byte, size)
	_, err = s.File.ReadAt(buff, 0)
	if err != nil {
		return
	}
	err = s.Load(data, buff)
	return err
}
func (s *ServerLog) Load(data map[string]interface{}, buff []byte) (err error) {
	str := string(buff)
	entrys := strings.Split(str, "\n")
	for i, entry := range entrys {
		if i == len(entrys)-1 { // 最后一个是空的 因为每个项 是 xxx\n
			continue
		}
		e := strings.Split(entry, ":")
		if len(e) != 3 {
			return fmt.Errorf("invalid log entry")
		}
		switch e[0] {
		case "A":
			data[e[1]] = e[2]
		case "U":
			data[e[1]] = e[2]
		case "D": // todo
		}
	}
	return
}
func (s *ServerLog) Close() {
	s.File.Close()
}

// todo 单节点变更
