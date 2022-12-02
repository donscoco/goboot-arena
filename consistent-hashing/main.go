package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

func main() {

	type Node struct {
		Name string
		Addr string
	}

	var n1 = Node{Name: "n91", Addr: "192.168.2.1:9091"}
	var n2 = Node{Name: "n92", Addr: "192.168.2.1:9092"}
	var n3 = Node{Name: "n93", Addr: "192.168.2.1:9093"}

	var ch = NewConsistentHash()
	ch.Add(n1.Name, n1, 1)
	ch.Add(n2.Name, n2, 1)
	ch.Add(n3.Name, n3, 1)

	var key1 = "abc"
	var key2 = "def"
	var key3 = "ghi"

	fmt.Printf("node1 hashcode:%d \n", hashKey(n1.Name+"0"))
	fmt.Printf("node2 hashcode:%d \n", hashKey(n2.Name+"0"))
	fmt.Printf("node3 hashcode:%d \n", hashKey(n3.Name+"0"))

	ret1 := ch.Get(key1)
	fmt.Printf("get key1:%d, return node:%d \n", hashKey(key1), hashKey(ret1.(Node).Name+"0"))
	ret2 := ch.Get(key2)
	fmt.Printf("get key2:%d, return node:%d \n", hashKey(key2), hashKey(ret2.(Node).Name+"0"))
	ret3 := ch.Get(key3)
	fmt.Printf("get key3:%d, return node:%d \n", hashKey(key3), hashKey(ret3.(Node).Name+"0"))

}

// 提供功能：
/*
hash
添加节点
根据hash获得节点
*/
type ConsistentHash struct {
	circle hashcode
	nodes  map[uint32]interface{}
	vnodes map[string]int // 每个节点对应的虚拟节点数量

	sync.Mutex
}

func NewConsistentHash() (c *ConsistentHash) {
	c = new(ConsistentHash)
	c.circle = make([]uint32, 0, 16)
	c.nodes = make(map[uint32]interface{}, 16)
	c.vnodes = make(map[string]int, 16)
	return
}

// 添加节点到 hash 环
func (c *ConsistentHash) Add(name string, node interface{}, virtualNum int) {
	c.Lock()
	defer c.Unlock()

	if virtualNum == 0 { // 每个节点在映射出多个虚拟节点，为了在哈希环上分布均匀，至少一个节点
		virtualNum = 1
	}

	for i := 0; i < virtualNum; i++ {
		h := hashKey(name + strconv.Itoa(i))
		c.circle = append(c.circle, h)
		sort.Sort(c.circle)
		c.nodes[h] = node
	}
	c.vnodes[name] = virtualNum
}

func (c *ConsistentHash) Remove(name string) {
	c.Lock()
	defer c.Unlock()

	virtualNum := c.vnodes[name]
	for i := 0; i < virtualNum; i++ {
		h := hashKey(name + strconv.Itoa(i))
		newCircle := make([]uint32, 0, len(c.circle))
		for _, hashcode := range c.circle {
			if hashcode == h {
				continue
			}
			newCircle = append(newCircle, hashcode) // 这里原本就有序，不需要重新排序
		}
		c.circle = newCircle
		delete(c.nodes, h)
	}
	delete(c.vnodes, name)

	//sort.Sort(c.circle)
}

func (c *ConsistentHash) Get(key string) (node interface{}) {
	c.Lock()
	defer c.Unlock()

	hc := hashKey(key)
	i := 0
	for ; i < len(c.circle); i++ {
		if c.circle[i] > hc {
			break
		}
	}
	if i == len(c.circle) {
		i = 0
	}
	return c.nodes[c.circle[i]]

}

// 实现排序接口
type hashcode []uint32

func (h hashcode) Len() int           { return len(h) }
func (h hashcode) Less(i, j int) bool { return h[i] < h[j] }
func (h hashcode) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}
