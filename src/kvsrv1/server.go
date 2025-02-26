package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]Inner

	// Your definitions here.
}

type Inner struct {
	value   string
	version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:   sync.Mutex{},
		data: make(map[string]Inner),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[server] get key:%s", args.Key)
	key := args.Key
	result, exist := kv.data[key]
	if exist {
		reply.Value = result.value
		reply.Version = result.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	DPrintf("[server] get answer value:%s version:%v err:%v\n", reply.Value, reply.Version, reply.Err)

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// Args.Version is 0.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[server] put key:%s value:%s version:%d", args.Key, args.Value, args.Version)
	key := args.Key
	value := args.Value
	version := args.Version
	result, exist := kv.data[key]
	if !exist {
		// 如果这个key-value不存在
		if version == 0 {
			// 如果传入版本为0时创建这个键值对
			inner := Inner{
				value:   value,
				version: 1,
			}
			kv.data[key] = inner
			DPrintf("[server] put new key:%s value:%s version:%d", args.Key, args.Value, args.Version)
			reply.Err = rpc.OK
		} else {
			// 如果传入版本不是0返回ErrNoKey错误
			reply.Err = rpc.ErrNoKey
		}
	} else {
		// 如果这个key-value存在
		// 首先判断版本对不对
		if version == result.version {
			// 如果版本一致
			result.value = value
			result.version++
			kv.data[key] = result
			DPrintf("[server] put update key:%s value: %s->%s version:%d", args.Key, result.value, args.Value, args.Version)
			reply.Err = rpc.OK
		} else {
			// 如果版本不一致
			reply.Err = rpc.ErrVersion
		}
	}
	DPrintf("[server] put answer err %v\n", reply.Err)
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
