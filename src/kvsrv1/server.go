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
	mu             sync.Mutex
	Map            map[string]string
	Version        rpc.Tversion
	storedGetReply map[string]*rpc.GetReply
	storedPutReply map[string]*rpc.PutReply
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// 初始化所有的 map
	kv.Map = make(map[string]string)
	kv.storedGetReply = make(map[string]*rpc.GetReply)
	kv.storedPutReply = make(map[string]*rpc.PutReply)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	storedReply, isResend := kv.storedGetReply[args.RequestId]
	if isResend {
		// 复制存储的回复内容到当前reply
		*reply = *storedReply
		return
	}
	value, exist := kv.Map[args.Key]
	if !exist {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = value
		reply.Version = kv.Version
		reply.Err = rpc.OK
	}
	kv.storedGetReply[args.RequestId] = reply

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.Map[args.Key]
	//先校验是否是重发消息
	storedReply, isResend := kv.storedPutReply[args.RequestId]
	if isResend {
		*reply = *storedReply
		log.Printf("..........putreply.err: %v", reply.Err)
		if args.Version != kv.Version {
			reply.Err = rpc.ErrMaybe
		}
		return
	}
	//在校验键是否存在
	if !exist {
		if args.Version == 0 {
			kv.Map[args.Key] = args.Value
			kv.Version += 1
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if kv.Version != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			kv.Map[args.Key] = args.Value
			kv.Version += 1
			reply.Err = rpc.OK
		}

	}
	kv.storedPutReply[args.RequestId] = reply

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
