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

type item struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kva map[string]*item
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kva = make(map[string]*item)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, ok := kv.kva[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = data.value
	reply.Version = data.version
	reply.Err = rpc.OK

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	tmp_key := args.Key
	tmp_val := args.Value
	tmp_version := args.Version

	data, ok := kv.kva[tmp_key]

	if !ok {
		if tmp_version == 0 {
			kv.kva[tmp_key] = &item{
				value:   tmp_val,
				version: tmp_version + 1,
			}
			reply.Err = rpc.OK
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	}

	if data.version == tmp_version {
		kv.kva[tmp_key].version++
		kv.kva[tmp_key].value = tmp_val
		reply.Err = rpc.OK
		return
	}

	reply.Err = rpc.ErrVersion

}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
