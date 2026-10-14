package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Val struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	m  map[string]Val
	mu sync.Mutex
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args := req.(type) {
	case rpc.GetArgs:
		val, ok := kv.m[args.Key]
		if !ok {
			reply := rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
			return reply
		}
		reply := rpc.GetReply{
			Value:   val.Value,
			Version: val.Version,
			Err:     rpc.OK,
		}
		return reply
	case rpc.PutArgs:
		val, ok := kv.m[args.Key]
		reply := rpc.PutReply{
			Err: rpc.OK,
		}
		if !ok {
			if args.Version != 0 {
				reply.Err = rpc.ErrNoKey
				return reply
			}
			args.Version++
			tmp_val := Val{
				Value:   args.Value,
				Version: args.Version,
			}
			kv.m[args.Key] = tmp_val
			return reply
		}
		if args.Version != val.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}
		val.Value = args.Value
		val.Version++
		kv.m[args.Key] = val
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	enc.Encode(kv.m)
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	var tmpData map[string]Val
	if err := dec.Decode(&tmpData); err != nil {
		panic(err)
	}
	kv.m = tmpData
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, m: make(map[string]Val)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
