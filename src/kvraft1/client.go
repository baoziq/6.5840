package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
	sum int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.sum = len(servers)
	ck.leader = 0
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	i := ck.leader
	for {
		args := rpc.GetArgs{
			Key: key,
		}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
		if !ok {
			i = (i + 1) % ck.sum
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			i = (i + 1) % ck.sum
			continue
		}
		ck.leader = i
		return reply.Value, reply.Version, reply.Err

	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	isFirst := true
	// log.Printf("client start Put\n")
	i := ck.leader
	for {
		args := rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
		if !ok {
			isFirst = false
			i = (i + 1) % ck.sum
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// log.Printf("Put rpc successed\n")
		if reply.Err == rpc.ErrWrongLeader {
			// The old leader may have committed the Put before losing leadership.
			isFirst = false
			// log.Printf("server %v is not leader, next\n", i)
			i = (i + 1) % ck.sum
			continue
		}
		ck.leader = i
		// log.Printf("server %v is leader\n", i)
		if reply.Err == rpc.ErrVersion {
			if isFirst {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		return reply.Err
	}
	// return rpc.ErrMaybe
}
