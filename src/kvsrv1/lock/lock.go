package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type LockStatus int

const (
	locked LockStatus = iota
	unlocked
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	lk.id = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		id, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			putErr := lk.ck.Put(lk.lockname, lk.id, 0)
			if putErr == rpc.OK {
				return
			}
			continue
		}
		if id == lk.id {
			return
		}
		if id == "" {
			putErr := lk.ck.Put(lk.lockname, lk.id, version)
			if putErr == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		id, version, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			continue
		}
		if id != lk.id {
			return
		}
		err = lk.ck.Put(lk.lockname, "", version)
		if err == rpc.OK {
			return
		}
	}
}
