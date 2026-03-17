package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
	id   string
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
	lk.name = lockname
	lk.id = ""
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, version, err := lk.ck.Get(lk.name)
		if lk.id != "" && val == lk.id {
			return
		}
		if err == rpc.ErrNoKey {
			if lk.id == "" {
				lk.id = kvtest.RandValue(8)
				}
				status := lk.ck.Put(lk.name, lk.id, 0)
				if status == rpc.OK {
					return
				}
				if status != rpc.ErrMaybe {
					lk.id = ""
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if val == "" {
				if lk.id == "" {
					lk.id = kvtest.RandValue(8)
				}
				status := lk.ck.Put(lk.name, lk.id, version)
				if status == rpc.OK {
					return
				}
				if status != rpc.ErrMaybe {
					lk.id = ""
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			time.Sleep(10 * time.Millisecond)
		}

	}

func (lk *Lock) Release() {
	for {
		val, version, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			lk.id = ""
			return
		}
		if val == "" {
			lk.id = ""
			return
		}
		if val == lk.id {
			status := lk.ck.Put(lk.name, "", version)
			if status == rpc.OK {
				lk.id = ""
				return
			}
			if status == rpc.ErrMaybe {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// If the lock is held by someone else, this clerk does not own it.
		lk.id = ""
		return
	}
}
