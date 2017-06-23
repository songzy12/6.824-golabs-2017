package raftkv

import (
    "bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
	"os"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		f, err := os.OpenFile("log_kvraft", os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
        if err != nil {
            log.Printf("error opening file: %v", err)
        }
        defer f.Close()
        log.SetOutput(f)
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op		string //"Put" or "Append" "Get"
	Key		string
	Value	string
	Id		int64
	Serial	int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db		map[string]string
	done	map[int64]int
	result	map[int]chan Op
}

func (kv *RaftKV) AppendEntry(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
    // if has no result[index], then make a channel to wait it to finish
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
        // AppendEntry will send the entry to raft servers
        // when raft servers reach consensus, it send msg to applyCh
        // when applyCh finished applying, it send msg back
		case op := <-ch:
			return op == entry
		case <-time.After(200 * time.Millisecond):
            DPrintf("AppendEntry timeout")
			return false
	}
}

func (kv *RaftKV) IsDone(id int64, serial int) bool {
	v, ok := kv.done[id]
	if ok {
		// if v is smaller than serial, then it has been done
		return v >= serial
	}
	return false
}

func (kv *RaftKV) Apply(args Op) {
	switch args.Op {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
        DPrintf("%d append %s to %s: %s", kv.me, args.Value, args.Key, kv.db[args.Key])
		kv.db[args.Key] += args.Value
	}
	kv.done[args.Id] = args.Serial
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{Op:"Get", Key:args.Key, Id:args.Id, Serial:args.Serial}

	ok := kv.AppendEntry(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false

		kv.mu.Lock()

		value, ok_ := kv.db[args.Key]
		DPrintf("%d get %v: %s\n", kv.me, entry, reply.Value)
        if !ok_ {
            reply.Err = ErrNoKey
        } else {
		    reply.Err = OK
            reply.Value = value
        }
		kv.done[args.Id] = args.Serial // this is Apply

        kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Op:args.Op, Key:args.Key, Value:args.Value,
				Id:args.Id, Serial:args.Serial}
	ok := kv.AppendEntry(entry)
    //DPrintf("%v append entry %v returns %v", kv.rf, entry, ok)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.done = make(map[int64]int)
	kv.result = make(map[int]chan Op)

    // applyCh to be 100
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			if msg.UseSnapshot {
                // when nextIndex[server] < baseIndex
				var LastIncludedIndex int
				var LastIncludedTerm int

				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.db = make(map[string]string)
				kv.done = make(map[int64]int)
				d.Decode(&kv.db)
				d.Decode(&kv.done)
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op) // this is type assertion
				kv.mu.Lock()
				if !kv.IsDone(op.Id, op.Serial) {
					kv.Apply(op)
				}

				ch, ok := kv.result[msg.Index]
				if ok {
					select {
						case <-kv.result[msg.Index]:
						default:
					}
					ch <- op
				} else {
					kv.result[msg.Index] = make(chan Op, 1)
				}

				//need snapshot
				if maxraftstate > 0 && kv.rf.GetRaftStateSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.done)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data, msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
