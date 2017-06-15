package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "time"
import "encoding/gob"
import "log"
import "bytes"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Op      string
    Key     string
    Value   string
    Id      int64
    Serial  int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    db      map[string]string
    done    map[int64]int
    result  map[int]chan Op
}

func (kv *ShardKV) AppendEntry(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
    DPrintf("%v is leader",  kv.me)

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
		case <-time.After(1000 * time.Millisecond):
            DPrintf("AppendEntry timeout")
			return false
	}
}

func (kv *ShardKV) IsDone(id int64, serial int) bool {
	v, ok := kv.done[id]
	if ok {
		// if v is smaller than serial, then it has been done
		return v >= serial
	}
	return false
}

func (kv *ShardKV) Apply(args Op) {
	switch args.Op {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.done[args.Id] = args.Serial
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    entry := Op{Op:"Get", Key:args.Key, Id:args.Id, Serial:args.Serial}

	ok := kv.AppendEntry(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false

		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.done[args.Id] = args.Serial // this is Apply
		log.Printf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.done = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

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
