package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "time"
import "encoding/gob"
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
    Err     Err

    Args    interface{}
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
    db      [shardmaster.NShards]map[string]string
    done    map[int64]int
    result  map[int]chan Op

    mck     *shardmaster.Clerk
    cfg     shardmaster.Config
}

func (kv *ShardKV) AppendEntry(entry Op) (bool, Err) {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false, OK
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
            // op may be ReconfigureArgs
			return op.Id == entry.Id && op.Serial == entry.Serial, op.Err
		case <-time.After(200 * time.Millisecond):
            DPrintf("AppendEntry timeout")
			return false, OK
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
    DPrintf("PutAppend %s to %s: %s", args.Value, args.Key,
            kv.db[key2shard(args.Key)][args.Key])
	switch args.Op {
	case "Put":
    kv.db[key2shard(args.Key)][args.Key] = args.Value
	case "Append":
    kv.db[key2shard(args.Key)][args.Key] += args.Value
    case "Reconfigure":
		args := args.Args.(ReconfigureArgs)
		DPrintf("Reconfigure args.Cfg.Num: %d, kv.cfg.Num: %d", args.Cfg.Num, kv.cfg.Num)
        if args.Cfg.Num <= kv.cfg.Num {
            return
        }
		// already reached consensus, merge db and ack
		for shardIndex, data := range args.StoreShard {
            DPrintf("kv.db of %d before reconfigure: %v", kv.me, kv.db)
			for k, v := range data {
				kv.db[shardIndex][k] = v
			}
            DPrintf("kv.db of %d after reconfigure: %v", kv.me, kv.db)
		}
		for clientId := range args.Ack {
			if _, exist := kv.done[clientId]; !exist || kv.done[clientId] < args.Ack[clientId] {
				kv.done[clientId] = args.Ack[clientId]
			}
		}
		kv.cfg = args.Cfg

        //for i := 0; i < shardmaster.NShards; i++ {
        //    if kv.cfg.Shards[i] != kv.gid {
        //        for k, _ := range kv.db[i] {
        //            delete(kv.db[i], k)
        //        }
        //    }
        //}

		DPrintf("kv.cfg after reconfigure: %v", kv.cfg)
	}
    // return timely
    kv.done[args.Id] = args.Serial
}

func (kv *ShardKV) CheckValidKey(key string) bool {
	shardId := key2shard(key)
    DPrintf("kv.gid: %d, kv.cfg.Shards[%d]: %d", kv.gid, shardId, kv.cfg.Shards[shardId])
	if kv.gid != kv.cfg.Shards[shardId] {
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    entry := Op{Op:"Get", Key:args.Key, Id:args.Id, Serial:args.Serial}

	ok, Err := kv.AppendEntry(entry)
    //DPrintf("%v append entry %v returns %v, %s", kv.rf, entry, ok, Err)
	if !ok {
		reply.WrongLeader = true
	} else {
		// reconfigure
		reply.WrongLeader = false
		reply.Err = Err

		kv.mu.Lock()
		reply.Value = kv.db[key2shard(args.Key)][args.Key] // since we need to set reply.Value
		kv.done[args.Id] = args.Serial // this is Apply
		DPrintf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Op:args.Op, Key:args.Key, Value:args.Value,
				Id:args.Id, Serial:args.Serial}

	ok, Err := kv.AppendEntry(entry)
    //DPrintf("%v append entry %v returns %v, %s", kv.rf, entry, ok, Err)
	if !ok {
		reply.WrongLeader = true
	} else {
		// reconfigure
		reply.WrongLeader = false
		reply.Err = Err
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

func (kv *ShardKV) SendTransferShard(gid int, args *TransferArgs, reply *TransferReply) bool {
	for _, server := range kv.cfg.Groups[gid] {
		//DPrintln("server", kv.gid, kv.me, "send transfer to:", gid, server)
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.TransferShard", args, reply)
		if ok {
			DPrintf("reply.Err: %s", reply.Err)
			if reply.Err == OK {
                // Delete: need to clear all logs here
				return true
			} else if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return false
}

func (kv *ShardKV) TransferShard(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("kv.cfg.Num: %d, args.ConfigNum: %d", kv.cfg.Num, args.ConfigNum)
	if kv.cfg.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// at that case, the target shards have been released by prior owner
	reply.Err = OK
	//??? why have to init in remote server
	reply.Ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.StoreShard[i] = make(map[string]string)
	}
	for _, shardIndex := range args.Shards {
		for k, v := range kv.db[shardIndex] {
			reply.StoreShard[shardIndex][k] = v
		}
	}

	for clientId := range kv.done {
		reply.Ack[clientId] = kv.done[clientId]
	}
}

func (kv *ShardKV) GetReconfigure(nextCfg shardmaster.Config) (ReconfigureArgs, bool) {
	retArgs := ReconfigureArgs{Cfg:nextCfg}
	retArgs.Ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		retArgs.StoreShard[i] = make(map[string]string)
	}
	retOk := true

	transShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.cfg.Shards[i] != kv.gid && nextCfg.Shards[i] == kv.gid {
            // trans shard i from gid to kv.gid
			gid := kv.cfg.Shards[i]
			if gid != 0 {
				if _, ok := transShards[gid]; !ok {
					transShards[gid] = []int{i}
				} else {
					transShards[gid] = append(transShards[gid], i)
				}
			}
		}
	}

    DPrintf("transShards: %v", transShards)

    // transShards[gid]: shards id from gid to kv.gid
	var ackMutex sync.Mutex
	var wait sync.WaitGroup
	for gid, value := range transShards {	// iterating map
		wait.Add(1)
		go func(gid int, value []int) {
			defer wait.Done()
			var reply TransferReply

			if kv.SendTransferShard(gid, &TransferArgs{ConfigNum:nextCfg.Num, Shards:value}, &reply) {
				ackMutex.Lock()
				//!!! be careful that can not init args here, for
				// it will re-init every time, lost data!
				for shardIndex, data := range reply.StoreShard {
					for k, v := range data {
						retArgs.StoreShard[shardIndex][k] = v
					}
				}
				for clientId := range reply.Ack {
					if _, exist := retArgs.Ack[clientId]; !exist || retArgs.Ack[clientId] < reply.Ack[clientId] {
						retArgs.Ack[clientId] = reply.Ack[clientId]
						//retArgs.Replies[clientId] = reply.Replies[clientId]
					}
				}
				ackMutex.Unlock()
			} else {
				retOk = false
			}
		} (gid, value)
	}
	wait.Wait()

	DPrintf("retArgs: %v, retOk: %v", retArgs, retOk)
	return retArgs, retOk
}

func (kv *ShardKV) SyncReconfigure(args ReconfigureArgs) bool {
	// retry 3 times
	for i := 0; i < 3; i++ {
		index, _, isLeader := kv.rf.Start(Op{Op:"Reconfigure", Args:args})
		if !isLeader {
			return false
		}

		kv.mu.Lock()
		if _, ok := kv.result[index]; !ok {
			kv.result[index] = make(chan Op, 1)
		}
		chanMsg := kv.result[index]
		kv.mu.Unlock()

		select {
		case msg := <-chanMsg:
			if tmpArgs, ok := msg.Args.(ReconfigureArgs); ok {
				DPrintf("args.Cfg.Num: %d, tmpArgs.Cfg.Num: %d", args.Cfg.Num, tmpArgs.Cfg.Num)
				if args.Cfg.Num == tmpArgs.Cfg.Num {
					return true
				}
			}
		case <- time.After(200 * time.Millisecond):
			DPrintf("SyncReconfigure timeout")
			continue
		}
	}
	return false
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

	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})
	gob.Register(shardmaster.Config{})

	gob.Register(ReconfigureArgs{})
	gob.Register(ReconfigureReply{})
	gob.Register(TransferArgs{})
	gob.Register(TransferReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
    for i := 0; i < shardmaster.NShards; i++ {
        kv.db[i] = make(map[string]string)
    }
	kv.done = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    go func() {
		for {
			if _, isLeader := kv.rf.GetState(); isLeader {
				latestCfg := kv.mck.Query(-1)
				for i := kv.cfg.Num + 1; i <= latestCfg.Num; i++ {
					args, ok := kv.GetReconfigure(kv.mck.Query(i))
					DPrintf("args: %v, ok: %v", args, ok)
					if !ok {
						break
					}
					if !kv.SyncReconfigure(args) {
						break
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
    }()

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
                for i := 0; i < shardmaster.NShards; i++ {
                    kv.db[i] = make(map[string]string)
                }
				kv.done = make(map[int64]int)
				d.Decode(&kv.db)
				d.Decode(&kv.done)
                d.Decode(&kv.cfg)
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op) // this is type assertion
				kv.mu.Lock()

                DPrintf("Apply Message: %v", op)
                op.Err = OK

				if op.Op == "Reconfigure" {
					kv.Apply(op)
				} else {
                    if !kv.CheckValidKey(op.Key) {
                        DPrintf("Not Valid Key: %v", op)
                        op.Err = ErrWrongGroup
                    } else if !kv.IsDone(op.Id, op.Serial) {
                        kv.Apply(op)
                        // else if rather than if
                    }
                }

				ch, ok := kv.result[msg.Index]
				if ok {
					select {
						case <-kv.result[msg.Index]:
						default:
					}
                    // here we do not know where Apply succeed 
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
                    e.Encode(kv.cfg)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data, msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
