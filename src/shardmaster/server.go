package shardmaster


import "raft"
import "labrpc"
import "sync"
import "time"
import "encoding/gob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
    done    map[int64]int // the latest serial delt for id
    result  map[int]chan Op // result[i] denote the applyCh for entry at index i
}


type Op struct {
	// Your data here.
    Op      string // "Join", "Leave", "Move", "Query"
    Id      int64
    Serial  int
    Args    interface{}
}

func (sm *ShardMaster) AppendEntry(entry Op) bool {
    index, _, isLeader := sm.rf.Start(entry)
    if !isLeader {
        return false
    }

    sm.mu.Lock()
    ch, ok := sm.result[index]

    if !ok {
        ch = make(chan Op, 1)
        sm.result[index] = ch
    }
    sm.mu.Unlock()

    select {
        case op := <-ch:
            return op.Id == entry.Id && op.Serial == entry.Serial  // since the op may be from previous round
        case <- time.After(1000 * time.Millisecond): // this is why sm.result[index] may be not empty
            DPrintf("AppendEntry timeout")
            return false
    }
}

func (sm *ShardMaster) IsDone(id int64, serial int) bool {
    v, ok := sm.done[id]
    if ok {
        return v >= serial
    }
    return false
}

func (sm *ShardMaster) NewConfig() *Config {
    // just copy the last Config 
    var c Config
    c.Num = len(sm.configs)
    c.Shards = sm.configs[c.Num-1].Shards
    c.Groups = map[int][]string{}
    for k, v := range sm.configs[c.Num-1].Groups {
        c.Groups[k] = v
    }
    sm.configs = append(sm.configs, c)
    return &sm.configs[c.Num]
}

func (sm *ShardMaster) GetHeavyGid(gidLoad map[int][]int) int {
    max := 0
    var gid int
    for k, v := range gidLoad {
        if len(v) > max {
            max = len(v)
            gid = k
        }
    }
    return gid
}

func (sm *ShardMaster) GetLightGid(gidLoad map[int][]int) int {
    min := 257 // from test_test.go 
    var gid int
    for k, v := range gidLoad {
        if len(v) < min {
            min = len(v)
            gid = k
        }
    }
    return gid
}

func (sm *ShardMaster) GetGidLoad(cfg *Config) map[int][]int {
    gidLoad := map[int][]int{}
    for k := range cfg.Groups {
        gidLoad[k] = []int{}
    }
    for k, v := range cfg.Shards {
        gidLoad[v] = append(gidLoad[v], k)
    }
    return gidLoad
}

func (sm *ShardMaster) Rebalance(cfg *Config, op string, gid int) {
    gidLoad := sm.GetGidLoad(cfg)
    switch op {
        case "Join":
            meanNum := NShards / len(cfg.Groups)
            for i := 0; i < meanNum; i++ {
                heavyGid := sm.GetHeavyGid(gidLoad)
                cfg.Shards[gidLoad[heavyGid][0]] = gid
                gidLoad[heavyGid] = gidLoad[heavyGid][1:]
            }
        case "Leave":
            shards := gidLoad[gid]
            delete(gidLoad, gid)
            for _, v := range(shards) {
                lightGid := sm.GetLightGid(gidLoad)
                cfg.Shards[v] = lightGid
                gidLoad[lightGid] = append(gidLoad[lightGid], v)
            }
    }
}

func (sm *ShardMaster) Apply(args Op) {
    DPrintf("args.Op: %v, args.Args: %v", args.Op, args.Args)
    switch args.Op {
    // TODO: fill these in
        case "Join":
            joinArgs := args.Args.(JoinArgs)
            cfg := sm.NewConfig()
            for GID, joinServers := range joinArgs.Servers {
                if _, ok := cfg.Groups[GID]; !ok {
                    cfg.Groups[GID] = joinServers
                    sm.Rebalance(cfg, "Join", GID)
                }
            }
            DPrintf("cfg.Shards: %v", cfg.Shards)
            DPrintf("cfg.Groups: %v", cfg.Groups)
        case "Leave":
            leaveArgs := args.Args.(LeaveArgs)
            cfg := sm.NewConfig()
            for _, GID := range leaveArgs.GIDs {
                DPrintf("GID: %d, cfg.Groups[GID]: %v", GID, cfg.Groups[GID])
                if _, ok := cfg.Groups[GID]; ok {
                    delete(cfg.Groups, GID)
                    sm.Rebalance(cfg, "Leave", GID)
                }
            }
            DPrintf("cfg.Shards: %v", cfg.Shards)
            DPrintf("cfg.Groups: %v", cfg.Groups)
        case "Move":
            moveArgs := args.Args.(MoveArgs)
            cfg := sm.NewConfig()
            cfg.Shards[moveArgs.Shard] = moveArgs.GID
    }
    sm.done[args.Id] = args.Serial
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
    entry := Op{Op: "Join", Args: *args, Id: args.Id, Serial: args.Serial}
    ok := sm.AppendEntry(entry)

    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
    entry := Op{Op: "Leave", Args: *args, Id: args.Id, Serial: args.Serial}
    ok := sm.AppendEntry(entry)

    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
    entry := Op{Op: "Move", Args: *args, Id: args.Id, Serial: args.Serial}
    ok := sm.AppendEntry(entry)

    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
    entry := Op{Op: "Query", Args: *args, Id: args.Id, Serial: args.Serial}
    ok := sm.AppendEntry(entry) // here it waits for apply


    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK

        sm.mu.Lock()
        if args.Num >= 0 && args.Num < len(sm.configs) {
            reply.Config = sm.configs[args.Num]
        } else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
        sm.done[args.Id] = args.Serial
	    sm.mu.Unlock()
    }
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveReply{})
	gob.Register(MoveReply{})
	gob.Register(QueryReply{})

    sm.done = make(map[int64]int)
    sm.result = make(map[int]chan Op)

    go func() {
        for {
            msg := <-sm.applyCh // when msg is committed

            // no UseSnapshot now
            op := msg.Command.(Op)
            sm.mu.Lock()
            if !sm.IsDone(op.Id, op.Serial) {
                sm.Apply(op)
            }

            ch, ok := sm.result[msg.Index]
            if ok {
                select {
                    // consume the existing channel
                    case <-sm.result[msg.Index]:
                    default:
                }
                ch <- op
            } else {
                sm.result[msg.Index] = make(chan Op, 1)
            }
            sm.mu.Unlock() // stupid!
        }
    }()

	return sm
}
