package shardmaster

import (
    "log"
    "os"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        f, err := os.OpenFile("log_shardmaster", os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
        if err != nil {
            log.Printf("error opening file: %v", err)
        }
        defer f.Close()
        log.SetOutput(f)
        log.Printf(format, a...)
    }
    return
}

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

    Id      int64
    Serial  int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

    Id      int64
    Serial  int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

    Id      int64
    Serial  int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

    Id      int64
    Serial  int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
