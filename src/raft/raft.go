package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "bytes"
import "encoding/gob"
import "labrpc"
import "math/rand"
import "sync"
import "time"

// import "bytes"
// import "encoding/gob"

// the reason is that when request for vote
// candidate may become follower right in the middle
const (
    FOLLOWER = iota
    CANDIDATE
    LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
    Command interface{} // let command be an integer
    Term    int         // term when command received by leader
    Index   int         // since there will be log truncate
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    CurrentTerm     int     // init to 0
    VotedFor        int     // init to -1
    Log             []Entry // first index is 1

    // even I vote for myself, I may not think myself is a leader
    state           int

    commitIndex     int         // init to 0
    lastApplied     int         // init to 0
    entryAppended   chan bool   // leader told me to append entry
    leaderVoted     chan bool   // candidate told me to vote
    leaderElected   chan bool   // I am selected as leader
    applyCommit     chan bool
    chanApply       chan ApplyMsg // used to communicate with kvraft server

    nextIndex       []int   // init to leader last log index + 1 
    matchIndex      []int   // init to 0

    voteCount       int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.CurrentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.CurrentTerm)
	 e.Encode(rf.VotedFor)
     e.Encode(rf.Log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
    d.Decode(&rf.Log)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int // candidate's term
    CandidateId     int // candidate requesting vote
    LastLogIndex    int // index of candidate's last log entry
    LastLogTerm     int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term            int     // currentTerm, for candidate to update
    VoteGranted     bool    // true means received vote
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
    // write them together to avoid race
    i := len(rf.Log) - 1
    return rf.Log[i].Index, rf.Log[i].Term
}

func moreUpToDate(term1 int, index1 int, term2 int, index2 int) bool {
    // write all the complex logic into a function!
    if term1 != term2 {
        return term1 > term2
    }
    return index1 >= index2
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    // we init log with one entry with term 0
    //DPrintf("%d with term %d request %d with term %d to vote", args.CandidateId, args.Term, rf.me, rf.CurrentTerm)

    rf.mu.Lock()
    defer rf.mu.Unlock()
    // eliminate all the possible race
    term, _ := rf.GetState()
    if args.Term < term {
        reply.Term = term
        reply.VoteGranted = false
        return
    }

    if args.Term > term {
        rf.CurrentTerm = args.Term
        rf.VotedFor = -1
        rf.state = FOLLOWER
        rf.persist()
        DPrintf("%d convert to follower, due to request vote args term", rf.me)
        // convert 
    }

    reply.Term = rf.CurrentTerm // not term, since term may have been changed
    lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()

    if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
      moreUpToDate(args.LastLogTerm, args.LastLogIndex,
                   lastLogTerm, lastLogIndex) {
        rf.VotedFor = args.CandidateId
        rf.persist()
        rf.state = FOLLOWER
        reply.VoteGranted = true
        rf.leaderVoted <- true
        DPrintf("%d convert to follower, due to vote granted", rf.me)
        return
    }

    //DPrintf("%d has last log term %d index %d, while candidate %d only has log term %d log index %d", rf.me, lastLogTerm, lastLogIndex, args.CandidateId, args.LastLogTerm, args.LastLogIndex)

    reply.VoteGranted = false
    return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    if !ok {
        return
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()

    term, _ := rf.GetState()
    if reply.Term > term {
        rf.CurrentTerm = reply.Term;
        rf.VotedFor = -1
        rf.state = FOLLOWER
        rf.persist()
        //DPrintf("%d convert to follower, due to request vote reply term", rf.me)
        return
    }
    // I may be in a new term currently 
    if args.Term != rf.CurrentTerm {
        return
    }
    if reply.VoteGranted {
        rf.voteCount += 1
        if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
            rf.state = LEADER
            DPrintf("%d convert to leader, due to majority vote", rf.me)
            lastLogIndex, _ := rf.getLastLogIndexTerm()
            for server := 0; server < len(rf.peers); server++ {
                rf.nextIndex[server] = lastLogIndex + 1
                rf.matchIndex[server] = 0
            }

            rf.leaderElected <- true
            go rf.sendAllAppendEntries()
        }
    }
}

func (rf *Raft) sendAllRequestVote() {
    rf.mu.Lock()
    lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
    term := rf.CurrentTerm
    rf.mu.Unlock()
    // use select and channel 
    for server := 0; server < len(rf.peers); server++ {
        if rf.state != CANDIDATE {
            return
        }
        if server == rf.me {
            continue
        }
        // &, cannot use the same one args
        args := &RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
        reply := &RequestVoteReply{}
        go rf.sendRequestVote(server, args, reply)
        // cannot wait here for ok
    }
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []Entry // empty for heartbeat
    LeaderCommit    int     // leader's commit index
}

type AppendEntriesReply struct {
    Term            int     // current term, for leader to update
    Success         bool    // true if contains matching prev index and term
    ConflictIndex   int
    ConflictTerm    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("follower %d received append entries from %d", rf.me, args.LeaderId)
    rf.mu.Lock()
	DPrintf("follower %d mu lock required", rf.me)
    defer rf.mu.Unlock()
    term, _ := rf.GetState()
    DPrintf("%d with term %d ask %d with term %d to append entries %v, rf.Log: %v", args.LeaderId, args.Term, rf.me, term, args.Entries, rf.Log)
    if args.Term < term {
        reply.Success = false
        reply.Term = term
        lastLogIndex, _ := rf.getLastLogIndexTerm()
        reply.ConflictIndex = lastLogIndex
        return
    }

    // the position of this heartbeat is important
    // since the process may not finish until end
    // but the heartbeat should be delivered whatever
    rf.entryAppended <- true
    if args.Term > term {
        rf.CurrentTerm = args.Term
        rf.VotedFor = -1
        rf.state = FOLLOWER
        rf.persist()
        DPrintf("%d convert to follower, due to append entries args term", rf.me)
    }

    reply.Term = rf.CurrentTerm // not term, same reason
    baseIndex := rf.Log[0].Index

    if args.PrevLogIndex > baseIndex {
        lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
        if lastLogIndex < args.PrevLogIndex {
            reply.ConflictIndex = lastLogIndex + 1
            reply.ConflictTerm = -1
            reply.Success = false
            DPrintf("log of %d with index %d term %d mismatch with log leader %d with index %d term %d", rf.me, lastLogIndex, lastLogTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
            return
        }
        if rf.Log[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm {
            reply.ConflictTerm = rf.Log[args.PrevLogIndex-baseIndex].Term
            prevLogIndex := args.PrevLogIndex
            for prevLogIndex >= baseIndex &&
              rf.Log[prevLogIndex-baseIndex].Term == reply.ConflictTerm {
                prevLogIndex--
            }
            reply.ConflictIndex = prevLogIndex + 1
            reply.Success = false
            DPrintf("log of %d with index %d term %d mismatch with log leader %d with index %d term %d", rf.me, lastLogIndex, lastLogTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
            return
        }
    }

    // since Log[baseIndex].Term will be compared with PreLogTerm
    if args.PrevLogIndex < baseIndex {
        // when there is no baseIndex, there is no such condition
        // here reply.Success should be false

        // but when a log is tructed. it must be committed
        // it is committed there, but it may not be committed here
        // this should be handled in sendAllAppendEntries

        // what is committed by me must have been committed by the master
    }

        lastLogIndex, _ := rf.getLastLogIndexTerm()
        for i := 0; i < len(args.Entries); i++ {
            // TODO: +1 or not, < or <=
            if args.PrevLogIndex+1+i-baseIndex <= 0 {
                continue
            }
            // TODO: whether this is fine
            if args.PrevLogIndex+1+i >= lastLogIndex+1 ||
              rf.Log[args.PrevLogIndex+1+i-baseIndex].Term != args.Entries[i].Term ||
              rf.Log[args.PrevLogIndex+1+i-baseIndex].Index != args.Entries[i].Index {
                rf.Log = append(rf.Log[:args.PrevLogIndex+1+i-baseIndex], args.Entries[i:]...)
                break
            }
        }
        rf.persist()

        reply.Success = true

    if args.LeaderCommit > rf.commitIndex {
        // should not compute with rf.getLastLogIndex()
        // since the follower's log maybe more than leader's log

        rf.commitIndex = args.PrevLogIndex + len(args.Entries)
        if args.LeaderCommit < rf.commitIndex {
            rf.commitIndex = args.LeaderCommit
        }
        rf.applyCommit <- true
        DPrintf("leader commit is %d, commitIndex of %d is now %d", args.LeaderCommit, rf.me, rf.commitIndex)
    }
    DPrintf("%d now have log %v", rf.me, rf.Log)
    return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    if !ok {
        DPrintf("append entries reply from %d is not ok", server)
        return false
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()

    term, _ := rf.GetState()


    if !reply.Success {
        if reply.Term > term {
            rf.CurrentTerm = reply.Term
            rf.VotedFor = -1
            rf.state = FOLLOWER
            rf.persist()
            DPrintf("%d convert to follower, due to append entries reply term", rf.me)
            // return timely to avoid mistake!
            return false
        }

        if args.Term != rf.CurrentTerm {
            return false
        }

        // cannot decrease nextIndex by 1 each time, we need to be faster
        rf.nextIndex[server] = reply.ConflictIndex
        DPrintf("nextIndex of leader %d for follower %d is now %d", rf.me, server, rf.nextIndex[server])
        // stupid me!
        return false
    }

    // I may be in a new term currently
    // be careful about the position!
    if args.Term != rf.CurrentTerm {
        return false
    }

    rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
    rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
    DPrintf("nextIndex of leader %d of follower %d now is %d", rf.me, server, rf.nextIndex[server])
    DPrintf("matchIndex of leader %d of follower %d now is %d", rf.me, server, rf.matchIndex[server])

    return true
}

func (rf *Raft) getPrevLogIndexTerm(server int) (int, int) {
    // the current log may not contain index [rf.nextIndex[server] - 1]
    baseIndex := rf.Log[0].Index
    i := len(rf.Log) - 1
    prevLogIndex := rf.Log[i].Index
    if rf.nextIndex[server] - 1 <= rf.Log[i].Index {
        prevLogIndex = rf.nextIndex[server] - 1
    }
    // prevLogTerm is the term of rd.Log with prevLogIndex
    // prevLogIndex may be less than baseIndex
    return prevLogIndex, rf.Log[prevLogIndex-baseIndex].Term
}

func (rf *Raft) sendAllAppendEntries() {
    // the reason for Persisit 2C: do not use append count like vote count
    // since there may be a terrible race condition

	rf.mu.Lock()
    defer rf.mu.Unlock()
	N := rf.commitIndex
	lastLogIndex, _ := rf.getLastLogIndexTerm()
    num := 0
    baseIndex := rf.Log[0].Index
	for i := rf.commitIndex + 1; i <= lastLogIndex ; i++ {
		num = 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i &&
        // this is what we mean by figure 8
        // we cannot simply leave it alone
              rf.Log[i-baseIndex].Term == rf.CurrentTerm {
				num++
			}
		}
		if num > len(rf.peers)/2 {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		DPrintf("commitIndex of leader %d is now %d with num %d", rf.me, rf.commitIndex, num)
		rf.applyCommit <- true
	}

    term, _ := rf.GetState()
    for server := 0; server < len(rf.peers); server++ {
        if rf.state != LEADER {
            return
        }
        if server == rf.me {
            continue
        }

        // nextIndex > baseIndex, prevLogIndex >= baseIndex,
        // only when follower can check the term of of prevLogIndex match or not
        if rf.nextIndex[server] > baseIndex {
            prevLogIndex, prevLogTerm := rf.getPrevLogIndexTerm(server)
            entries := rf.Log[prevLogIndex+1-baseIndex:]
            args := &AppendEntriesArgs{Term: term,
                                   LeaderId: rf.me,
                                   PrevLogIndex: prevLogIndex,
                                   PrevLogTerm: prevLogTerm,
                                   Entries: entries,
                                   LeaderCommit: rf.commitIndex}

            reply := &AppendEntriesReply{}
		    DPrintf("leader %d send append entries to follower %d", rf.me, server)
            go rf.sendAppendEntries(server, args, reply)
        } else {
                // if nextIndex <= baseIndex
				var args InstallSnapshotArgs
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.Log[0].Index
				args.LastIncludedTerm = rf.Log[0].Term
				args.Data = rf.persister.snapshot
				go func(server int,args InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, args, reply) // if ok, then update nextIndex
				}(server,args)
        }
    }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
    // command is really a int, in config.go

    // defer is necessary to pass Concurrent Starts 2B
    // since the index may change during the process
    rf.mu.Lock()
    defer rf.mu.Unlock()

	index, _ := rf.getLastLogIndexTerm()
    index ++
    term, isLeader := rf.GetState()

	// Your code here (2B).
    if !isLeader {
        // if not leader, return index -1
        return -1, term, isLeader
    }
    rf.Log = append(rf.Log, Entry{Command: command, Term: term, Index: index})
    rf.persist()
    // just wait for the next heart beat to append entries
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func electionTimeout() time.Duration {
    // in test_test.go, RaftElectionTimeout = 1000
    // the election timeout can not be too short,
    // for example, in Figure 8, there will be multiple rpc calls for a successful append
    // that is not relevant, as long as heartbeat is short enough
    return time.Millisecond * time.Duration(300+rand.Intn(200)) // elect a leader within five seconds
}

func heartbeatTimeout() time.Duration {
    // no more than 10 per second
    return time.Millisecond * time.Duration(60) // I think this is important
}

func (rf *Raft) GetRaftStateSize() int {
    return rf.persister.RaftStateSize()
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.Log[0].Index
	lastIndex, _ := rf.getLastLogIndexTerm()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		return
	}

	var newLogEntries []Entry

	newLogEntries = append(newLogEntries, Entry{Index: index, Term: rf.Log[index-baseIndex].Term})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.Log[i-baseIndex])
	}

	rf.Log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []Entry) []Entry {

	var newLogEntries []Entry
	newLogEntries = append(newLogEntries, Entry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == lastIncludedIndex && log[index].Term == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.Log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.Log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	go func() {
		rf.chanApply <- msg
	}()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs,reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	rf.entryAppended <- true
	rf.state = FOLLOWER
	rf.CurrentTerm = rf.CurrentTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.Log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.Log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()

	rf.chanApply <- msg
}

func (rf *Raft) sendInstallSnapshot(server int,args InstallSnapshotArgs,reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.state = FOLLOWER
			rf.VotedFor = -1
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    rf.CurrentTerm = 0
    rf.VotedFor = -1
    rf.state = FOLLOWER
    // since real Command is generated by rand.Int(), which is non-negative
    // but it is not so for the kvraft, where Command is op
    rf.Log = []Entry{Entry{Command: -1, Term: 0, Index: 0}}
    // buffer size makes it not block when send
    rf.leaderVoted = make(chan bool, 100)
    rf.leaderElected = make(chan bool, 100)
    rf.entryAppended = make(chan bool, 100)
    rf.applyCommit = make(chan bool, 100)
    rf.chanApply = applyCh

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    rf.readSnapshot(persister.ReadSnapshot())

    go func() {
        for {
            <-rf.applyCommit
            rf.mu.Lock()
            baseIndex := rf.Log[0].Index
            for rf.commitIndex > rf.lastApplied {
                msg := ApplyMsg{Index: rf.lastApplied+1,
                                Command: rf.Log[rf.lastApplied+1-baseIndex].Command} // TODO: why?
                applyCh <- msg
                rf.lastApplied += 1
            }
            rf.mu.Unlock()
        }
    }()

    go func() {
        for {
            switch rf.state {
                case FOLLOWER:
                    select{
                        case <-time.After(electionTimeout()):
                            //DPrintf("%d convert to candidate, due to election timeout", rf.me)
                            rf.state = CANDIDATE
                        // otherwise, the timer will reset automatically
                        case <- rf.entryAppended:
                        case <- rf.leaderVoted:
                    }
                case CANDIDATE:
                    rf.mu.Lock()
                    rf.voteCount = 1
                    rf.CurrentTerm += 1
                    rf.VotedFor = rf.me
                    rf.persist()
                    rf.mu.Unlock()
                    go rf.sendAllRequestVote()
                    select {
                        case <- time.After(electionTimeout()):
                        case <- rf.leaderElected:
                        case <- rf.entryAppended:
                    }
                case LEADER:
                    // no for loop here, since there is already a out loop
                    time.Sleep(heartbeatTimeout())
                    go rf.sendAllAppendEntries()
            }
        }
    }()

	return rf
}
