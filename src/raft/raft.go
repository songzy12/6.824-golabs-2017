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
// candidate may become follwer right in the middle
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
    currentTerm     int     // init to 0
    votedFor        int     // init to -1
    log             []Entry // first index is 1

    // even I vote for myself, I may not think myself is a leader
    state           int

    commitIndex     int         // init to 0
    lastApplied     int         // init to 0
    entryAppended   chan bool   // leader told me to append entry
    leaderVoted     chan bool   // candidate told me to vote
    leaderElected   chan bool   // I am selected as leader
    applyCommit     chan bool

    nextIndex       []int   // init to leader last log index + 1 
    matchIndex      []int   // init to 0

    voteCount       int
    appendCount     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
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
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
     e.Encode(rf.log)
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
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
    d.Decode(&rf.log)
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
    index := len(rf.log) - 1
    return index, rf.log[index].Term
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
    DPrintf("%d with term %d request %d with term %d to vote", args.CandidateId, args.Term, rf.me, rf.currentTerm)

    // eliminate all the possible race
    term, _ := rf.GetState()
    if args.Term < term {
        reply.Term = term
        reply.VoteGranted = false
        return
    }

    if args.Term > term {
        rf.mu.Lock()
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.state = FOLLOWER
        rf.persist()
        rf.mu.Unlock()
        DPrintf("%d convert to follower, due to request vote args term", rf.me)
        // convert 
    }

    reply.Term = term
    lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()

    if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
      moreUpToDate(args.LastLogTerm, args.LastLogIndex,
                   lastLogTerm, lastLogIndex) {
        rf.mu.Lock()
        rf.votedFor = args.CandidateId
        rf.persist()
        rf.state = FOLLOWER
        rf.mu.Unlock()
        reply.VoteGranted = true
        rf.leaderVoted <- true
        DPrintf("%d convert to follower, due to vote granted", rf.me)
        return
    }

    DPrintf("%d has last log term %d index %d, while candidate %d only has log term %d log index %d", rf.me, lastLogTerm, lastLogIndex, args.CandidateId, args.LastLogTerm, args.LastLogIndex)

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
    term, _ := rf.GetState()
    if reply.Term > term {
        rf.mu.Lock()
        rf.currentTerm = reply.Term;
        rf.votedFor = -1
        rf.state = FOLLOWER
        rf.persist()
        rf.mu.Unlock()
        DPrintf("%d convert to follower, due to request vote reply term", rf.me)
        return
    }
    // I may be in a new term currently 
    if args.Term != rf.currentTerm {
        return
    }
    if reply.VoteGranted {
        rf.voteCount += 1
        if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
            rf.mu.Lock()
            rf.state = LEADER
            DPrintf("%d convert to leader, due to majority vote", rf.me)
            lastLogIndex, _ := rf.getLastLogIndexTerm()
            for server := 0; server < len(rf.peers); server++ {
                rf.nextIndex[server] = lastLogIndex + 1
                rf.matchIndex[server] = 0
            }
            rf.mu.Unlock()

            rf.leaderElected <- true
            go rf.sendAllAppendEntries()
        }
    }
}

func (rf *Raft) sendAllRequestVote() {
    rf.mu.Lock()
    rf.voteCount = 1
    lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
    args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
    rf.mu.Unlock()
    // use select and channel 
    for server := 0; server < len(rf.peers); server++ {
        rf.mu.Lock()
        state := rf.state
        rf.mu.Unlock()
        if state != CANDIDATE {
            return
        }
        if server == rf.me {
            continue
        }
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
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term, _ := rf.GetState()
    DPrintf("%d with term %d ask %d with term %d to append entries %v, rf.log: %v", args.LeaderId, args.Term, rf.me, term, args.Entries, rf.log)
    if args.Term < term {
        reply.Success = false
        reply.Term = term
        return
    }

    if args.Term > term {
        rf.currentTerm = args.Term
        rf.votedFor = args.LeaderId
        rf.state = FOLLOWER
        rf.persist()
        DPrintf("%d convert to follower, due to append entries args term", rf.me)
    }

    reply.Term = term

    if args.PrevLogIndex > 0 {
        lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
        if lastLogIndex < args.PrevLogIndex {
            reply.ConflictIndex = lastLogIndex
            reply.ConflictTerm = -1
            reply.Success = false
            DPrintf("log of %d with index %d term %d mismatch with log leader %d with index %d term %d", rf.me, lastLogIndex, lastLogTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
            return
        }
        if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
            reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
            for args.PrevLogIndex > 0 &&
              rf.log[args.PrevLogIndex - 1].Term == reply.ConflictTerm {
                args.PrevLogIndex--
            }
            reply.ConflictIndex = args.PrevLogIndex
            reply.Success = false
            DPrintf("log of %d with index %d term %d mismatch with log leader %d with index %d term %d", rf.me, lastLogIndex, lastLogTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
            return
        }
    }

    for i := 0; i < len(args.Entries); i++ {
        if args.PrevLogIndex+1+i >= len(rf.log) ||
          rf.log[args.PrevLogIndex+1+i] != args.Entries[i] {
            rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
            break
        }
    }
    rf.persist()

    reply.Success = true

    if args.LeaderCommit > rf.commitIndex {
        // rf.commitIndex = min(rf.getLastLogIndex(), args.LeaderCommit)

        // should not compute with rf.getLastLogIndex()
        // since the follower's log maybe more than leader's log
        rf.commitIndex = args.PrevLogIndex + len(args.Entries)
        if args.LeaderCommit < rf.commitIndex {
            rf.commitIndex = args.LeaderCommit
        }
        rf.applyCommit <- true
        DPrintf("leader commit is %d, commitIndex of %d is now %d", args.LeaderCommit, rf.me, rf.commitIndex)
    }
    DPrintf("%d now have log %v", rf.me, rf.log)
    rf.entryAppended <- true
    return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    if !ok {
        DPrintf("%d not ok", server)
        return false
    }

    term, _ := rf.GetState()


    if !reply.Success {
        if reply.Term > term {
            rf.mu.Lock()
            rf.currentTerm = reply.Term
            rf.votedFor = -1
            rf.state = FOLLOWER
            rf.persist()
            rf.mu.Unlock()
            DPrintf("%d convert to follower, due to append entries reply term", rf.me)
            // return timely to avoid mistake!
            return false
        }
        // I may be in a new term currently
        if args.Term != rf.currentTerm {
            return false
        }
        // cannot decrease nextIndex by 1 each time, we need to be faster
        rf.mu.Lock()
        if reply.ConflictTerm != -1 {
            i, _ := rf.getLastLogIndexTerm()
            for i > 0 && rf.log[i].Term != reply.ConflictTerm {
                i--
            }
            if i > 0 {
                rf.nextIndex[server] = i + 1
            } else if reply.ConflictIndex > 0 {
                rf.nextIndex[server] = reply.ConflictIndex
            }
        } else if reply.ConflictIndex > 0 {
        // you not simply write a else here 
            rf.nextIndex[server] = reply.ConflictIndex
        }
        rf.mu.Unlock()
        DPrintf("nextIndex of leader %d for follower %d is now %d", rf.me, server, rf.nextIndex[server])
        // stupid me!
        return false
    }

    rf.mu.Lock()
    rf.appendCount += 1
    rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
    DPrintf("nextIndex of leader %d of follower %d now is %d", rf.me, server, rf.nextIndex[server])
    rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
    rf.mu.Unlock()

    if rf.state == LEADER && reply.Term == rf.currentTerm &&
      rf.appendCount > len(rf.peers) / 2 {
        // this is what we mean by figure 8
        // we cannot simply leave it alone
        i := args.PrevLogIndex + len(args.Entries)
        rf.mu.Lock()
        for i > 0 && rf.log[i].Term != rf.currentTerm {
            i--
        }

        rf.commitIndex = i
        DPrintf("%d of %d appended, commitIndex of %d is now %d", rf.appendCount, len(rf.peers), rf.me, rf.commitIndex)
        rf.mu.Unlock()
        rf.applyCommit <- true
    }
    return true
}

func (rf *Raft) getPrevLogIndexTerm(server int) (int, int) {
    // the current log may not contain index [rf.nextIndex[server] - 1]
    index := len(rf.log) - 1
    if rf.nextIndex[server] - 1 <= index {
        index = rf.nextIndex[server] - 1
    }
    return index, rf.log[index].Term
}

func (rf *Raft) sendAllAppendEntries() {
    rf.appendCount = 1
    term, _ := rf.GetState()
    for server := 0; server < len(rf.peers); server++ {
        if rf.state != LEADER {
            return
        }
        if server == rf.me {
            continue
        }

        rf.mu.Lock()
        prevLogIndex, prevLogTerm := rf.getPrevLogIndexTerm(server)
        entries := rf.log[prevLogIndex+1:]
        args := &AppendEntriesArgs{Term: term,
                               LeaderId: rf.me,
                               PrevLogIndex: prevLogIndex,
                               PrevLogTerm: prevLogTerm,
                               Entries: entries,
                               LeaderCommit: rf.commitIndex}
        rf.mu.Unlock()

        reply := &AppendEntriesReply{}
        go rf.sendAppendEntries(server, args, reply)
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
	index, _ := rf.getLastLogIndexTerm()
    index ++
    term, isLeader := rf.GetState()

	// Your code here (2B).
    if !isLeader {
        return index, term, isLeader
    }
    rf.mu.Lock()
    rf.log = append(rf.log, Entry{Command: command, Term: term})
    rf.persist()
    rf.mu.Unlock()
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
    return time.Millisecond * time.Duration(300+rand.Intn(200))
}

func heartbeatTimeout() time.Duration {
    // no more than 10 per second
    return time.Millisecond * time.Duration(120)
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
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.state = FOLLOWER
    // since real Command is generated by rand.Int(), which is non-negative
    rf.log = []Entry{Entry{Command: -1, Term: 0}}
    rf.leaderVoted = make(chan bool)
    rf.leaderElected = make(chan bool)
    rf.entryAppended = make(chan bool)
    rf.applyCommit = make(chan bool)

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go func() {
        for {
            <-rf.applyCommit
            rf.mu.Lock()
            for rf.commitIndex > rf.lastApplied {
                rf.lastApplied += 1
                msg := ApplyMsg{Index: rf.lastApplied,
                                Command: rf.log[rf.lastApplied].Command}
                applyCh <- msg
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
                            DPrintf("%d convert to candidate, due to election timeout", rf.me)
                            rf.state = CANDIDATE
                        // otherwise, the timer will reset automatically
                        case <- rf.entryAppended:
                        case <- rf.leaderVoted:
                    }
                case CANDIDATE:
                    rf.mu.Lock()
                    rf.currentTerm += 1
                    rf.votedFor = rf.me
                    rf.persist()
                    rf.mu.Unlock()
                    go rf.sendAllRequestVote()
                    select {
                        case <- time.After(electionTimeout()):
                        case <- rf.leaderElected:
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
