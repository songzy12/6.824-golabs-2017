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

import "sync"
import "time"
import "labrpc"
import "math/rand"

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
    leaderElected  chan bool   // I am selected as leader

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
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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

func (rf *Raft) getLastLogTerm() int {
    return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) getLastLogIndex() int {
    return len(rf.log) - 1
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
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = FOLLOWER
        DPrintf("%d convert to follower, due to request vote args term", rf.me)
        // convert 
        rf.votedFor = -1
    }

    reply.Term = rf.currentTerm

    if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
      moreUpToDate(args.LastLogTerm, args.LastLogIndex,
                   rf.getLastLogTerm(), rf.getLastLogIndex()) {
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
        rf.state = FOLLOWER
        DPrintf("%d convert to follower, due to vote granted", rf.me)
        rf.leaderVoted <- true
        return
    }

    DPrintf("%d has last log term %d index %d, while candidate %d only has log term %d log index %d", rf.me, rf.getLastLogTerm(), rf.getLastLogIndex(), args.CandidateId, args.LastLogTerm, args.LastLogIndex)
    // TODO: what if not as up-to-date
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
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term;
        rf.votedFor = -1
        rf.state = FOLLOWER
        DPrintf("%d convert to follower, due to request vote reply term", rf.me)
        return
    }
    if reply.VoteGranted {
        rf.voteCount += 1
        if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
            rf.state = LEADER
            DPrintf("%d convert to leader, due to majority vote", rf.me)
            for server := 0; server < len(rf.peers); server++ {
                rf.nextIndex[server] = rf.getLastLogIndex() + 1
                rf.matchIndex[server] = 0
            }

            rf.leaderElected <- true
            go rf.sendAllAppendEntries()
        }
    }
}

func (rf *Raft) sendAllRequestVote() {
    rf.voteCount = 1
    // use select and channel 
    for server := 0; server < len(rf.peers); server++ {
        if rf.state != CANDIDATE {
            return
        }
        if server == rf.me {
            continue
        }
        args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.getLastLogIndex(), LastLogTerm: rf.getLastLogTerm()}
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
    DPrintf("%d with term %d ask %d with term %d to append entries %v, rf.log: %v", args.LeaderId, args.Term, rf.me, rf.currentTerm, args.Entries, rf.log)
    if args.Term < rf.currentTerm {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = FOLLOWER
        DPrintf("%d convert to follower, due to append entries args term", rf.me)
        rf.votedFor = args.LeaderId
    }

    reply.Term = rf.currentTerm

    if args.PrevLogIndex > 0 {
        if rf.getLastLogIndex() < args.PrevLogIndex {
            reply.ConflictIndex = rf.getLastLogIndex()
            reply.ConflictTerm = -1
            reply.Success = false
            DPrintf("log of %d with index %d term %d mismatch with log leader %d with index %d term %d", rf.me, rf.getLastLogIndex(), rf.getLastLogTerm(), args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
            return
        }
        if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
            reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
            for args.PrevLogIndex - 1 > 0 &&
              rf.log[args.PrevLogIndex - 1].Term == reply.ConflictTerm {
                args.PrevLogIndex--
            }
            reply.ConflictIndex = args.PrevLogIndex
            reply.Success = false
            DPrintf("log of %d with index %d term %d mismatch with log leader %d with index %d term %d", rf.me, rf.getLastLogIndex(), rf.getLastLogTerm(), args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
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
    reply.Success = true

    if args.LeaderCommit > rf.commitIndex {
        // rf.commitIndex = min(rf.getLastLogIndex(), args.LeaderCommit)
        rf.commitIndex = rf.getLastLogIndex()
        if args.LeaderCommit < rf.getLastLogIndex() {
            rf.commitIndex = args.LeaderCommit
        }
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
    if !reply.Success {
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            DPrintf("%d convert to follower, due to append entries reply term", rf.me)
            rf.state = FOLLOWER
            rf.votedFor = -1
        }
        // cannot decrease nextIndex by 1 each time, we need to be faster
        if reply.ConflictTerm != -1 {
            i := rf.getLastLogIndex()
            for i > 0 && rf.log[i].Term != reply.ConflictTerm {
                i--
            }
            if i > 0 {
                rf.nextIndex[server] = i + 1
            } else {
                rf.nextIndex[server] = reply.ConflictIndex
            }
        } else {
            rf.nextIndex[server] = reply.ConflictIndex
        }
        DPrintf("nextIndex of leader %d for follower %d is now %d", rf.me, server, rf.nextIndex[server])
        // stupid me!
        return false
    }
    rf.appendCount += 1
    rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
    DPrintf("nextIndex of leader %d of follower %d now is %d", rf.me, server, rf.nextIndex[server])
    rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
    if rf.state == LEADER && reply.Term == rf.currentTerm &&
      rf.appendCount > len(rf.peers) / 2 {
        if args.PrevLogIndex + len(args.Entries) > rf.commitIndex {
            rf.commitIndex = args.PrevLogIndex + len(args.Entries)
        }
        DPrintf("%d of %d appended, commitIndex of %d is now %d", rf.appendCount, len(rf.peers), rf.me, rf.commitIndex)
    }
    return true
}

func (rf *Raft) getPrevLogIndex(server int) int {
    return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int {
    return rf.log[rf.getPrevLogIndex(server)].Term
}

func (rf *Raft) sendAllAppendEntries() {
    rf.appendCount = 1
    for server := 0; server < len(rf.peers); server++ {
        if rf.state != LEADER {
            return
        }
        if server == rf.me {
            continue
        }

        entries := rf.log[rf.nextIndex[server]:]

        args := &AppendEntriesArgs{Term: rf.currentTerm,
                                   LeaderId: rf.me,
                                   PrevLogIndex: rf.getPrevLogIndex(server),
                                   PrevLogTerm: rf.getPrevLogTerm(server),
                                   Entries: entries,
                                   LeaderCommit: rf.commitIndex}
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
	index := rf.getLastLogIndex() + 1
    term, isLeader := rf.GetState()

	// Your code here (2B).
    if !isLeader {
        return index, term, isLeader
    }
    rf.log = append(rf.log, Entry{Command: command, Term: rf.currentTerm})
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
    return time.Millisecond * time.Duration(300+rand.Intn(150))
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

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go func() {
        for {
            //repeat
            for rf.commitIndex > rf.lastApplied {
                rf.lastApplied += 1
                msg := ApplyMsg{Index: rf.lastApplied,
                               Command: rf.log[rf.lastApplied].Command}
                applyCh <- msg
            }

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
                    rf.currentTerm += 1
                    rf.votedFor = rf.me
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
