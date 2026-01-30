package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

const Debugr = 1

type CommitEntry struct {
	Command any
	Index   int
	Term    int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
	Dead
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type LogEntry struct {
	Command any
	Term    int
}

type Raft struct {
	mu                   sync.Mutex
	id                   int
	peerIds              []int
	server               *Server
	storage              Storage
	commitChan           chan<- CommitEntry
	newCommitReadyChan   chan struct{}
	newCommitReadyChanWg sync.WaitGroup
	triggerAEChan        chan struct{}
	currentTerm          int
	votedFor             int
	log                  []LogEntry
	commitIndex          int
	lastApplied          int
	state                RaftState
	electionResetEvent   time.Time
	nextIndex            map[int]int
	matchIndex           map[int]int
}

func NewRaft(id int, peerIds []int, server *Server, storage Storage, ready <-chan any, commitChan chan<- CommitEntry) *Raft {
	r := &Raft{
		id:                 id,
		peerIds:            peerIds,
		server:             server,
		storage:            storage,
		commitChan:         commitChan,
		newCommitReadyChan: make(chan struct{}, 16),
		triggerAEChan:      make(chan struct{}, 1),
		state:              Follower,
		votedFor:           -1,
		commitIndex:        -1,
		lastApplied:        -1,
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
	}
	if r.storage.HasData() {
		r.restoreFromStorage()
	}

	go func() {
		<-ready
		r.mu.Lock()
		r.electionResetEvent = time.Now()
		r.mu.Unlock()
		r.runElectionTimer()
	}()

	r.newCommitReadyChanWg.Add(1)
	go r.commitChanSender()
	return r
}

func (r *Raft) Report() (id int, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.id, r.currentTerm, r.state == Leader
}

func (r *Raft) Submit(command any) int {
	r.mu.Lock()
	r.dlog("Submit received by %v: %v", r.state, command)
	if r.state == Leader {
		submitIndex := len(r.log)
		r.log = append(r.log, LogEntry{Command: command, Term: r.currentTerm})
		r.persistToStorage()
		r.dlog("... log=%v", r.log)
		r.mu.Unlock()
		r.triggerAEChan <- struct{}{}
		return submitIndex
	}

	r.mu.Unlock()
	return -1
}

func (r *Raft) Stop() {
	r.dlog("CM.Stop called")
	r.mu.Lock()
	r.state = Dead
	r.mu.Unlock()
	r.dlog("becomes Dead")
	close(r.newCommitReadyChan)
	r.newCommitReadyChanWg.Wait()
}

func (r *Raft) restoreFromStorage() {
	if termData, found := r.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&r.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := r.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&r.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := r.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&r.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

func (r *Raft) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(r.currentTerm); err != nil {
		log.Fatal(err)
	}
	r.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(r.votedFor); err != nil {
		log.Fatal(err)
	}
	r.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(r.log); err != nil {
		log.Fatal(err)
	}
	r.storage.Set("log", logData.Bytes())
}

func (r *Raft) dlog(format string, args ...any) {
	if Debugr > 0 {
		msg := fmt.Sprintf(format, args...)
		if strings.Contains(msg, "AppendEntries") && strings.Contains(msg, "Entries:[]") {
			return
		}
		if strings.Contains(msg, "becomes Leader") {
			log.Printf("\033[32m[%d] %s\033[0m", r.id, msg) // Green
		} else if strings.Contains(msg, "becomes Candidate") || strings.Contains(msg, "election") {
			log.Printf("\033[33m[%d] %s\033[0m", r.id, msg) // Yellow
		} else if strings.Contains(msg, "term out of date") {
			log.Printf("\033[31m[%d] %s\033[0m", r.id, msg) // Red
		} else {
			log.Printf("[%d] %s", r.id, msg)
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()
	r.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, r.currentTerm, r.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > r.currentTerm {
		r.dlog("... term out of date in RequestVote")
		r.becomeFollower(args.Term)
	}

	if r.currentTerm == args.Term &&
		(r.votedFor == -1 || r.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		r.votedFor = args.CandidateId
		r.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = r.currentTerm
	r.persistToStorage()
	r.dlog("... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Dead {
		return nil
	}
	r.dlog("AppendEntries: %+v", args)

	if args.Term > r.currentTerm {
		r.dlog("... term out of date in AppendEntries")
		r.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == r.currentTerm {
		if r.state != Follower {
			r.becomeFollower(args.Term)
		}
		r.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(r.log) && args.PrevLogTerm == r.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(r.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if r.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex < len(args.Entries) {
				r.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				r.log = append(r.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				r.dlog("... log is now: %v", r.log)
			}

			if args.LeaderCommit > r.commitIndex {
				r.commitIndex = min(args.LeaderCommit, len(r.log)-1)
				r.dlog("... setting commitIndex=%d", r.commitIndex)
				r.newCommitReadyChan <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(r.log) {
				reply.ConflictIndex = len(r.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = r.log[args.PrevLogIndex].Term
				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if r.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = r.currentTerm
	r.persistToStorage()
	return nil
}

func (r *Raft) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (r *Raft) runElectionTimer() {
	timeoutDuration := r.electionTimeout()
	r.mu.Lock()
	termStarted := r.currentTerm
	r.mu.Unlock()
	r.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		r.mu.Lock()
		if r.state != Candidate && r.state != Follower {
			r.dlog("in election timer state=%s, bailing out", r.state)
			r.mu.Unlock()
			return
		}

		if termStarted != r.currentTerm {
			r.dlog("in election timer term changed from %d to %d, bailing out", termStarted, r.currentTerm)
			r.mu.Unlock()
			return
		}

		if elapsed := time.Since(r.electionResetEvent); elapsed >= timeoutDuration {
			r.startElection()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startElection() {
	r.state = Candidate
	r.currentTerm += 1
	savedCurrentTerm := r.currentTerm
	r.electionResetEvent = time.Now()
	r.votedFor = r.id
	r.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, r.log)

	votesReceived := 1

	for _, peerId := range r.peerIds {
		go func() {
			r.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := r.lastLogIndexAndTerm()
			r.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  r.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			r.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			if err := r.server.Call(peerId, "Raft.RequestVote", args, &reply); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.dlog("received RequestVoteReply %+v", reply)

				if r.state != Candidate {
					r.dlog("while waiting for reply, state = %v", r.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					r.dlog("term out of date in RequestVoteReply")
					r.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(r.peerIds)+1 {
							// Won the election!
							r.dlog("wins election with %d votes", votesReceived)
							r.startLeader()
							return
						}
					}
				}
			}
		}()
	}

	go r.runElectionTimer()
}

func (r *Raft) becomeFollower(term int) {
	r.dlog("becomes Follower with term=%d; log=%v", term, r.log)
	r.state = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.electionResetEvent = time.Now()

	go r.runElectionTimer()
}

func (r *Raft) startLeader() {
	r.state = Leader

	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = len(r.log)
		r.matchIndex[peerId] = -1
	}
	r.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", r.currentTerm, r.nextIndex, r.matchIndex, r.log)
	go func(heartbeatTimeout time.Duration) {
		r.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-r.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				r.mu.Lock()
				if r.state != Leader {
					r.mu.Unlock()
					return
				}
				r.mu.Unlock()
				r.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

func (r *Raft) leaderSendAEs() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	for _, peerId := range r.peerIds {
		go func() {
			r.mu.Lock()
			ni := r.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = r.log[prevLogIndex].Term
			}
			entries := r.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     r.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: r.commitIndex,
			}
			r.mu.Unlock()
			r.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := r.server.Call(peerId, "Raft.AppendEntries", args, &reply); err == nil {
				r.mu.Lock()
				if reply.Term > r.currentTerm {
					r.dlog("term out of date in heartbeat reply")
					r.becomeFollower(reply.Term)
					r.mu.Unlock()
					return
				}

				if r.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						r.nextIndex[peerId] = ni + len(entries)
						r.matchIndex[peerId] = r.nextIndex[peerId] - 1

						savedCommitIndex := r.commitIndex
						for i := r.commitIndex + 1; i < len(r.log); i++ {
							if r.log[i].Term == r.currentTerm {
								matchCount := 1
								for _, peerId := range r.peerIds {
									if r.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(r.peerIds)+1 {
									r.commitIndex = i
								}
							}
						}
						// r.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, r.nextIndex, r.matchIndex, r.commitIndex)
						if r.commitIndex != savedCommitIndex {
							r.dlog("leader sets commitIndex := %d", r.commitIndex)
							r.mu.Unlock()
							r.newCommitReadyChan <- struct{}{}
							r.triggerAEChan <- struct{}{}
						} else {
							r.mu.Unlock()
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(r.log) - 1; i >= 0; i-- {
								if r.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								r.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								r.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							r.nextIndex[peerId] = reply.ConflictIndex
						}
						r.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
						r.mu.Unlock()
					}
				} else {
					r.mu.Unlock()
				}
			}
		}()
	}
}

func (r *Raft) lastLogIndexAndTerm() (int, int) {
	if len(r.log) > 0 {
		lastIndex := len(r.log) - 1
		return lastIndex, r.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (r *Raft) commitChanSender() {
	defer r.newCommitReadyChanWg.Done()

	for range r.newCommitReadyChan {
		r.mu.Lock()
		savedTerm := r.currentTerm
		savedLastApplied := r.lastApplied
		var entries []LogEntry
		if r.commitIndex > r.lastApplied {
			entries = r.log[r.lastApplied+1 : r.commitIndex+1]
			r.lastApplied = r.commitIndex
		}
		r.mu.Unlock()
		r.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			r.dlog("send on commitchan i=%v, entry=%v", i, entry)
			r.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	r.dlog("commitChanSender done")
}
