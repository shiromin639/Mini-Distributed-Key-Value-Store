package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Server struct {
	mu          sync.Mutex
	serverId    int
	peerIds     []int
	r           *Raft
	storage     Storage
	rpcProxy    *RPCProxy
	rpcServer   *rpc.Server
	listener    net.Listener
	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client
	ready       <-chan any
	quit        chan any
	wg          sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, storage Storage, ready <-chan any, commitChan chan<- CommitEntry) *Server {
	return &Server{
		serverId:    serverId,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		storage:     storage,
		ready:       ready,
		commitChan:  commitChan,
		quit:        make(chan any),
	}
}

func (s *Server) Serve(addr string) {
	s.mu.Lock()
	s.r = NewRaft(s.serverId, s.peerIds, s, s.storage, s.ready, s.commitChan)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = NewProxy(s.r)
	s.rpcServer.RegisterName("Raft", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) Submit(command any) int {
	return s.r.Submit(command)
}
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.r.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		err := s.rpcProxy.Call(peer, serviceMethod, args, reply)
		if err != nil {
			s.mu.Lock()
			s.peerClients[id] = nil
			s.mu.Unlock()
		}
		return err
	}
}

func (s *Server) IsLeader() bool {
	_, _, isLeader := s.r.Report()
	return isLeader
}

func (s *Server) Proxy() *RPCProxy {
	return s.rpcProxy
}

type RPCProxy struct {
	mu                 sync.Mutex
	r                  *Raft
	numCallsBeforeDrop int
}

func NewProxy(r *Raft) *RPCProxy {
	return &RPCProxy{
		r:                  r,
		numCallsBeforeDrop: -1,
	}
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.r.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.r.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.r.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.r.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.r.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.r.AppendEntries(args, reply)
}

func (rpp *RPCProxy) Call(peer *rpc.Client, method string, args any, reply any) error {
	rpp.mu.Lock()
	if rpp.numCallsBeforeDrop == 0 {
		rpp.mu.Unlock()
		rpp.r.dlog("drop Call %s: %v", method, args)
		return fmt.Errorf("RPC failed")
	} else {
		if rpp.numCallsBeforeDrop > 0 {
			rpp.numCallsBeforeDrop--
		}
		rpp.mu.Unlock()
		return peer.Call(method, args, reply)
	}
}

func (rpp *RPCProxy) DropCallsAfterN(n int) {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = n
}

func (rpp *RPCProxy) DontDropCalls() {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = -1
}
