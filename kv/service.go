package kv

import (
	"fmt"
	"log"
	"net/http"
	"raft-kv/raft"
	"strings"
	"time"
)

var PeerMap = map[int]string{
	1: ":3001",
	2: ":3002",
	3: ":3003",
}

var HttpMap = map[int]string{
	1: ":8001",
	2: ":8002",
	3: ":8003",
}

type Service struct {
	id         int
	apiPort    string
	raftServer *raft.Server
	store      *KVStore
	commitChan chan raft.CommitEntry
	readyChan  chan any
}

func NewService(id int) *Service {
	return &Service{
		id:         id,
		apiPort:    HttpMap[id],
		store:      NewKVStore(),
		commitChan: make(chan raft.CommitEntry),
		readyChan:  make(chan any),
	}
}

func (s *Service) Start() {
	storage := raft.NewMapStorage()

	var peerIds []int
	for pId := range PeerMap {
		if pId != s.id {
			peerIds = append(peerIds, pId)
		}
	}

	s.raftServer = raft.NewServer(s.id, peerIds, storage, s.readyChan, s.commitChan)
	s.raftServer.Serve(PeerMap[s.id])
	s.connectPeers(peerIds)

	go s.processCommits()

	s.serveHTTP()
}

func (s *Service) connectPeers(peerIds []int) {
	go func() {
		for _, peerId := range peerIds {
			addr := PeerMap[peerId]

			go func(pId int, pAddr string) {
				for {
					err := s.raftServer.ConnectToPeer(pId, &SimpleAddr{pAddr})
					if err != nil {
						time.Sleep(1 * time.Second)
					} else {
						time.Sleep(3 * time.Second)
					}
				}
			}(peerId, addr)
		}
		close(s.readyChan)
	}()
}

func (s *Service) processCommits() {
	for entry := range s.commitChan {
		cmd, ok := entry.Command.(string)
		if !ok {
			continue
		}
		parts := strings.SplitN(cmd, ":", 2)
		if len(parts) == 2 {
			s.store.Set(parts[0], parts[1])
		}
	}
}

func (s *Service) serveHTTP() {
	mux := http.NewServeMux()

	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		cmd := key + ":" + val

		log.Printf("User requested SET %s=%s", key, val)
		idx := s.raftServer.Submit(cmd)

		if idx == -1 {
			http.Error(w, "I am not the Leader", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "Ok! Submitted at index %d\n", idx)
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := s.store.Get(key)
		fmt.Fprintf(w, "%s\n", val)
	})

	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		if s.raftServer.IsLeader() {
			fmt.Fprint(w, "I am LEADER\n")
		} else {
			fmt.Fprint(w, "I am FOLLOWER\n")
		}
	})

	log.Printf("HTTP User API listening on %s", s.apiPort)
	log.Fatal(http.ListenAndServe(s.apiPort, mux))
}

type SimpleAddr struct {
	Addr string
}

func (s *SimpleAddr) Network() string { return "tcp" }
func (s *SimpleAddr) String() string  { return s.Addr }
