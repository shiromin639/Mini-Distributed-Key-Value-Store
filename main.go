package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"raft-kv/kv"
)

func main() {
	id := flag.Int("id", 1, "ID of this node (1, 2, 3)")
	flag.Parse()
	if _, ok := kv.PeerMap[*id]; !ok {
		log.Fatal("Invalid ID. Must be 1, 2, or 3")
	}
	printBanner(*id)
	service := kv.NewService(*id)
	service.Start()
}

func printBanner(id int) {
	fmt.Print("\033[H\033[2J") // Clear terminal code
	fmt.Printf("==========================================\n")
	fmt.Printf("   RAFT DISTRIBUTED DB - NODE %d          \n", id)
	fmt.Printf("==========================================\n")
	fmt.Printf("   HTTP API: %s   |   RPC: %s\n", kv.HttpMap[id], kv.PeerMap[id])
	fmt.Printf("   PID: %d\n", os.Getpid())
	fmt.Printf("==========================================\n\n")
}
