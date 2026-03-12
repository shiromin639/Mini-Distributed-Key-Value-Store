# Distributed Key-Value Store

A distributed key-value store built in Go, using the Raft consensus algorithm to keep data consistent across 3 nodes.

## Requirements

- Go 1.18+

## Run

Open 3 terminals, one for each node:

```bash
# Terminal 1
go run main.go -id 1

# Terminal 2
go run main.go -id 2

# Terminal 3
go run main.go -id 3
```

The nodes will connect to each other automatically and elect a leader.

## Ports

| Node | HTTP API | Raft RPC |
|------|----------|----------|
| 1    | :8001    | :3001    |
| 2    | :8002    | :3002    |
| 3    | :8003    | :3003    |

## Usage

**Set a value** (must send to the leader)
```bash
curl "http://localhost:8001/set?key=name&val=long"
```

**Get a value** (can read from any node)
```bash
curl "http://localhost:8001/get?key=name"
```

**Check if a node is the leader**
```bash
curl http://localhost:8001/debug
curl http://localhost:8002/debug
curl http://localhost:8003/debug
```

## Tests

```bash
cd raft
go test ./...
```
