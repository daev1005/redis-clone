# Simple Redis Clone in Python

A Redis-compatible in-memory database built from scratch in Python. Supports replication, blocking reads, lists, streams, sorted sets, and pub/sub.

## Features

- Core Commands: PING, ECHO, SET, GET, INCR

- Lists: LPUSH, RPUSH, LPOP, BLPOP, LRANGE, LLEN

- Streams: XADD, XRANGE, XREAD

- Sorted Sets: ZADD, ZRANGE, ZRANK, ZCARD, ZSCORE, ZREM

- Pub/Sub: SUBSCRIBE, PUBLISH, UNSUBSCRIBE

- Replication: Implements REPLCONF, PSYNC, and WAIT

- Config & Introspection: CONFIG GET, INFO replication, KEYS

- Full RESP Protocol support

## Installation

### Clone the repository:

`git clone https://github.com/YOUR_USERNAME/redis-clone.git`
`cd redis-clone`

## Usage

### Start the server:

`python3 -m app.main`
- <img width="741" height="70" alt="image" src="https://github.com/user-attachments/assets/ad834818-7440-44e6-b6e7-625060a558aa" />


### Connect with redis-cli:

`redis-cli -p 6379`

## Commands Usage
### Basic Commands
- PING                      # -> +PONG
- ECHO "Hello"             # -> "Hello"
- SET key value [PX 1000]  # -> +OK
- GET key                  # -> "value"
- INCR key             # -> (integer) 1

### Lists
- LPUSH mylist "a"             # -> (integer) 1
- RPUSH mylist "b"             # -> (integer) 2
- LRANGE mylist 0 -1           # -> ["a", "b"]
- LLEN mylist                  # -> (integer) 2
- LPOP mylist                  # -> "a"
- BLPOP mylist 0               # -> ["mylist", "b"]  # Blocks if empty

### Streams
- XADD mystream * field value  # -> "1689851248173-0"
- XRANGE mystream - +          # -> list of entries
- XREAD STREAMS mystream 0     # -> list of new entries
- XREAD BLOCK ms STREAMS mysteam id  #->  Read entries from stream (blocking supported)

### Pub/Sub
- SUBSCRIBE channel            # -> "subscribe"
- PUBLISH channel "Hello"      # -> (integer) number of subscribers
- UNSUBSCRIBE channel          # -> "unsubscribe"

### Sorted Sets
- ZADD myzset 1 "one"          # -> (integer) 1
- ZRANGE myzset 0 -1           # -> ["one"]
- ZCARD myzset                 # -> (integer) 1
- ZRANK myzset "one"           # -> (integer) 0
- ZSCORE myzset "one"          # -> "1"
- ZREM myzset "one"            # -> (integer) 1

### Other Commands
- TYPE key                     # -> "string" | "list" | "stream" | "zset"
- KEYS *                       # -> ["key1", "key2"]
- CONFIG GET dir|dbfilename    # -> ["dir", "/tmp" | "dbfilename", "dump.rdb"]
- INFO replication             # -> replication info
- WAIT numreplicas timeout     # -> Wait for replicas to acknowledge

### Basic Commands Demo:
- <img width="335" height="200" alt="Screenshot 2025-08-31 155855" src="https://github.com/user-attachments/assets/55b77739-2b82-4f3d-95e0-f42b902d27d6" />

## Replication Setup

### Start the primary:

`python3 app/main.py --port 6379`


### Start a replica:

`python3 app/main.py --port 6380 --replicaof localhost 6379`


### Replication supports:

- PSYNC for initial data sync

- REPLCONF for replica handshake

- WAIT to ensure writes propagate to replicas

## Tech Stack

- Language: Python 3

- Networking: TCP Sockets

- Protocol: RESP (Redis Serialization Protocol)

## Roadmap

 Add RDB/AOF persistence

 Improve async I/O for concurrency

 Support pattern-based pub/sub

## Acknowledgment

Inspired by the Redis architecture and built to deeply understand data structures, networking, and replication internals.
