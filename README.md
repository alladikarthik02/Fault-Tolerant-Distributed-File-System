# Fault Tolerant Distributed File System

A Java/Spring Boot distributed file system with Reed-Solomon erasure coding, journaled metadata, lease-based writes, and a 3-node Docker cluster. Files are chunked, erasure-coded into 9 shards each, and spread across storage nodes — the system survives the loss of any single node with zero client-visible errors.

## Key features

* **Reed-Solomon 6+3 erasure coding** — each 4 MB chunk is split into 6 data + 3 parity shards. Any 6 of 9 can reconstruct the original. **1.5× storage overhead** vs 3× for full replication, with the same fault tolerance as 4× replication.
* **Rack-aware shard placement** — shards are distributed round-robin across nodes so losing any single node still leaves 6 surviving shards per chunk.
* **Write-ahead journaled metadata** — every file metadata mutation is appended to a JSON-lines journal before updating memory. Survives restart via replay.
* **Lease-based writes** — clients acquire time-limited exclusive locks. Expired leases are auto-reclaimed.
* **Automatic shard rebuild** — reconstructs missing shards when nodes fail.
* **SHA-256 checksum verification** on every chunk read.
* **3-node Docker cluster** for realistic failure testing.

## Architecture

```
                      ┌──────────────┐
              client →│  Controller  │  :8080
                      │  Spring Boot │
                      └──────┬───────┘
                             │ HTTP
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌─────────┐    ┌─────────┐    ┌─────────┐
        │ node-1  │    │ node-2  │    │ node-3  │
        │  :9001  │    │  :9002  │    │  :9003  │
        └─────────┘    └─────────┘    └─────────┘
       shards 0,3,6   shards 1,4,7   shards 2,5,8
```

## Tech stack

* Java 17, Spring Boot 3.2
* Backblaze Reed-Solomon
* Jackson
* Docker & Docker Compose
* JUnit 5

## Project structure

```
dfs-project/
├── pom.xml
├── Dockerfile
├── docker-compose.yml
├── src/main/java/com/dfs/
├── src/test/java/com/dfs/
└── storage-node/
```

## Prerequisites

* JDK 17+
* Maven 3.8+
* Docker

## Build and test

```bash
mvn clean test
mvn -DskipTests package
cd storage-node && mvn -DskipTests package && cd ..
```

## Run cluster

```bash
docker compose build
docker compose up -d
docker compose ps
```

## REST API

### Upload

```bash
curl -F 'file=@file.bin' http://localhost:8080/api/files
```

### Download

```bash
curl http://localhost:8080/api/files/{fileId}
```

### Delete

```bash
curl -X DELETE http://localhost:8080/api/files/{fileId}
```

## Failure demo

```bash
docker stop dfs-node-3
curl http://localhost:8080/api/files/{fileId}
curl -X POST http://localhost:8080/api/admin/rebuild
docker start dfs-node-3
```

## Design decisions

* **Erasure coding over replication** → lower storage cost
* **Round-robin placement** → guarantees recovery
* **Journaled metadata** → fast + durable
* **Single controller** → simpler design trade-off

## Tests

23 total tests covering:

* chunking
* encoding/decoding
* metadata recovery
* concurrency

## License

Backblaze Reed-Solomon (MIT). Rest for educational use.
