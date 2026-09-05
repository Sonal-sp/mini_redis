# 🚀 Mini Redis (Rust Tokio Engine + CLI + GUI)

A high-performance, asynchronous in-memory key-value database and cache built with **Rust**, **Tokio**, and **RESP2** (Redis Serialization Protocol). It includes a multi-threaded async TCP server with AOF persistence, an interactive command-line REPL (CLI), and a modern desktop GUI tool built with **`egui`** (`eframe`).

---

## 🌟 Features

- **⚡ Async I/O Engine**: Multi-threaded TCP server built on `Tokio` runtime.
- **📜 Full RESP2 Protocol**: Robust zero-copy frame parser and codec for Simple Strings, Errors, Integers, Bulk Strings, Arrays, and Null values.
- **💾 Rich Data Types**:
  - **Strings**: `SET`, `GET`, `DEL`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `EXISTS`, `TYPE`
  - **Key Expiration & TTL**: `EXPIRE`, `PERSIST`, `TTL` with active background eviction worker
  - **Hashes**: `HSET`, `HGET`, `HGETALL`, `HDEL`
  - **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
  - **Pub/Sub Subsystem**: Real-time broadcast messaging with `PUBLISH` and `SUBSCRIBE`
  - **Server & Keyspace**: `KEYS`, `DBSIZE`, `FLUSHDB`, `INFO`, `PING`, `ECHO`
- **🛡️ AOF (Append-Only File) Persistence**: Asynchronously logs mutating operations and replays state on startup.
- **💻 Interactive CLI REPL**:
  - Tab auto-completion for Redis commands
  - Color-coded output formatting
  - Persistent command history
- **🖥️ Native Desktop GUI (`eframe`/`egui`)**:
  - **Key Explorer**: Browse keys, live search filter, view TTL countdowns, edit values (String, Hash, List), delete keys, add new keys.
  - **Console Tab**: Direct interactive command terminal.
  - **Pub/Sub Tab**: Subscribe to live topics and publish payloads with live timestamped logs.
  - **Server Stats Tab**: Real-time memory and server metrics.

---

## 📁 Workspace Architecture

```
mini_redis/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── mini-redis-core/          # Protocol parser, storage engine, AOF, client SDK
│   ├── mini-redis-server/        # Asynchronous TCP Server binary
│   ├── mini-redis-cli/           # Interactive REPL Terminal client
│   └── mini-redis-gui/           # Native Desktop GUI client
```

---

## 🚀 Getting Started & How to Run

### 1. Build the Entire Workspace
```bash
cargo build --release
```

### 2. Run the Server
```bash
cargo run -p mini-redis-server
```
*Custom Options:*
```bash
cargo run -p mini-redis-server -- --host 127.0.0.1 --port 6379 --aof-path appendonly.aof
```

### 3. Run the Interactive CLI
```bash
cargo run -p mini-redis-cli
```
*Direct Command Execution:*
```bash
cargo run -p mini-redis-cli -- SET greeting "Hello Rust" EX 60
cargo run -p mini-redis-cli -- GET greeting
```

### 4. Run the Desktop GUI
```bash
cargo run -p mini-redis-gui
```

---

## 📖 Supported Commands Reference

| Command | Syntax | Description |
| :--- | :--- | :--- |
| `PING` | `PING [msg]` | Test connection; returns `PONG` or message |
| `SET` | `SET key value [EX secs] [PX ms]` | Set key to value with optional TTL |
| `GET` | `GET key` | Retrieve string value |
| `DEL` | `DEL key [key ...]` | Delete one or more keys |
| `EXISTS` | `EXISTS key [key ...]` | Check if keys exist |
| `EXPIRE` | `EXPIRE key seconds` | Set timeout on key |
| `PERSIST` | `PERSIST key` | Remove expiration timeout |
| `TTL` | `TTL key` | Get remaining TTL in seconds |
| `KEYS` | `KEYS [pattern]` | List matching keys (e.g. `KEYS *`) |
| `TYPE` | `TYPE key` | Returns data type (`string`, `hash`, `list`) |
| `INCR` | `INCR key` / `DECR key` | Increment / decrement integer |
| `HSET` | `HSET key field value` | Set field in hash |
| `HGET` | `HGET key field` | Get field from hash |
| `HGETALL` | `HGETALL key` | Return all fields & values in hash |
| `HDEL` | `HDEL key field [field ...]` | Delete hash fields |
| `LPUSH` | `LPUSH key val [val ...]` | Prepend elements to list |
| `RPUSH` | `RPUSH key val [val ...]` | Append elements to list |
| `LPOP` | `LPOP key [count]` | Pop elements from list head |
| `RPOP` | `RPOP key [count]` | Pop elements from list tail |
| `LRANGE` | `LRANGE key start stop` | Get range of elements from list |
| `LLEN` | `LLEN key` | Get length of list |
| `PUBLISH` | `PUBLISH channel message` | Publish message to channel |
| `SUBSCRIBE` | `SUBSCRIBE channel [chan ...]`| Stream incoming channel messages |
| `DBSIZE` | `DBSIZE` | Return total number of stored keys |
| `FLUSHDB` | `FLUSHDB` | Delete all keys |
| `INFO` | `INFO` | Server statistics and metrics |
