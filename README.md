# 🚀 Mini Redis (Rust Tokio Engine + CLI + GUI)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![Tokio](https://img.shields.io/badge/Async-Tokio-blue.svg)](https://tokio.rs)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

A high-performance, multi-threaded in-memory key-value store, cache, and message broker built from scratch in **Rust** using **Tokio** and the **RESP2** (Redis Serialization Protocol).

This repository features:
- ⚡ **Asynchronous TCP Server** with Append-Only File (AOF) persistence and TTL eviction.
- 💻 **Interactive CLI REPL** with syntax highlighting, command autocompletion, and history.
- 🖥️ **Cross-Platform Desktop GUI (`eframe`/`egui`)** with live key browser, inline editor, raw terminal console, and real-time Pub/Sub monitor.

---

## 📸 Screenshots

> Place your screenshots in `assets/screenshots/` (or update the paths below):

### 🖥️ Desktop GUI Studio (`mini-redis-gui`)
![Mini Redis Studio GUI](assets/screenshots/redis%20gui.png)

### 💻 Interactive REPL Client (`mini-redis-cli`)
![Mini Redis CLI](assets\screenshots/redis%20cli.png)



---

## 🌟 Key Features

- **⚡ Async I/O Engine**: Multi-threaded TCP server built on the `Tokio` runtime.
- **📜 Full RESP2 Protocol**: Robust zero-copy frame parser and codec for Simple Strings, Errors, Integers, Bulk Strings, Arrays, and Null values.
- **💾 Rich Data Types & Commands**:
  - **Strings**: `SET`, `GET`, `DEL`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `EXISTS`, `TYPE`
  - **Key Expiration & TTL**: `EXPIRE`, `PERSIST`, `TTL` with active background eviction worker
  - **Hashes**: `HSET`, `HGET`, `HGETALL`, `HDEL`
  - **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
  - **Pub/Sub Subsystem**: Real-time broadcast messaging with `PUBLISH` and `SUBSCRIBE`
  - **Server & Keyspace**: `KEYS`, `DBSIZE`, `FLUSHDB`, `INFO`, `PING`, `ECHO`
- **🛡️ AOF (Append-Only File) Persistence**: Asynchronously logs mutating operations and replays state seamlessly on startup.
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
├── assets/screenshots/           # Screenshots folder
├── run-server.bat                # 1-Click Server launcher
├── run-cli.bat                   # 1-Click CLI launcher
└── run-gui.bat                   # 1-Click GUI launcher
```

---

## 🚀 Getting Started & How to Run

### 1. Clone & Build the Project
```bash
git clone https://github.com/Sonal-sp/mini_redis.git
cd mini_redis
cargo build
```

### 2. Start the Mini Redis Server
```powershell
cargo run -p mini-redis-server
```
*(Or double-click `run-server.bat`)*

### 3. Launch the Interactive CLI
```powershell
cargo run -p mini-redis-cli
```
*(Or double-click `run-cli.bat`)*

### 4. Launch the Desktop GUI
```powershell
cargo run -p mini-redis-gui
```
*(Or double-click `run-gui.bat`)*

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

---

## ⭐ Show Your Support

If you found this project helpful, educational, or fun to explore, please consider giving it a **Star ⭐** on GitHub! It means a lot and helps support future developments.

---

## 👨‍💻 Author & Open to Connect

Hi there! I'm **Sonal Parmar**, a **Computer Engineering** student with a strong passion for systems programming, low-level architecture, Rust, and building high-performance software.

I am always open to connecting, discussing technical ideas, open-source projects, and new opportunities:

- 🐙 **GitHub**: [@Sonal-sp](https://github.com/Sonal-sp)
- 💼 **LinkedIn**: [Sonal Parmar](https://www.linkedin.com/) *(Connect on LinkedIn)*
- 📧 **Email**: [sonalparmar2697@gmail.com](mailto:sonalparmar2697@gmail.com)

---

## 📄 License

Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for more information.
