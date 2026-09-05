use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Supported Redis data types in Mini Redis
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<String, Bytes>),
    Set(HashSet<Bytes>),
}

impl DataType {
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
            DataType::List(_) => "list",
            DataType::Hash(_) => "hash",
            DataType::Set(_) => "set",
        }
    }
}

/// An entry in the database with optional expiration timestamp
#[derive(Clone, Debug)]
pub struct Entry {
    pub data: DataType,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new_string(val: Bytes, expires_at: Option<Instant>) -> Self {
        Self {
            data: DataType::String(val),
            expires_at,
        }
    }

    pub fn new_list(list: VecDeque<Bytes>, expires_at: Option<Instant>) -> Self {
        Self {
            data: DataType::List(list),
            expires_at,
        }
    }

    pub fn new_hash(hash: HashMap<String, Bytes>, expires_at: Option<Instant>) -> Self {
        Self {
            data: DataType::Hash(hash),
            expires_at,
        }
    }

    pub fn new_set(set: HashSet<Bytes>, expires_at: Option<Instant>) -> Self {
        Self {
            data: DataType::Set(set),
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            Instant::now() >= exp
        } else {
            false
        }
    }
}

/// Thread-safe in-memory database shared across all connection tasks.
#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

struct Shared {
    entries: RwLock<HashMap<String, Entry>>,
    pubsub: Mutex<HashMap<String, broadcast::Sender<Bytes>>>,
    created_at: Instant,
    total_commands: AtomicUsize,
    total_connections: AtomicUsize,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                entries: RwLock::new(HashMap::new()),
                pubsub: Mutex::new(HashMap::new()),
                created_at: Instant::now(),
                total_commands: AtomicUsize::new(0),
                total_connections: AtomicUsize::new(0),
            }),
        }
    }

    pub fn increment_commands(&self) {
        self.shared.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_connections(&self) {
        self.shared.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    // ==========================================
    // String & Core Key-Value Operations
    // ==========================================

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                None
            } else if let DataType::String(ref val) = entry.data {
                Some(val.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut entries = self.shared.entries.write();
        let expires_at = expire.map(|d| Instant::now() + d);
        entries.insert(key, Entry::new_string(value, expires_at));
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut entries = self.shared.entries.write();
        let mut count = 0;
        for key in keys {
            if entries.remove(key).is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn exists(&self, keys: &[String]) -> usize {
        let mut entries = self.shared.entries.write();
        let mut count = 0;
        for key in keys {
            if let Some(entry) = entries.get(key) {
                if entry.is_expired() {
                    entries.remove(key);
                } else {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn expire(&self, key: &str, duration: Duration) -> bool {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                false
            } else {
                entry.expires_at = Some(Instant::now() + duration);
                true
            }
        } else {
            false
        }
    }

    pub fn ttl(&self, key: &str) -> i64 {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                -2 // Key does not exist (expired)
            } else if let Some(expires_at) = entry.expires_at {
                let now = Instant::now();
                if expires_at > now {
                    (expires_at - now).as_secs() as i64
                } else {
                    -2
                }
            } else {
                -1 // Key exists with no expiration
            }
        } else {
            -2 // Key does not exist
        }
    }

    pub fn persist(&self, key: &str) -> bool {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                false
            } else if entry.expires_at.is_some() {
                entry.expires_at = None;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn type_of(&self, key: &str) -> &'static str {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                "none"
            } else {
                entry.data.type_name()
            }
        } else {
            "none"
        }
    }

    pub fn keys(&self, pattern: Option<&str>) -> Vec<String> {
        let mut entries = self.shared.entries.write();
        let mut valid_keys = Vec::new();
        let mut expired_keys = Vec::new();

        for (k, v) in entries.iter() {
            if v.is_expired() {
                expired_keys.push(k.clone());
            } else {
                match pattern {
                    None | Some("*") => valid_keys.push(k.clone()),
                    Some(pat) => {
                        if pat.ends_with('*') {
                            let prefix = &pat[..pat.len() - 1];
                            if k.starts_with(prefix) {
                                valid_keys.push(k.clone());
                            }
                        } else if k == pat {
                            valid_keys.push(k.clone());
                        }
                    }
                }
            }
        }

        for k in expired_keys {
            entries.remove(&k);
        }

        valid_keys
    }

    pub fn flushdb(&self) {
        let mut entries = self.shared.entries.write();
        entries.clear();
    }

    pub fn dbsize(&self) -> usize {
        let mut entries = self.shared.entries.write();
        entries.retain(|_, v| !v.is_expired());
        entries.len()
    }

    pub fn incr(&self, key: &str, delta: i64) -> Result<i64, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                let new_val = delta;
                entries.insert(
                    key.to_string(),
                    Entry::new_string(Bytes::from(new_val.to_string()), None),
                );
                return Ok(new_val);
            }
            match &mut entry.data {
                DataType::String(val) => {
                    let s = std::str::from_utf8(val).map_err(|_| "ERR value is not an integer")?;
                    let n: i64 = s.parse().map_err(|_| "ERR value is not an integer")?;
                    let new_val = n + delta;
                    *val = Bytes::from(new_val.to_string());
                    Ok(new_val)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            let new_val = delta;
            entries.insert(
                key.to_string(),
                Entry::new_string(Bytes::from(new_val.to_string()), None),
            );
            Ok(new_val)
        }
    }

    // ==========================================
    // Hash Operations (HSET, HGET, HGETALL, HDEL)
    // ==========================================

    pub fn hset(&self, key: String, field: String, val: Bytes) -> Result<bool, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(&key) {
            if entry.is_expired() {
                entries.remove(&key);
                let mut map = HashMap::new();
                map.insert(field, val);
                entries.insert(key, Entry::new_hash(map, None));
                return Ok(true);
            }
            match &mut entry.data {
                DataType::Hash(map) => {
                    let is_new = map.insert(field, val).is_none();
                    Ok(is_new)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            let mut map = HashMap::new();
            map.insert(field, val);
            entries.insert(key, Entry::new_hash(map, None));
            Ok(true)
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(None);
            }
            match &entry.data {
                DataType::Hash(map) => Ok(map.get(field).cloned()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(None)
        }
    }

    pub fn hgetall(&self, key: &str) -> Result<Option<HashMap<String, Bytes>>, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(None);
            }
            match &entry.data {
                DataType::Hash(map) => Ok(Some(map.clone())),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(None)
        }
    }

    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<usize, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(0);
            }
            match &mut entry.data {
                DataType::Hash(map) => {
                    let mut count = 0;
                    for f in fields {
                        if map.remove(f).is_some() {
                            count += 1;
                        }
                    }
                    Ok(count)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(0)
        }
    }

    // ==========================================
    // List Operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
    // ==========================================

    pub fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(&key) {
            if entry.is_expired() {
                entries.remove(&key);
                let mut list = VecDeque::new();
                for v in values {
                    list.push_front(v);
                }
                let len = list.len();
                entries.insert(key, Entry::new_list(list, None));
                return Ok(len);
            }
            match &mut entry.data {
                DataType::List(list) => {
                    for v in values {
                        list.push_front(v);
                    }
                    Ok(list.len())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            let mut list = VecDeque::new();
            for v in values {
                list.push_front(v);
            }
            let len = list.len();
            entries.insert(key, Entry::new_list(list, None));
            Ok(len)
        }
    }

    pub fn rpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(&key) {
            if entry.is_expired() {
                entries.remove(&key);
                let mut list = VecDeque::new();
                for v in values {
                    list.push_back(v);
                }
                let len = list.len();
                entries.insert(key, Entry::new_list(list, None));
                return Ok(len);
            }
            match &mut entry.data {
                DataType::List(list) => {
                    for v in values {
                        list.push_back(v);
                    }
                    Ok(list.len())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            let mut list = VecDeque::new();
            for v in values {
                list.push_back(v);
            }
            let len = list.len();
            entries.insert(key, Entry::new_list(list, None));
            Ok(len)
        }
    }

    pub fn lpop(&self, key: &str, count: usize) -> Result<Vec<Bytes>, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(Vec::new());
            }
            match &mut entry.data {
                DataType::List(list) => {
                    let mut out = Vec::new();
                    for _ in 0..count {
                        if let Some(item) = list.pop_front() {
                            out.push(item);
                        } else {
                            break;
                        }
                    }
                    Ok(out)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn rpop(&self, key: &str, count: usize) -> Result<Vec<Bytes>, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(Vec::new());
            }
            match &mut entry.data {
                DataType::List(list) => {
                    let mut out = Vec::new();
                    for _ in 0..count {
                        if let Some(item) = list.pop_back() {
                            out.push(item);
                        } else {
                            break;
                        }
                    }
                    Ok(out)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(Vec::new());
            }
            match &entry.data {
                DataType::List(list) => {
                    let len = list.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }

                    let s = if start < 0 {
                        (len + start).max(0) as usize
                    } else {
                        start.min(len) as usize
                    };

                    let e = if stop < 0 {
                        (len + stop).max(0) as usize
                    } else {
                        stop.min(len - 1) as usize
                    };

                    if s > e || s >= list.len() {
                        return Ok(Vec::new());
                    }

                    let mut out = Vec::new();
                    for i in s..=e {
                        if let Some(val) = list.get(i) {
                            out.push(val.clone());
                        }
                    }
                    Ok(out)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn llen(&self, key: &str) -> Result<usize, &'static str> {
        let mut entries = self.shared.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                entries.remove(key);
                return Ok(0);
            }
            match &entry.data {
                DataType::List(list) => Ok(list.len()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(0)
        }
    }

    // ==========================================
    // Pub / Sub Subsystem
    // ==========================================

    pub fn publish(&self, channel: &str, message: Bytes) -> usize {
        let pubsub = self.shared.pubsub.lock();
        if let Some(sender) = pubsub.get(channel) {
            sender.send(message).unwrap_or(0)
        } else {
            0
        }
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<Bytes> {
        let mut pubsub = self.shared.pubsub.lock();
        if let Some(sender) = pubsub.get(channel) {
            sender.subscribe()
        } else {
            let (sender, receiver) = broadcast::channel(1024);
            pubsub.insert(channel.to_string(), sender);
            receiver
        }
    }

    // ==========================================
    // Info & Server Stats
    // ==========================================

    pub fn info(&self) -> String {
        let entries = self.shared.entries.read();
        let uptime = self.shared.created_at.elapsed().as_secs();
        let total_cmd = self.shared.total_commands.load(Ordering::Relaxed);
        let total_conn = self.shared.total_connections.load(Ordering::Relaxed);
        let keys_count = entries.len();

        format!(
            "# Server\r\n\
             mini_redis_version:0.1.0\r\n\
             uptime_in_seconds:{}\r\n\
             \r\n\
             # Clients\r\n\
             connected_clients:{}\r\n\
             \r\n\
             # Stats\r\n\
             total_commands_processed:{}\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={},expires=0,avg_ttl=0\r\n",
            uptime, total_conn, total_cmd, keys_count
        )
    }

    // ==========================================
    // Active TTL Worker Loop
    // ==========================================

    pub fn purge_expired_keys(&self) -> usize {
        let mut entries = self.shared.entries.write();
        let before = entries.len();
        entries.retain(|_, v| !v.is_expired());
        before - entries.len()
    }
}

/// Spawns a background task that periodically purges expired keys.
pub fn start_active_expiration_worker(db: Db, interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            db.purge_expired_keys();
        }
    })
}
