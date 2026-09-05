use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub enum Tab {
    KeyExplorer,
    Console,
    PubSub,
    ServerStats,
}

#[derive(Clone, Debug)]
pub struct KeySummary {
    pub name: String,
    pub key_type: String,
    #[allow(dead_code)]
    pub ttl: i64,
}

#[derive(Clone, Debug)]
pub enum KeyDetail {
    String(String),
    Hash(HashMap<String, String>),
    List(Vec<String>),
    NotFound,
}

#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub timestamp: String,
    pub channel: String,
    pub payload: String,
}

#[derive(Clone, Debug)]
pub struct ConsoleEntry {
    pub timestamp: String,
    pub command: String,
    pub response: String,
    pub is_error: bool,
}
