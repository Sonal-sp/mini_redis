use bytes::Bytes;
use chrono::Local;
use eframe::egui;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio::runtime::Handle;

use mini_redis_core::{Client, Frame};
use crate::types::{ConsoleEntry, KeyDetail, KeySummary, PubSubMessage, Tab};

enum AsyncAction {
    Connected(String),
    ConnectionFailed(String),
    Disconnected,
    KeysLoaded(Vec<KeySummary>),
    KeyDetailLoaded(String, KeyDetail, i64, String),
    KeySaved(String),
    KeyDeleted(String),
    ConsoleResponse(ConsoleEntry),
    PubSubReceived(PubSubMessage),
    InfoLoaded(String),
    #[allow(dead_code)]
    ErrorNotice(String),
}

pub struct MiniRedisGuiApp {
    // Tokio runtime handle for background async tasks
    tokio_handle: Handle,

    // Channels for async communication between Tokio tasks and UI thread
    action_tx: Sender<AsyncAction>,
    action_rx: Receiver<AsyncAction>,

    // Connection state
    host: String,
    port: String,
    is_connected: bool,
    is_connecting: bool,
    status_message: String,

    // Active UI tab
    active_tab: Tab,

    // Key Explorer state
    key_filter: String,
    keys: Vec<KeySummary>,
    selected_key: Option<String>,
    selected_key_type: String,
    selected_key_ttl: i64,
    selected_key_detail: Option<KeyDetail>,
    string_edit_buffer: String,
    #[allow(dead_code)]
    new_hash_field: String,
    #[allow(dead_code)]
    new_hash_value: String,
    #[allow(dead_code)]
    new_list_element: String,
    new_ttl_input: String,

    // New Key Modal state
    show_new_key_modal: bool,
    new_key_name: String,
    new_key_type: String,
    new_key_value: String,
    new_key_ttl: String,

    // Console tab state
    console_input: String,
    console_history: Vec<ConsoleEntry>,

    // Pub/Sub tab state
    pubsub_channel_input: String,
    subscribed_channels: Vec<String>,
    pubsub_messages: Vec<PubSubMessage>,
    publish_channel: String,
    publish_payload: String,

    // Server Info tab state
    server_info_raw: String,
}

impl MiniRedisGuiApp {
    pub fn new(tokio_handle: Handle) -> Self {
        let (action_tx, action_rx) = channel();

        Self {
            tokio_handle,
            action_tx,
            action_rx,
            host: "127.0.0.1".to_string(),
            port: "6379".to_string(),
            is_connected: false,
            is_connecting: false,
            status_message: "Not connected".to_string(),
            active_tab: Tab::KeyExplorer,
            key_filter: "".to_string(),
            keys: Vec::new(),
            selected_key: None,
            selected_key_type: "".to_string(),
            selected_key_ttl: -1,
            selected_key_detail: None,
            string_edit_buffer: "".to_string(),
            new_hash_field: "".to_string(),
            new_hash_value: "".to_string(),
            new_list_element: "".to_string(),
            new_ttl_input: "".to_string(),
            show_new_key_modal: false,
            new_key_name: "".to_string(),
            new_key_type: "string".to_string(),
            new_key_value: "".to_string(),
            new_key_ttl: "".to_string(),
            console_input: "".to_string(),
            console_history: Vec::new(),
            pubsub_channel_input: "events".to_string(),
            subscribed_channels: Vec::new(),
            pubsub_messages: Vec::new(),
            publish_channel: "events".to_string(),
            publish_payload: "Hello from Mini Redis GUI!".to_string(),
            server_info_raw: "".to_string(),
        }
    }

    fn server_addr(&self) -> String {
        format!("{}:{}", self.host.trim(), self.port.trim())
    }

    fn trigger_connect(&mut self) {
        if self.is_connecting {
            return;
        }

        self.is_connecting = true;
        self.status_message = "Connecting...".to_string();
        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            match Client::connect(&addr).await {
                Ok(mut client) => {
                    match client.ping(None).await {
                        Ok(_) => {
                            let _ = tx.send(AsyncAction::Connected(addr));
                        }
                        Err(e) => {
                            let _ = tx.send(AsyncAction::ConnectionFailed(e.to_string()));
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(AsyncAction::ConnectionFailed(e.to_string()));
                }
            }
        });
    }

    fn trigger_refresh_keys(&self) {
        if !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                if let Ok(keys) = client.keys("*").await {
                    let mut list = Vec::new();
                    for k in keys {
                        let t = client.type_of(&k).await.unwrap_or_else(|_| "string".to_string());
                        let ttl = client.ttl(&k).await.unwrap_or(-1);
                        list.push(KeySummary {
                            name: k,
                            key_type: t,
                            ttl,
                        });
                    }
                    let _ = tx.send(AsyncAction::KeysLoaded(list));
                }
            }
        });
    }

    fn trigger_load_key_detail(&self, key: String) {
        if !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                let key_type = client.type_of(&key).await.unwrap_or_else(|_| "none".to_string());
                let ttl = client.ttl(&key).await.unwrap_or(-1);

                match key_type.as_str() {
                    "string" => {
                        if let Ok(Some(val)) = client.get(&key).await {
                            let s = String::from_utf8_lossy(&val).to_string();
                            let _ = tx.send(AsyncAction::KeyDetailLoaded(
                                key,
                                KeyDetail::String(s),
                                ttl,
                                key_type,
                            ));
                        } else {
                            let _ = tx.send(AsyncAction::KeyDetailLoaded(
                                key,
                                KeyDetail::NotFound,
                                ttl,
                                key_type,
                            ));
                        }
                    }
                    "hash" => {
                        let cmd = format!("HGETALL {}", key);
                        if let Ok(Frame::Array(arr)) = client.execute_raw(&cmd).await {
                            let mut map = HashMap::new();
                            let mut i = 0;
                            while i + 1 < arr.len() {
                                if let (Frame::Bulk(k), Frame::Bulk(v)) = (&arr[i], &arr[i + 1]) {
                                    map.insert(
                                        String::from_utf8_lossy(k).to_string(),
                                        String::from_utf8_lossy(v).to_string(),
                                    );
                                }
                                i += 2;
                            }
                            let _ = tx.send(AsyncAction::KeyDetailLoaded(
                                key,
                                KeyDetail::Hash(map),
                                ttl,
                                key_type,
                            ));
                        }
                    }
                    "list" => {
                        let cmd = format!("LRANGE {} 0 -1", key);
                        if let Ok(Frame::Array(arr)) = client.execute_raw(&cmd).await {
                            let mut list = Vec::new();
                            for item in arr {
                                if let Frame::Bulk(b) = item {
                                    list.push(String::from_utf8_lossy(&b).to_string());
                                }
                            }
                            let _ = tx.send(AsyncAction::KeyDetailLoaded(
                                key,
                                KeyDetail::List(list),
                                ttl,
                                key_type,
                            ));
                        }
                    }
                    _ => {
                        let _ = tx.send(AsyncAction::KeyDetailLoaded(
                            key,
                            KeyDetail::NotFound,
                            ttl,
                            key_type,
                        ));
                    }
                }
            }
        });
    }

    fn trigger_save_string_key(&self, key: String, val: String) {
        if !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                let _ = client.set(&key, Bytes::from(val), None).await;
                let _ = tx.send(AsyncAction::KeySaved(key));
            }
        });
    }

    fn trigger_delete_key(&self, key: String) {
        if !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                let _ = client.del(&[&key]).await;
                let _ = tx.send(AsyncAction::KeyDeleted(key));
            }
        });
    }

    fn trigger_set_ttl(&self, key: String, seconds: u64) {
        if !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                let cmd = format!("EXPIRE {} {}", key, seconds);
                let _ = client.execute_raw(&cmd).await;
                let _ = tx.send(AsyncAction::KeySaved(key));
            }
        });
    }

    fn trigger_add_new_key(&mut self) {
        if !self.is_connected || self.new_key_name.trim().is_empty() {
            return;
        }

        let name = self.new_key_name.trim().to_string();
        let ktype = self.new_key_type.clone();
        let val = self.new_key_value.clone();
        let ttl_opt: Option<u64> = self.new_key_ttl.trim().parse().ok();
        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                match ktype.as_str() {
                    "string" => {
                        let _ = client.set(&name, Bytes::from(val), ttl_opt).await;
                    }
                    "hash" => {
                        let _ = client.execute_raw(&format!("HSET {} default \"{}\"", name, val)).await;
                        if let Some(ttl) = ttl_opt {
                            let _ = client.execute_raw(&format!("EXPIRE {} {}", name, ttl)).await;
                        }
                    }
                    "list" => {
                        let _ = client.execute_raw(&format!("RPUSH {} \"{}\"", name, val)).await;
                        if let Some(ttl) = ttl_opt {
                            let _ = client.execute_raw(&format!("EXPIRE {} {}", name, ttl)).await;
                        }
                    }
                    _ => {}
                }
                let _ = tx.send(AsyncAction::KeySaved(name));
            }
        });

        self.show_new_key_modal = false;
        self.new_key_name.clear();
        self.new_key_value.clear();
        self.new_key_ttl.clear();
    }

    fn trigger_execute_console(&mut self) {
        let input = self.console_input.trim().to_string();
        if input.is_empty() || !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();
        let timestamp = Local::now().format("%H:%M:%S").to_string();
        let cmd = input.clone();
        self.console_input.clear();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                match client.execute_raw(&cmd).await {
                    Ok(frame) => {
                        let is_err = matches!(frame, Frame::Error(_));
                        let entry = ConsoleEntry {
                            timestamp,
                            command: cmd,
                            response: frame.to_display_string(),
                            is_error: is_err,
                        };
                        let _ = tx.send(AsyncAction::ConsoleResponse(entry));
                    }
                    Err(e) => {
                        let entry = ConsoleEntry {
                            timestamp,
                            command: cmd,
                            response: format!("Error: {}", e),
                            is_error: true,
                        };
                        let _ = tx.send(AsyncAction::ConsoleResponse(entry));
                    }
                }
            }
        });
    }

    fn trigger_subscribe(&mut self) {
        let chan = self.pubsub_channel_input.trim().to_string();
        if chan.is_empty() || !self.is_connected || self.subscribed_channels.contains(&chan) {
            return;
        }

        self.subscribed_channels.push(chan.clone());
        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                let cmd = format!("SUBSCRIBE {}", chan);
                if let Ok(_) = client.execute_raw(&cmd).await {
                    loop {
                        match client.read_frame().await {
                            Ok(Some(Frame::Array(arr))) => {
                                if arr.len() >= 3 {
                                    if let (Frame::Bulk(ch), Frame::Bulk(payload)) = (&arr[1], &arr[2]) {
                                        let msg = PubSubMessage {
                                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                                            channel: String::from_utf8_lossy(ch).to_string(),
                                            payload: String::from_utf8_lossy(payload).to_string(),
                                        };
                                        let _ = tx.send(AsyncAction::PubSubReceived(msg));
                                    }
                                }
                            }
                            Ok(Some(_)) => {}
                            _ => break,
                        }
                    }
                }
            }
        });
    }

    fn trigger_publish(&self) {
        let chan = self.publish_channel.trim().to_string();
        let payload = self.publish_payload.trim().to_string();
        if chan.is_empty() || !self.is_connected {
            return;
        }

        let addr = self.server_addr();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                let _ = client.publish(&chan, &payload).await;
            }
        });
    }

    fn trigger_load_info(&self) {
        if !self.is_connected {
            return;
        }

        let addr = self.server_addr();
        let tx = self.action_tx.clone();

        self.tokio_handle.spawn(async move {
            if let Ok(mut client) = Client::connect(&addr).await {
                if let Ok(info) = client.info().await {
                    let _ = tx.send(AsyncAction::InfoLoaded(info));
                }
            }
        });
    }

    fn process_async_messages(&mut self) {
        while let Ok(action) = self.action_rx.try_recv() {
            match action {
                AsyncAction::Connected(addr) => {
                    self.is_connected = true;
                    self.is_connecting = false;
                    self.status_message = format!("Connected to {}", addr);
                    self.trigger_refresh_keys();
                    self.trigger_load_info();
                }
                AsyncAction::ConnectionFailed(err) => {
                    self.is_connected = false;
                    self.is_connecting = false;
                    self.status_message = format!("Connection failed: {}", err);
                }
                AsyncAction::Disconnected => {
                    self.is_connected = false;
                    self.is_connecting = false;
                    self.status_message = "Disconnected".to_string();
                    self.keys.clear();
                    self.selected_key = None;
                    self.selected_key_detail = None;
                }
                AsyncAction::KeysLoaded(keys) => {
                    self.keys = keys;
                }
                AsyncAction::KeyDetailLoaded(key, detail, ttl, ktype) => {
                    if self.selected_key.as_deref() == Some(&key) {
                        self.selected_key_type = ktype;
                        self.selected_key_ttl = ttl;
                        if let KeyDetail::String(ref s) = detail {
                            self.string_edit_buffer = s.clone();
                        }
                        self.selected_key_detail = Some(detail);
                    }
                }
                AsyncAction::KeySaved(key) => {
                    self.trigger_refresh_keys();
                    if self.selected_key.as_deref() == Some(&key) {
                        self.trigger_load_key_detail(key);
                    }
                }
                AsyncAction::KeyDeleted(_key) => {
                    self.selected_key = None;
                    self.selected_key_detail = None;
                    self.trigger_refresh_keys();
                }
                AsyncAction::ConsoleResponse(entry) => {
                    self.console_history.push(entry);
                }
                AsyncAction::PubSubReceived(msg) => {
                    self.pubsub_messages.push(msg);
                }
                AsyncAction::InfoLoaded(info) => {
                    self.server_info_raw = info;
                }
                AsyncAction::ErrorNotice(err) => {
                    self.status_message = format!("Error: {}", err);
                }
            }
        }
    }
}

impl eframe::App for MiniRedisGuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_async_messages();

        // Top Connection Bar Panel
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.heading("🚀 Mini Redis Studio");
                ui.add_space(20.0);

                ui.label("Host:");
                ui.add(egui::TextEdit::singleline(&mut self.host).desired_width(120.0));

                ui.label("Port:");
                ui.add(egui::TextEdit::singleline(&mut self.port).desired_width(60.0));

                if !self.is_connected {
                    if ui.button(if self.is_connecting { "Connecting..." } else { "🔌 Connect" }).clicked() {
                        self.trigger_connect();
                    }
                } else {
                    if ui.button("❌ Disconnect").clicked() {
                        let _ = self.action_tx.send(AsyncAction::Disconnected);
                    }
                }

                ui.add_space(15.0);
                if self.is_connected {
                    ui.colored_label(egui::Color32::from_rgb(0, 220, 100), format!("● {}", self.status_message));
                } else {
                    ui.colored_label(egui::Color32::from_rgb(220, 60, 60), format!("○ {}", self.status_message));
                }
            });
            ui.add_space(6.0);

            // Tab Navigation Bar
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.active_tab, Tab::KeyExplorer, "🔑 Key Explorer");
                ui.selectable_value(&mut self.active_tab, Tab::Console, "💻 Console CLI");
                ui.selectable_value(&mut self.active_tab, Tab::PubSub, "📡 Pub/Sub Monitor");
                ui.selectable_value(&mut self.active_tab, Tab::ServerStats, "📊 Server Stats");
            });
            ui.add_space(6.0);
        });

        // Bottom Status Bar
        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(format!("Total Keys: {}", self.keys.len()));
                ui.separator();
                ui.label(format!("Active Tab: {:?}", self.active_tab));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label("Mini Redis v0.1.0 (Rust)");
                });
            });
        });

        // Main Central View Panel
        egui::CentralPanel::default().show(ctx, |ui| {
            match self.active_tab {
                Tab::KeyExplorer => self.render_key_explorer(ui),
                Tab::Console => self.render_console(ui),
                Tab::PubSub => self.render_pubsub(ui),
                Tab::ServerStats => self.render_server_stats(ui),
            }
        });

        // New Key Modal Window
        if self.show_new_key_modal {
            egui::Window::new("➕ Add New Key")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.add_space(5.0);
                    ui.horizontal(|ui| {
                        ui.label("Key Name:");
                        ui.text_edit_singleline(&mut self.new_key_name);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Key Type:");
                        egui::ComboBox::from_id_salt("new_key_type_combo")
                            .selected_text(&self.new_key_type)
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut self.new_key_type, "string".to_string(), "String");
                                ui.selectable_value(&mut self.new_key_type, "hash".to_string(), "Hash");
                                ui.selectable_value(&mut self.new_key_type, "list".to_string(), "List");
                            });
                    });

                    ui.horizontal(|ui| {
                        ui.label("Value / Initial Item:");
                        ui.text_edit_singleline(&mut self.new_key_value);
                    });

                    ui.horizontal(|ui| {
                        ui.label("TTL (seconds, optional):");
                        ui.text_edit_singleline(&mut self.new_key_ttl);
                    });

                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        if ui.button("Save Key").clicked() {
                            self.trigger_add_new_key();
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_new_key_modal = false;
                        }
                    });
                });
        }
    }
}

impl MiniRedisGuiApp {
    fn render_key_explorer(&mut self, ui: &mut egui::Ui) {
        if !self.is_connected {
            ui.centered_and_justified(|ui| {
                ui.label("⚠️ Please connect to Mini Redis server first using the Connect button above.");
            });
            return;
        }

        ui.columns(2, |columns| {
            // Left Column: Keys List & Search
            columns[0].vertical(|ui| {
                ui.horizontal(|ui| {
                    ui.label("🔍 Filter:");
                    ui.text_edit_singleline(&mut self.key_filter);
                    if ui.button("🔄 Refresh").clicked() {
                        self.trigger_refresh_keys();
                    }
                    if ui.button("➕ Add Key").clicked() {
                        self.show_new_key_modal = true;
                    }
                });
                ui.separator();

                let filter = self.key_filter.to_lowercase();
                let filtered_keys: Vec<KeySummary> = self
                    .keys
                    .iter()
                    .filter(|k| filter.is_empty() || k.name.to_lowercase().contains(&filter))
                    .cloned()
                    .collect();

                egui::ScrollArea::vertical()
                    .id_salt("keys_scroll")
                    .show(ui, |ui| {
                        for k in filtered_keys {
                            let is_selected = self.selected_key.as_deref() == Some(&k.name);
                            let label = format!("[{}] {}", k.key_type.to_uppercase(), k.name);

                            if ui.selectable_label(is_selected, label).clicked() {
                                self.selected_key = Some(k.name.clone());
                                self.trigger_load_key_detail(k.name.clone());
                            }
                        }
                    });
            });

            // Right Column: Key Inspector & Editor
            columns[1].vertical(|ui| {
                if let Some(ref key_name) = self.selected_key.clone() {
                    ui.horizontal(|ui| {
                        ui.heading(format!("Key: {}", key_name));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("🗑️ Delete Key").clicked() {
                                self.trigger_delete_key(key_name.clone());
                            }
                        });
                    });

                    ui.horizontal(|ui| {
                        ui.label(format!("Type: {}", self.selected_key_type.to_uppercase()));
                        ui.separator();
                        let ttl_str = if self.selected_key_ttl == -1 {
                            "No Expiration".to_string()
                        } else {
                            format!("{}s remaining", self.selected_key_ttl)
                        };
                        ui.label(format!("TTL: {}", ttl_str));
                    });

                    ui.horizontal(|ui| {
                        ui.label("Set TTL (s):");
                        ui.add(egui::TextEdit::singleline(&mut self.new_ttl_input).desired_width(60.0));
                        if ui.button("Update TTL").clicked() {
                            if let Ok(secs) = self.new_ttl_input.trim().parse::<u64>() {
                                self.trigger_set_ttl(key_name.clone(), secs);
                                self.new_ttl_input.clear();
                            }
                        }
                    });

                    ui.separator();

                    // Display Value Editor based on Key Type
                    if let Some(ref detail) = self.selected_key_detail {
                        match detail {
                            KeyDetail::String(_) => {
                                ui.label("Value Editor:");
                                ui.add(
                                    egui::TextEdit::multiline(&mut self.string_edit_buffer)
                                        .desired_rows(12)
                                        .desired_width(f32::INFINITY),
                                );
                                if ui.button("💾 Save Changes").clicked() {
                                    self.trigger_save_string_key(key_name.clone(), self.string_edit_buffer.clone());
                                }
                            }
                            KeyDetail::Hash(map) => {
                                ui.label("Hash Fields:");
                                egui::ScrollArea::vertical()
                                    .id_salt("hash_scroll")
                                    .max_height(200.0)
                                    .show(ui, |ui| {
                                        for (f, v) in map {
                                            ui.horizontal(|ui| {
                                                ui.label(format!("{}:", f));
                                                ui.label(v);
                                            });
                                        }
                                    });
                            }
                            KeyDetail::List(list) => {
                                ui.label("List Items:");
                                egui::ScrollArea::vertical()
                                    .id_salt("list_scroll")
                                    .max_height(200.0)
                                    .show(ui, |ui| {
                                        for (i, v) in list.iter().enumerate() {
                                            ui.label(format!("{}. {}", i + 1, v));
                                        }
                                    });
                            }
                            KeyDetail::NotFound => {
                                ui.label("Key not found or expired.");
                            }
                        }
                    } else {
                        ui.spinner();
                    }
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.label("Select a key on the left to inspect and edit.");
                    });
                }
            });
        });
    }

    fn render_console(&mut self, ui: &mut egui::Ui) {
        ui.vertical(|ui| {
            ui.heading("💻 Interactive Redis Console");
            ui.label("Type Redis commands directly (e.g., SET foo bar, GET foo, KEYS *, PING, INFO)");
            ui.separator();

            // Console output history
            egui::ScrollArea::vertical()
                .id_salt("console_history_scroll")
                .max_height(350.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for entry in &self.console_history {
                        ui.horizontal(|ui| {
                            ui.colored_label(egui::Color32::GRAY, format!("[{}]", entry.timestamp));
                            ui.colored_label(egui::Color32::LIGHT_BLUE, format!("> {}", entry.command));
                        });
                        if entry.is_error {
                            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), &entry.response);
                        } else {
                            ui.colored_label(egui::Color32::LIGHT_GREEN, &entry.response);
                        }
                        ui.add_space(4.0);
                    }
                });

            ui.separator();
            ui.horizontal(|ui| {
                let response = ui.add(
                    egui::TextEdit::singleline(&mut self.console_input)
                        .hint_text("Enter command (e.g. SET message hello)")
                        .desired_width(ui.available_width() - 80.0),
                );

                if (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                    || ui.button("Send ↵").clicked()
                {
                    self.trigger_execute_console();
                }
            });
        });
    }

    fn render_pubsub(&mut self, ui: &mut egui::Ui) {
        ui.vertical(|ui| {
            ui.heading("📡 Real-time Pub/Sub Monitor");
            ui.separator();

            // Subscription controls
            ui.horizontal(|ui| {
                ui.label("Channel to Subscribe:");
                ui.text_edit_singleline(&mut self.pubsub_channel_input);
                if ui.button("📥 Subscribe").clicked() {
                    self.trigger_subscribe();
                }
            });

            if !self.subscribed_channels.is_empty() {
                ui.horizontal(|ui| {
                    ui.label("Subscribed Channels:");
                    for ch in &self.subscribed_channels {
                        ui.colored_label(egui::Color32::LIGHT_GREEN, format!("[{}]", ch));
                    }
                });
            }

            ui.separator();
            ui.label("Incoming Message Stream:");

            egui::ScrollArea::vertical()
                .id_salt("pubsub_msg_scroll")
                .max_height(200.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    if self.pubsub_messages.is_empty() {
                        ui.label("(No messages received yet)");
                    }
                    for msg in &self.pubsub_messages {
                        ui.horizontal(|ui| {
                            ui.colored_label(egui::Color32::GRAY, format!("[{}]", msg.timestamp));
                            ui.colored_label(egui::Color32::LIGHT_YELLOW, format!("[{}]", msg.channel));
                            ui.label(&msg.payload);
                        });
                    }
                });

            ui.separator();
            ui.heading("📤 Publish Message");
            ui.horizontal(|ui| {
                ui.label("Channel:");
                ui.add(egui::TextEdit::singleline(&mut self.publish_channel).desired_width(120.0));
                ui.label("Payload:");
                ui.text_edit_singleline(&mut self.publish_payload);
                if ui.button("Publish 🚀").clicked() {
                    self.trigger_publish();
                }
            });
        });
    }

    fn render_server_stats(&mut self, ui: &mut egui::Ui) {
        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                ui.heading("📊 Server Metrics & Info");
                if ui.button("🔄 Refresh Stats").clicked() {
                    self.trigger_load_info();
                }
            });
            ui.separator();

            egui::ScrollArea::vertical().show(ui, |ui| {
                if self.server_info_raw.is_empty() {
                    ui.label("No server stats available. Connect to server to view stats.");
                } else {
                    ui.monospace(&self.server_info_raw);
                }
            });
        });
    }
}
