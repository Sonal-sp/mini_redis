use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use mini_redis_core::{
    start_active_expiration_worker, Aof, Command, Connection, Db, Frame,
};

#[derive(Parser, Debug)]
#[command(name = "mini-redis-server", author, version, about = "High-performance Mini Redis Server")]
struct Cli {
    /// Bind host address
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Bind port
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Path to Append-Only File (AOF) for persistence
    #[arg(long, default_value = "appendonly.aof")]
    aof_path: PathBuf,

    /// Disable AOF persistence
    #[arg(long, default_value_t = false)]
    no_aof: bool,

    /// Active TTL eviction check interval in milliseconds
    #[arg(long, default_value_t = 100)]
    ttl_interval_ms: u64,
}

fn print_banner(host: &str, port: u16, aof_enabled: bool) {
    println!(
        r#"
  __  __ _       _       ____          _ _      
 |  \/  (_)_ __ (_)     |  _ \ ___  __| (_)___  
 | |\/| | | '_ \| |_____| |_) / _ \/ _` | / __| 
 | |  | | | | | | |_____|  _ <  __/ (_| | \__ \ 
 |_|  |_|_|_| |_|_|     |_| \_\___|\__,_|_|___/ 
 
 Mini Redis Server v0.1.0 (Rust Tokio Async Engine)
 ----------------------------------------------------
 > Running in standalone mode
 > Server initialized on {}:{}
 > AOF Persistence: {}
 > Press Ctrl+C to stop the server
"#,
        host,
        port,
        if aof_enabled { "Enabled" } else { "Disabled" }
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber with INFO default
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mini_redis_server=info,mini_redis_core=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);

    let db = Db::new();

    // Setup AOF if enabled
    let aof = if !cli.no_aof {
        info!("AOF persistence enabled: {:?}", cli.aof_path);
        // Replay existing AOF
        match Aof::replay_direct(&cli.aof_path, &db) {
            Ok(count) => {
                if count > 0 {
                    info!("Restored {} commands from AOF log file", count);
                }
            }
            Err(e) => {
                warn!("Could not replay AOF log: {}", e);
            }
        }

        match Aof::open(&cli.aof_path) {
            Ok(aof_instance) => Some(Arc::new(aof_instance)),
            Err(e) => {
                error!("Failed to open AOF file {:?}: {}", cli.aof_path, e);
                None
            }
        }
    } else {
        info!("AOF persistence is disabled");
        None
    };

    // Start background active TTL eviction worker
    start_active_expiration_worker(db.clone(), Duration::from_millis(cli.ttl_interval_ms));

    // Bind TCP listener
    let listener = TcpListener::bind(&addr).await?;
    print_banner(&cli.host, cli.port, aof.is_some());

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((socket, peer_addr)) => {
                        db.increment_connections();
                        let db_clone = db.clone();
                        let aof_clone = aof.clone();

                        tokio::spawn(async move {
                            if let Err(e) = process_connection(socket, peer_addr, db_clone, aof_clone).await {
                                warn!("Connection error from {}: {}", peer_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received, gracefully terminating Mini Redis server...");
                break;
            }
        }
    }

    Ok(())
}

async fn process_connection(
    socket: TcpStream,
    _peer_addr: SocketAddr,
    db: Db,
    aof: Option<Arc<Aof>>,
) -> Result<()> {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await? {
        db.increment_commands();

        let cmd = match Command::from_frame(frame.clone()) {
            Ok(cmd) => cmd,
            Err(e) => {
                let err_frame = Frame::Error(format!("ERR {}", e));
                connection.write_frame(&err_frame).await?;
                continue;
            }
        };

        match cmd {
            Command::Subscribe(sub) => {
                // Enter Pub/Sub subscription mode
                handle_pubsub_session(&mut connection, sub.channels(), &db).await?;
                return Ok(());
            }
            _ => {
                // If it's a write command and AOF is enabled, append to AOF
                if cmd.is_write_command() {
                    if let Some(ref aof_engine) = aof {
                        if let Some(aof_frame) = cmd.to_frame() {
                            if let Err(e) = aof_engine.append_frame(&aof_frame) {
                                error!("Failed to append command to AOF: {}", e);
                            }
                        }
                    }
                }

                // Apply command and send response
                let response = cmd.apply(&db);
                connection.write_frame(&response).await?;
            }
        }
    }

    Ok(())
}

/// Handles a connection that has entered SUBSCRIBE mode.
async fn handle_pubsub_session(
    connection: &mut Connection,
    channels: &[String],
    db: &Db,
) -> Result<()> {
    // Send subscription confirmation frames
    let mut receivers = Vec::new();

    for (i, channel) in channels.iter().enumerate() {
        let rx = db.subscribe(channel);
        receivers.push((channel.clone(), rx));

        let confirm_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("subscribe")),
            Frame::Bulk(Bytes::from(channel.clone())),
            Frame::Integer((i + 1) as i64),
        ]);
        connection.write_frame(&confirm_frame).await?;
    }

    // Combine receivers and read stream
    let mut stream_map = tokio_stream::StreamMap::new();
    for (channel, rx) in receivers {
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
        stream_map.insert(channel, stream);
    }

    loop {
        tokio::select! {
            Some((channel, Ok(msg))) = stream_map.next() => {
                let msg_frame = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("message")),
                    Frame::Bulk(Bytes::from(channel)),
                    Frame::Bulk(msg),
                ]);
                connection.write_frame(&msg_frame).await?;
            }
            maybe_frame = connection.read_frame() => {
                match maybe_frame? {
                    Some(frame) => {
                        let cmd = match Command::from_frame(frame) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                connection.write_frame(&Frame::Error(format!("ERR {}", e))).await?;
                                continue;
                            }
                        };

                        match cmd {
                            Command::Ping(p) => {
                                let pong = p.apply();
                                connection.write_frame(&pong).await?;
                            }
                            Command::Subscribe(sub) => {
                                for channel in sub.channels() {
                                    let rx = db.subscribe(channel);
                                    let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
                                    stream_map.insert(channel.clone(), stream);

                                    let confirm_frame = Frame::Array(vec![
                                        Frame::Bulk(Bytes::from("subscribe")),
                                        Frame::Bulk(Bytes::from(channel.clone())),
                                        Frame::Integer(stream_map.len() as i64),
                                    ]);
                                    connection.write_frame(&confirm_frame).await?;
                                }
                            }
                            _ => {
                                connection.write_frame(&Frame::Error("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context".into())).await?;
                            }
                        }
                    }
                    None => break, // Client disconnected
                }
            }
        }
    }

    Ok(())
}
