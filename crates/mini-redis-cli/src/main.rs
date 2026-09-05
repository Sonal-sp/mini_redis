use anyhow::Result;
use clap::Parser;
use colored::*;
use mini_redis_core::{Client, Frame};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};
use std::borrow::Cow;

#[derive(Parser, Debug)]
#[command(name = "mini-redis-cli", author, version, about = "Mini Redis Command Line Interface")]
struct Cli {
    /// Server host
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Command to execute directly (if not provided, enters interactive REPL mode)
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

const COMMAND_LIST: &[&str] = &[
    "PING", "GET", "SET", "DEL", "EXPIRE", "PERSIST", "TTL", "KEYS", "EXISTS", "TYPE",
    "DBSIZE", "FLUSHDB", "INCR", "DECR", "INCRBY", "DECRBY", "HSET", "HGET", "HGETALL",
    "HDEL", "LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN", "PUBLISH", "SUBSCRIBE",
    "INFO", "QUIT", "EXIT", "HELP", "CLEAR",
];

struct RedisHelper;

impl Completer for RedisHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let mut candidates = Vec::new();
        let current_word = &line[..pos].trim_start();

        if !current_word.contains(' ') {
            let upper = current_word.to_uppercase();
            for cmd in COMMAND_LIST {
                if cmd.starts_with(&upper) {
                    candidates.push(Pair {
                        display: cmd.to_string(),
                        replacement: format!("{} ", cmd),
                    });
                }
            }
        }

        Ok((0, candidates))
    }
}

impl Hinter for RedisHelper {
    type Hint = String;
    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for RedisHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Owned(prompt.bright_green().bold().to_string())
    }
}

impl Validator for RedisHelper {}
impl Helper for RedisHelper {}

fn format_frame_output(frame: &Frame) -> String {
    match frame {
        Frame::Simple(s) => s.green().bold().to_string(),
        Frame::Error(e) => format!("(error) {}", e).red().bold().to_string(),
        Frame::Integer(i) => format!("(integer) {}", i).yellow().to_string(),
        Frame::Bulk(b) => match String::from_utf8(b.to_vec()) {
            Ok(s) => format!("\"{}\"", s).bright_cyan().to_string(),
            Err(_) => format!("{:?}", b).bright_cyan().to_string(),
        },
        Frame::Null => "(nil)".bright_magenta().italic().to_string(),
        Frame::NullArray => "(empty array)".bright_magenta().italic().to_string(),
        Frame::Array(arr) => {
            if arr.is_empty() {
                return "(empty list or set)".bright_magenta().italic().to_string();
            }
            let mut out = String::new();
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push('\n');
                }
                out.push_str(&format!(
                    "{}) {}",
                    (i + 1).to_string().bright_blue(),
                    format_frame_output(item)
                ));
            }
            out
        }
    }
}

fn print_help() {
    println!(
        "{}",
        r#"
Mini Redis CLI - Supported Commands:
--------------------------------------------------------------------------------
Key-Value:
  SET key value [EX seconds]     Set key to hold string value
  GET key                        Get value of key
  DEL key [key ...]              Delete one or more keys
  EXISTS key [key ...]           Check if key(s) exist
  EXPIRE key seconds             Set time-to-live for a key
  TTL key                        Get remaining time-to-live in seconds
  PERSIST key                    Remove key expiration
  TYPE key                       Get data type of key
  KEYS [pattern]                 Find keys matching pattern (e.g. KEYS *)
  INCR key / DECR key            Increment/decrement integer value
  INCRBY key delta               Increment integer value by delta

Hash:
  HSET key field value           Set hash field
  HGET key field                 Get hash field value
  HGETALL key                    Get all fields and values in hash
  HDEL key field [field ...]     Delete one or more hash fields

List:
  LPUSH / RPUSH key val [val..]  Prepend / append values to list
  LPOP / RPOP key [count]        Remove & return elements from list
  LRANGE key start stop          Get range of elements from list
  LLEN key                       Get length of list

Pub/Sub:
  PUBLISH channel message        Post a message to a channel
  SUBSCRIBE channel [chan ...]   Listen for messages on channel(s)

Server:
  PING [message]                 Ping server (returns PONG or message)
  DBSIZE                         Get total number of keys in database
  FLUSHDB                        Delete all keys in the current database
  INFO                           Get server metrics and information
  CLEAR                          Clear terminal screen
  QUIT / EXIT                    Exit the interactive CLI
--------------------------------------------------------------------------------
"#
        .bright_yellow()
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);

    // Direct command execution mode (e.g. mini-redis-cli set foo bar)
    if !cli.command.is_empty() {
        let mut client = match Client::connect(&addr).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "{} Could not connect to Mini Redis at {}: {}",
                    "Error:".red().bold(),
                    addr,
                    e
                );
                std::process::exit(1);
            }
        };

        let frames: Vec<Frame> = cli.command.into_iter().map(Frame::from_string).collect();

        match client.send_frame(Frame::Array(frames)).await {
            Ok(frame) => {
                println!("{}", format_frame_output(&frame));
            }
            Err(e) => {
                eprintln!("(error) {}", e);
                std::process::exit(1);
            }
        }
        return Ok(());
    }

    // Interactive REPL Mode
    println!(
        "{} {}",
        "Connected to Mini Redis at".bright_green(),
        addr.bright_cyan().bold()
    );
    println!("Type {} for list of commands, {} to exit.\n", "HELP".yellow().bold(), "QUIT".yellow().bold());

    let mut client = match Client::connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "{} Could not connect to Mini Redis at {}: {}",
                "Error:".red().bold(),
                addr,
                e
            );
            std::process::exit(1);
        }
    };

    let mut rl = Editor::new()?;
    rl.set_helper(Some(RedisHelper));
    let history_file = ".mini_redis_history";
    let _ = rl.load_history(history_file);

    let prompt = format!("{}> ", addr);

    loop {
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                let upper = trimmed.to_uppercase();
                if upper == "QUIT" || upper == "EXIT" {
                    println!("{}", "Goodbye!".bright_cyan());
                    break;
                } else if upper == "CLEAR" {
                    print!("\x1B[2J\x1B[1;1H");
                    continue;
                } else if upper == "HELP" {
                    print_help();
                    continue;
                }

                // Check if SUBSCRIBE command is run in REPL
                if upper.starts_with("SUBSCRIBE ") {
                    println!("{}", "Entering Pub/Sub subscription mode (press Ctrl+C to exit subscription)...".bright_yellow());
                    match client.execute_raw(trimmed).await {
                        Ok(initial_resp) => {
                            println!("{}", format_frame_output(&initial_resp));
                            // Loop reading messages
                            loop {
                                match client.read_frame().await {
                                    Ok(Some(msg_frame)) => {
                                        println!("{}", format_frame_output(&msg_frame));
                                    }
                                    Ok(None) => {
                                        println!("{}", "Server closed connection".red());
                                        break;
                                    }
                                    Err(e) => {
                                        println!("Subscription error: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => println!("(error) {}", e),
                    }
                    continue;
                }

                match client.execute_raw(trimmed).await {
                    Ok(frame) => {
                        println!("{}", format_frame_output(&frame));
                    }
                    Err(e) => {
                        println!("(error) {}", e);
                        // Try to reconnect if connection was dropped
                        if let Ok(new_client) = Client::connect(&addr).await {
                            client = new_client;
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "Goodbye!".bright_cyan());
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    let _ = rl.save_history(history_file);
    Ok(())
}
