use bytes::BytesMut;
use std::io::{self, BufRead, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use cedis::resp::{RespParser, RespValue};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut host = "127.0.0.1".to_string();
    let mut port = 6379u16;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--host" | "-h" => {
                if i + 1 < args.len() {
                    host = args[i + 1].clone();
                    i += 1;
                }
            }
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    if let Ok(p) = args[i + 1].parse() {
                        port = p;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr).await?;
    eprintln!("Connected to {addr}");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        print!("cedis> ");
        io::stdout().flush()?;

        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break; // EOF
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Parse the line into tokens
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }

        // Build RESP array
        let cmd = RespValue::array(
            tokens
                .into_iter()
                .map(|t| RespValue::bulk_string(t.as_bytes().to_vec()))
                .collect(),
        );

        // Send command
        stream.write_all(&cmd.serialize()).await?;

        // Read response
        let mut buf = BytesMut::with_capacity(4096);
        loop {
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                eprintln!("Connection closed by server");
                return Ok(());
            }

            match RespParser::parse(&mut buf) {
                Ok(Some(response)) => {
                    print_resp_value(&response, 0);
                    break;
                }
                Ok(None) => continue,
                Err(e) => {
                    eprintln!("Protocol error: {e}");
                    break;
                }
            }
        }

        if line.to_uppercase() == "QUIT" {
            break;
        }
    }

    Ok(())
}

fn print_resp_value(value: &RespValue, indent: usize) {
    let prefix = " ".repeat(indent);
    match value {
        RespValue::SimpleString(s) => println!("{prefix}{s}"),
        RespValue::Error(s) => println!("{prefix}(error) {s}"),
        RespValue::Integer(n) => println!("{prefix}(integer) {n}"),
        RespValue::BulkString(None) => println!("{prefix}(nil)"),
        RespValue::BulkString(Some(data)) => {
            let s = String::from_utf8_lossy(data);
            println!("{prefix}\"{s}\"");
        }
        RespValue::Array(None) => println!("{prefix}(nil)"),
        RespValue::Array(Some(items)) => {
            if items.is_empty() {
                println!("{prefix}(empty array)");
            } else {
                for (i, item) in items.iter().enumerate() {
                    print!("{prefix}{}) ", i + 1);
                    print_resp_value_inline(item);
                }
            }
        }
    }
}

fn print_resp_value_inline(value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => println!("{s}"),
        RespValue::Error(s) => println!("(error) {s}"),
        RespValue::Integer(n) => println!("(integer) {n}"),
        RespValue::BulkString(None) => println!("(nil)"),
        RespValue::BulkString(Some(data)) => {
            let s = String::from_utf8_lossy(data);
            println!("\"{s}\"");
        }
        RespValue::Array(None) => println!("(nil)"),
        RespValue::Array(Some(items)) => {
            if items.is_empty() {
                println!("(empty array)");
            } else {
                println!();
                for (i, item) in items.iter().enumerate() {
                    print!("   {}) ", i + 1);
                    print_resp_value_inline(item);
                }
            }
        }
    }
}
