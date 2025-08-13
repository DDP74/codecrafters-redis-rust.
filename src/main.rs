#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::{Read, BufReader, BufRead};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let thread_handle = std::thread::spawn(move ||handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream) ->  Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut buffer = [0; 4096];

    loop {
        let bsize = stream.read(&mut buffer)?;

        if bsize == 0 {
            println!("No data received");
            break;
        }

        println!("Received {} bytes", bsize);
        println!("Buffer: {:?}", &buffer[..bsize]);
        // The first byte of a RESP message indicates its type.
        // For commands, it's always an array, which starts with '*'.

        if char::from(buffer[0]) == '*' {

            let command_parts = parse_resp_array(&buffer).unwrap_or_default();
            println!("Command parts: {:#?}", command_parts);

            if command_parts.is_empty() {
                continue;
            }

            let command = command_parts[0].to_uppercase();

            match command.as_str() {
                "PING" => {
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                _ => {
                    // Command not implemented yet
                }
            }
        }
    }

    Ok(())
}

/// Parses a RESP array from the reader into a Vec of strings.
fn parse_resp_array(buffer: &[u8]) -> Result<Vec<String>, std::io::Error> {
    let mut reader = BufReader::new(buffer);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let num_elements = line.trim_end().parse::<usize>().unwrap_or(0);

    let mut parts = Vec::with_capacity(num_elements);
    for _ in 0..num_elements {
        // Each part of an array is a Bulk String, starting with '$'.
        let mut first_byte = [0; 1];
        reader.read(&mut first_byte)?;
        if first_byte[0] == b'$' {
            line.clear();
            reader.read_line(&mut line)?;
            let len = line.trim_end().parse::<usize>().unwrap_or(0);

            let mut buf = vec![0; len + 2]; // +2 for \r\n
            reader.read_exact(&mut buf)?;
            parts.push(String::from_utf8_lossy(&buf[..len]).to_string());
        }
    }
    Ok(parts)
}
