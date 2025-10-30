#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::{Read, BufReader, BufRead};
use std::ptr::read;
use bytes::BufMut;

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
                std::thread::spawn(move ||handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream) ->  Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut buffer_in = Vec::new();
    let mut buffer_out: Vec<u8> = Vec::new();

    loop {
        println!("Waiting for data...");

        let bsize = stream.read_to_end(&mut buffer_in).unwrap();

        if bsize == 0 {
            println!("No data from client received");
            break;
        }

        println!("Received {} bytes", bsize);
        println!("Buffer: {:?}", &buffer_in[..bsize]);

        // The first byte of a RESP message indicates its type.
        // For commands, it's always an array, which starts with '*'.

        if char::from(buffer_in[0]) == '*' {

            let command_parts = parse_resp_array(&buffer_in).unwrap();
            println!("Command parts: {:#?}", command_parts);

            if command_parts.is_empty() {
                continue;
            }

            let command = command_parts[0].to_uppercase();

            match command.as_str() {
                "PING" => {
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                "ECHO" => {
                    buffer_out.clear();
                    buffer_out.extend_from_slice(b"$");
                    buffer_out.extend_from_slice(command_parts[1].len().to_string().as_bytes());
                    buffer_out.extend_from_slice(b"\r\n");
                    buffer_out.extend_from_slice(command_parts[1].as_bytes());
                    buffer_out.extend_from_slice(b"\r\n");
                    println!("Echoing: {:?}", String::from_utf8_lossy(&buffer_out));
                    stream.write_all(&buffer_out).unwrap();
                    //stream.write_all(b"$3\r\n").unwrap();
                    //stream.write_all(command_parts[1].as_bytes()).unwrap();
                    //stream.write_all(b"\r\n").unwrap();
                }
                _ => {
                    println!("Unknown command: {}", command);
                    stream.write_all(b"-ERR unknown command\r\n").unwrap();
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
    let lines   = reader.lines();
    //println!("Lines: {:#?}", lines);

    let mut parts = Vec::new();

    let mut arrays_command_length = 0;
    let mut bulk_string_length = 0;
    for ( index ,mut line) in lines.enumerate() {
        println!("Index: {:#?}", index);
        println!("Line: {:#?}", line);

        let mut line = line.unwrap();
        let mut chars = line.chars();
        // The first byte of a RESP message indicates its type.
        // For commands, it's always an array, which starts with '*'.
        let mut char = chars.next().unwrap();
        println!("Char --->: {:#?}", char);
        if index == 0 && char  != '*' {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid data"));
        } else if index == 0 && char == '*' {
                char = chars.next().unwrap();
                if char.is_digit(10) {
                    arrays_command_length = char.to_digit(10).unwrap() as usize;
                    println!("Arrays command length: {:#?}", arrays_command_length);
                }
            continue;
        }
        if char == '$' {
            char = chars.next().unwrap();
            if char.is_digit(10) {
                bulk_string_length = char.to_digit(10).unwrap() as usize;
                println!("Bulk string length: {:#?}", bulk_string_length);
            }
            continue;
        }
        println!("Condition char != '$' || char != '*' : {:#?}", !char.eq(& '$') || char.eq(& '*'));
        if char != '$' || char != '*' {
            println!("Command to push: {:#?}", line);
            if bulk_string_length != line.trim_end_matches('\r').trim_end_matches('\n').len() {
                println!("Bulk string: {:#?}", line);
            }
            parts.push(line);
        }
    }
    Ok(parts)
}
