#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::Read;

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

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        stream.read(&mut buffer).unwrap();
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}

