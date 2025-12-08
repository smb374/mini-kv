mod args;
mod connection;
mod server;

use std::{io, net::SocketAddr};

use crate::{args::parse_args, server::Server};

fn main() -> io::Result<()> {
    let (addr, workers) = parse_args(std::env::args_os()).unwrap_or_else(|e| {
        eprintln!("Parse arg error: {e}");
        (SocketAddr::from(([0, 0, 0, 0], 6379)), 4)
    });
    eprintln!("Using addr={}, wrokers={}", addr, workers);
    let mut serv = Server::new(addr, workers)?;

    if let Err(e) = serv.main_loop() {
        eprintln!("main loop error: {}", e);
    };
    serv.cleanup()?;
    eprintln!("Server shutdown");

    Ok(())
}
