mod args;
mod connection;
mod server;

use std::{io, net::SocketAddr};

use log::LevelFilter;

use crate::{args::parse_args, server::Server};

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .map_err(io::Error::other)?;
    let (addr, workers) = parse_args(std::env::args_os()).unwrap_or_else(|e| {
        log::error!("Parse arg error: {e}");
        (SocketAddr::from(([0, 0, 0, 0], 6379)), 4)
    });
    log::info!("Using addr={}, wrokers={}", addr, workers);
    let mut serv = Server::new(addr, workers)?;

    if let Err(e) = serv.main_loop() {
        log::error!("main loop error: {}", e);
    };
    serv.cleanup()?;
    log::info!("Server shutdown");

    Ok(())
}
