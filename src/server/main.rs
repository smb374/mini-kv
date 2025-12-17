mod args;
mod connection;
mod server;

use std::io;

use log::LevelFilter;

use crate::{args::parse_args, server::Server};

fn print_help() {
    eprintln!(
        "Usage: kv_server [-H <addr>|--host=<addr>] [-p <port>|--port=<port>] [-w <workers>|--workers=<workers>] [-d <dir>|--data_dir=<dir>]"
    );
    eprintln!("Usage: kv_server [-h|--help]");
}

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .map_err(io::Error::other)?;
    let sargs = match parse_args(std::env::args_os()) {
        Ok(Some(x)) => x,
        Ok(None) => {
            print_help();
            return Ok(());
        }
        Err(e) => {
            eprintln!("Failed to parse args: {e}");
            print_help();
            return Err(io::Error::other(e));
        }
    };
    log::info!(
        "Using addr={}, wrokers={}, data_dir={}",
        &sargs.addr,
        sargs.workers,
        &sargs.data_dir
    );
    let mut serv = Server::new(sargs)?;

    if let Err(e) = serv.main_loop() {
        log::error!("main loop error: {}", e);
    };
    serv.cleanup()?;
    log::info!("Server shutdown");

    Ok(())
}
