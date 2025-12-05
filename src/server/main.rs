mod connection;
mod server;

use std::io;

use crate::server::Server;

fn main() -> io::Result<()> {
    let mut serv = Server::new("0.0.0.0:6379".parse().unwrap(), 4)?;

    if let Err(e) = serv.main_loop() {
        eprintln!("main loop error: {}", e);
    };
    serv.cleanup()?;
    eprintln!("Server shutdown");

    Ok(())
}
