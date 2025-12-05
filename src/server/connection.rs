use std::io::{self, Read, Write};

use bytes::{BufMut, BytesMut};
use mio::net::TcpStream;

pub struct ConnState {
    pub sock: TcpStream,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
}

impl ConnState {
    pub fn new(sock: TcpStream) -> Self {
        Self {
            sock,
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
        }
    }

    pub fn read_sock(&mut self) -> io::Result<(usize, bool)> {
        let mut buf = [0u8; 4096];
        let mut nread: usize = 0;
        loop {
            match self.sock.read(&mut buf) {
                Ok(0) => break Ok((nread, true)),
                Ok(n) => {
                    self.read_buf.put(&buf[..n]);
                    nread += n;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break Ok((nread, false)),
                Err(e) => break Err(e),
            }
        }
    }

    pub fn write_sock(&mut self) -> io::Result<(usize, bool)> {
        if self.write_buf.is_empty() {
            return Ok((0, false));
        }
        let mut nwrite: usize = 0;
        loop {
            match self.sock.write(&self.write_buf) {
                Ok(0) => break Ok((nwrite, true)),
                Ok(n) => {
                    let _ = self.write_buf.split_to(n);
                    nwrite += n;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break Ok((nwrite, false)),
                Err(e) => break Err(e),
            }
        }
    }
}
