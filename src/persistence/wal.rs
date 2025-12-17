use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::proto::ProtocolData;

const RECORD_HEADER_SIZE: usize = 7;
const BLOCK_SIZE: usize = 32768;
const SYNC_BLOCKS_THRES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WALType {
    Full = 1,
    First = 2,
    Mid = 3,
    Last = 4,
}

#[derive(Debug)]
pub struct WALRecord {
    checksum: u32,
    len: u16,
    pub typ: WALType,
    pub data: Bytes,
}

enum Record {
    Single(WALRecord),
    Multi(Vec<WALRecord>),
}

#[derive(Debug)]
pub struct WALWriter {
    block_buf: Box<[u8]>,
    cursor: usize,
    wblks: usize,
    written: usize,
    f: File,
    path: String,
}

pub struct WALReader {
    block_buf: Box<[u8]>,
    cursor: usize,
    rblks: usize,
    f: File,
    path: String,
}

#[derive(Debug, thiserror::Error)]
pub enum WALReadError {
    #[error("Failed to read block: {0}")]
    Io(io::Error),
    #[error("Invalid record type {0}")]
    InvalidType(u8),
    #[error("Invalid len {0} for payload, block has {1} remaining")]
    InvalidLen(usize, usize),
    #[error("Invalid payload checksum, expect {0:08X}, got {1:08X}")]
    InvalidChecksum(u32, u32),
}

impl WALRecord {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32_le(self.checksum);
        buf.put_u16_le(self.len);
        buf.put_u8(self.typ as u8);
        buf.put_slice(&self.data);
    }
}

impl WALReader {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let path = format!("{}/wal.log", dir.as_ref().to_string_lossy());
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        f.seek(SeekFrom::Start(0))?;
        let mut reader = Self {
            f,
            path,
            block_buf: crate::alloc_box_buffer(BLOCK_SIZE),
            cursor: 0,
            rblks: 0,
        };
        reader.read_blk()?;
        Ok(reader)
    }

    fn read_blk(&mut self) -> io::Result<()> {
        log::debug!("WALReader: read blk {}", self.rblks + 1);
        self.f.read_exact(&mut self.block_buf)?;
        self.cursor = 0;
        self.rblks += 1;
        Ok(())
    }

    pub fn decode_one(&mut self) -> Result<WALRecord, WALReadError> {
        if self.cursor + RECORD_HEADER_SIZE > BLOCK_SIZE {
            // End of block, read a new one
            self.read_blk().map_err(WALReadError::Io)?;
        }
        let mut buf = &self.block_buf[self.cursor..];
        let csum = buf.get_u32_le();
        let len = buf.get_u16_le();
        let typ = buf.get_u8();
        let space = BLOCK_SIZE - RECORD_HEADER_SIZE - self.cursor;
        if len as usize > space {
            return Err(WALReadError::InvalidLen(len as usize, space));
        }
        let payload = buf.copy_to_bytes(len as usize);
        let wtyp = match typ {
            1 => WALType::Full,
            2 => WALType::First,
            3 => WALType::Mid,
            4 => WALType::Last,
            _ => return Err(WALReadError::InvalidType(typ)),
        };
        let psum = crc32c::crc32c(&payload);
        if csum != psum {
            return Err(WALReadError::InvalidChecksum(csum, psum));
        }
        self.cursor += RECORD_HEADER_SIZE + len as usize;
        Ok(WALRecord {
            checksum: csum,
            len,
            typ: wtyp,
            data: payload,
        })
    }

    pub fn into_writer(mut self) -> io::Result<WALWriter> {
        let loc = (self.rblks - 1) * BLOCK_SIZE;
        self.f.seek(SeekFrom::Start(loc as u64))?;
        Ok(WALWriter {
            block_buf: self.block_buf,
            cursor: self.cursor,
            wblks: 0,
            written: 0,
            f: self.f,
            path: self.path,
        })
    }
}

impl WALWriter {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let path = format!("{}/wal.log", dir.as_ref().to_string_lossy());
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            f,
            path,
            block_buf: crate::alloc_box_buffer(BLOCK_SIZE),
            cursor: 0,
            wblks: 0,
            written: 0,
        })
    }

    pub fn flush(&mut self, force_sync: bool) -> io::Result<()> {
        if self.written != 0 {
            self.block_buf[self.cursor..].fill(0u8);
            self.f.write_all(&self.block_buf)?;
            self.wblks += 1;
            self.cursor = 0;
            if force_sync || self.wblks % SYNC_BLOCKS_THRES == 0 {
                self.f.sync_all()?;
            }
        }
        Ok(())
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.flush(true)?;
        self.f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        Ok(())
    }

    pub fn append(&mut self, proto: ProtocolData) -> io::Result<usize> {
        if self.cursor + RECORD_HEADER_SIZE > BLOCK_SIZE {
            self.flush(false)?;
        }
        let record = self.create_record(proto);
        let mut sz = 0;
        match record {
            Record::Single(r) => {
                let mut buf = &mut self.block_buf[self.cursor..];
                r.encode(&mut buf);
                sz = RECORD_HEADER_SIZE + r.len as usize;
                self.cursor += sz;
            }
            Record::Multi(v) => {
                v.iter().try_for_each(|r| -> io::Result<()> {
                    let mut buf = &mut self.block_buf[self.cursor..];
                    r.encode(&mut buf);
                    sz += RECORD_HEADER_SIZE + r.len as usize;
                    self.cursor += RECORD_HEADER_SIZE + r.len as usize;
                    if self.cursor + RECORD_HEADER_SIZE > BLOCK_SIZE {
                        self.flush(false)?;
                    }
                    Ok(())
                })?;
            }
        }
        self.written += sz;
        Ok(sz)
    }

    fn create_record(&self, proto: ProtocolData) -> Record {
        let mut buf = BytesMut::with_capacity(4096);
        proto.encode(&mut buf);
        let mut data = buf.freeze();
        let space = BLOCK_SIZE - self.cursor;
        if space >= RECORD_HEADER_SIZE && data.len() + RECORD_HEADER_SIZE > space {
            let fst_data = data.split_to(space - RECORD_HEADER_SIZE);
            let mut v = vec![WALRecord {
                checksum: crc32c::crc32c(&fst_data),
                len: fst_data.len() as u16,
                typ: WALType::First,
                data: fst_data,
            }];
            while data.len() > BLOCK_SIZE - RECORD_HEADER_SIZE {
                let mdat = data.split_to(BLOCK_SIZE - RECORD_HEADER_SIZE);
                v.push(WALRecord {
                    checksum: crc32c::crc32c(&mdat),
                    len: mdat.len() as u16,
                    typ: WALType::Mid,
                    data: mdat,
                });
            }
            v.push(WALRecord {
                checksum: crc32c::crc32c(&data),
                len: data.len() as u16,
                typ: WALType::Last,
                data,
            });
            Record::Multi(v)
        } else {
            Record::Single(WALRecord {
                checksum: crc32c::crc32c(&data),
                len: data.len() as u16,
                typ: WALType::Full,
                data,
            })
        }
    }
}
