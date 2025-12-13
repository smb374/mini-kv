use std::{str::FromStr, sync::Arc};

use crate::proto::ProtocolData;

pub enum Command {
    Get {
        key: Arc<[u8]>,
    },
    Set {
        key: Arc<[u8]>,
        val: Arc<[u8]>,
    },
    Del {
        key: Arc<[u8]>,
    },
    Keys,
    Hello,
    Pexpire {
        key: Arc<[u8]>,
        expire_ms: u64,
    },
    Pttl {
        key: Arc<[u8]>,
    },
    Zadd {
        key: Arc<[u8]>,
        score: f64,
        name: Arc<[u8]>,
    },
    Zscore {
        key: Arc<[u8]>,
        name: Arc<[u8]>,
    },
    Zrem {
        key: Arc<[u8]>,
        name: Arc<[u8]>,
    },
    Zquery {
        key: Arc<[u8]>,
        score: f64,
        name: Arc<[u8]>,
        offset: i64,
        limit: usize,
    },
}

pub fn parse_command(dat: &ProtocolData) -> Option<Command> {
    let ProtocolData::Array(Some(args)) = dat else {
        return None;
    };

    let mut valid_args: Vec<Arc<[u8]>> = Vec::new();
    for a in args.iter() {
        let ProtocolData::BulkString(Some(s)) = a else {
            break;
        };
        valid_args.push(Arc::clone(s));
    }
    if valid_args.is_empty() {
        return None;
    }
    let mut cmd = String::from_utf8_lossy(valid_args[0].as_ref());
    cmd.to_mut().make_ascii_uppercase();

    match cmd.as_ref() {
        "GET" if valid_args.len() >= 2 => Some(Command::Get {
            key: Arc::clone(&valid_args[1]),
        }),
        "SET" if valid_args.len() >= 3 => Some(Command::Set {
            key: Arc::clone(&valid_args[1]),
            val: Arc::clone(&valid_args[2]),
        }),
        "DEL" if valid_args.len() >= 2 => Some(Command::Del {
            key: Arc::clone(&valid_args[1]),
        }),
        "KEYS" => Some(Command::Keys),
        "HELLO" => Some(Command::Hello),
        "PEXPIRE" if valid_args.len() >= 3 => {
            u64::from_str_radix(String::from_utf8_lossy(valid_args[2].as_ref()).as_ref(), 10)
                .ok()
                .map(|x| Command::Pexpire {
                    key: Arc::clone(&valid_args[1]),
                    expire_ms: x,
                })
        }
        "PTTL" if valid_args.len() >= 2 => Some(Command::Pttl {
            key: Arc::clone(&valid_args[1]),
        }),
        "ZADD" if valid_args.len() >= 4 => {
            f64::from_str(String::from_utf8_lossy(valid_args[2].as_ref()).as_ref())
                .ok()
                .map(|x| Command::Zadd {
                    key: Arc::clone(&valid_args[1]),
                    score: x,
                    name: Arc::clone(&valid_args[3]),
                })
        }
        "ZSCORE" if valid_args.len() >= 3 => Some(Command::Zscore {
            key: Arc::clone(&valid_args[1]),
            name: Arc::clone(&valid_args[2]),
        }),
        "ZREM" if valid_args.len() >= 3 => Some(Command::Zrem {
            key: Arc::clone(&valid_args[1]),
            name: Arc::clone(&valid_args[2]),
        }),
        "ZQUERY" if valid_args.len() >= 6 => {
            f64::from_str(String::from_utf8_lossy(valid_args[2].as_ref()).as_ref())
                .ok()
                .zip(
                    i64::from_str_radix(
                        String::from_utf8_lossy(valid_args[4].as_ref()).as_ref(),
                        10,
                    )
                    .ok(),
                )
                .zip(
                    usize::from_str_radix(
                        String::from_utf8_lossy(valid_args[5].as_ref()).as_ref(),
                        10,
                    )
                    .ok(),
                )
                .map(|((x, y), z)| Command::Zquery {
                    key: Arc::clone(&valid_args[1]),
                    score: x,
                    name: Arc::clone(&valid_args[3]),
                    offset: y,
                    limit: z,
                })
        }
        _ => None,
    }
}
