use std::{str::FromStr, sync::Arc};

use crate::proto::ProtocolData;

pub enum Command {
    Get {
        key: Arc<str>,
    },
    Set {
        key: Arc<str>,
        val: Arc<[u8]>,
    },
    Del {
        key: Arc<str>,
    },
    Keys,
    Pexpire {
        key: Arc<str>,
        expire_ms: u64,
    },
    Pttl {
        key: Arc<str>,
    },
    Zadd {
        key: Arc<str>,
        score: f64,
        name: Arc<str>,
    },
    Zscore {
        key: Arc<str>,
        name: Arc<str>,
    },
    Zrem {
        key: Arc<str>,
        name: Arc<str>,
    },
    Zquery {
        key: Arc<str>,
        score: f64,
        name: Arc<str>,
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
            key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
        }),
        "SET" if valid_args.len() >= 3 => Some(Command::Set {
            key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
            val: Arc::clone(&valid_args[2]),
        }),
        "DEL" if valid_args.len() >= 2 => Some(Command::Del {
            key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
        }),
        "KEYS" => Some(Command::Keys),
        "PEXPIRE" if valid_args.len() >= 3 => {
            u64::from_str_radix(String::from_utf8_lossy(valid_args[2].as_ref()).as_ref(), 10)
                .ok()
                .map(|x| Command::Pexpire {
                    key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
                    expire_ms: x,
                })
        }
        "PTTL" if valid_args.len() >= 2 => Some(Command::Pttl {
            key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
        }),
        "ZADD" if valid_args.len() >= 4 => {
            f64::from_str(String::from_utf8_lossy(valid_args[2].as_ref()).as_ref())
                .ok()
                .map(|x| Command::Zadd {
                    key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
                    score: x,
                    name: Arc::from(String::from_utf8_lossy(valid_args[3].as_ref())),
                })
        }
        "ZSCORE" if valid_args.len() >= 3 => Some(Command::Zscore {
            key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
            name: Arc::from(String::from_utf8_lossy(valid_args[2].as_ref())),
        }),
        "ZREM" if valid_args.len() >= 3 => Some(Command::Zrem {
            key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
            name: Arc::from(String::from_utf8_lossy(valid_args[2].as_ref())),
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
                    key: Arc::from(String::from_utf8_lossy(valid_args[1].as_ref())),
                    score: x,
                    name: Arc::from(String::from_utf8_lossy(valid_args[3].as_ref())),
                    offset: y,
                    limit: z,
                })
        }
        _ => None,
    }
}
