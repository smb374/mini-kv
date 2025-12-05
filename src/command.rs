use std::{str::FromStr, sync::Arc};

use crate::proto::ProtocolData;

pub enum Command {
    Get {
        key: Arc<str>,
    },
    Set {
        key: Arc<str>,
        val: Arc<str>,
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

    let mut cow_args: Vec<Arc<str>> = Vec::new();
    for a in args.iter() {
        let ProtocolData::BulkString(Some(b)) = a else {
            break;
        };
        let s = String::from_utf8_lossy(b.as_ref());
        cow_args.push(Arc::from(s.as_ref()));
    }
    if cow_args.is_empty() {
        return None;
    }
    Arc::make_mut(&mut cow_args[0]).make_ascii_uppercase();

    match cow_args[0].as_ref() {
        "GET" => Some(Command::Get {
            key: Arc::clone(&cow_args[1]),
        }),
        "SET" => Some(Command::Set {
            key: Arc::clone(&cow_args[1]),
            val: Arc::clone(&cow_args[2]),
        }),
        "DEL" => Some(Command::Del {
            key: Arc::clone(&cow_args[1]),
        }),
        "KEYS" => Some(Command::Keys),
        "PEXPIRE" => u64::from_str_radix(cow_args[2].as_ref(), 10)
            .ok()
            .map(|x| Command::Pexpire {
                key: Arc::clone(&cow_args[1]),
                expire_ms: x,
            }),
        "PTTL" => Some(Command::Pttl {
            key: Arc::clone(&cow_args[1]),
        }),
        "ZADD" => f64::from_str(cow_args[2].as_ref())
            .ok()
            .map(|x| Command::Zadd {
                key: Arc::clone(&cow_args[1]),
                score: x,
                name: Arc::clone(&cow_args[3]),
            }),
        "ZSCORE" => Some(Command::Zscore {
            key: Arc::clone(&cow_args[1]),
            name: Arc::clone(&cow_args[2]),
        }),
        "ZREM" => Some(Command::Zrem {
            key: Arc::clone(&cow_args[1]),
            name: Arc::clone(&cow_args[2]),
        }),
        "ZQUERY" => f64::from_str(cow_args[2].as_ref())
            .ok()
            .zip(i64::from_str_radix(cow_args[4].as_ref(), 10).ok())
            .zip(usize::from_str_radix(cow_args[5].as_ref(), 10).ok())
            .map(|((x, y), z)| Command::Zquery {
                key: Arc::clone(&cow_args[1]),
                score: x,
                name: Arc::clone(&cow_args[3]),
                offset: y,
                limit: z,
            }),
        _ => None,
    }
}
