use std::{
    iter::{Cloned, Enumerate},
    str::FromStr,
    sync::Arc,
};

use nom::{
    IResult, Input, Parser,
    combinator::{map, map_opt, map_res},
    error::{ErrorKind, ParseError},
    multi::{count, many1},
};

use crate::proto::ProtocolData;

#[derive(Debug)]
pub struct Tokens<'a>(&'a [Arc<[u8]>]);

impl Clone for Tokens<'_> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<'a> nom::Input for Tokens<'a> {
    type Item = Arc<[u8]>;
    type Iter = Cloned<std::slice::Iter<'a, Self::Item>>;
    type IterIndices = Enumerate<Self::Iter>;

    fn input_len(&self) -> usize {
        self.0.len()
    }

    fn take(&self, index: usize) -> Self {
        Self(&self.0[..index])
    }

    fn take_from(&self, index: usize) -> Self {
        Self(&self.0[index..])
    }

    fn take_split(&self, index: usize) -> (Self, Self) {
        (self.take_from(index), self.take(index))
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.0.iter().position(|x| predicate(Arc::clone(x)))
    }

    fn iter_elements(&self) -> Self::Iter {
        self.0.iter().cloned()
    }

    fn iter_indices(&self) -> Self::IterIndices {
        self.iter_elements().enumerate()
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        if count <= self.0.len() {
            Ok(count)
        } else {
            Err(nom::Needed::new(count - self.0.len()))
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Get {
        key: Arc<[u8]>,
    },
    Set {
        key: Arc<[u8]>,
        val: Arc<[u8]>,
    },
    Del {
        keys: Arc<[Arc<[u8]>]>,
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

impl Command {
    pub fn is_write(&self) -> bool {
        match self {
            Command::Set { key: _, val: _ }
            | Command::Del { keys: _ }
            | Command::Zadd {
                key: _,
                score: _,
                name: _,
            }
            | Command::Zrem { key: _, name: _ } => true,
            _ => false,
        }
    }
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
    let tokens = Tokens(&valid_args);
    let mut cmd = Arc::clone(&valid_args[0]);
    Arc::make_mut(&mut cmd).make_ascii_uppercase();
    match cmd.as_ref() {
        b"GET" => parse_get(tokens).ok().map(|x| x.1),
        b"SET" => parse_set(tokens).ok().map(|x| x.1),
        b"DEL" => parse_del(tokens).ok().map(|x| x.1),
        b"HELLO" => Some(Command::Hello),
        b"KEYS" => Some(Command::Keys),
        b"PEXPIRE" => parse_pexpire(tokens).ok().map(|x| x.1),
        b"PTTL" => parse_pttl(tokens).ok().map(|x| x.1),
        b"ZADD" => parse_zadd(tokens).ok().map(|x| x.1),
        b"ZSCORE" => parse_zscore(tokens).ok().map(|x| x.1),
        b"ZREM" => parse_zrem(tokens).ok().map(|x| x.1),
        b"ZQUERY" => parse_zquery(tokens).ok().map(|x| x.1),
        _ => None,
    }
}

fn parse_get(t: Tokens) -> IResult<Tokens, Command> {
    map((token_eq(b"GET"), any_token), |(_, key)| Command::Get {
        key,
    })
    .parse(t)
}

fn parse_set(t: Tokens) -> IResult<Tokens, Command> {
    map((token_eq(b"SET"), any_token, any_token), |(_, key, val)| {
        Command::Set { key, val }
    })
    .parse(t)
}

fn parse_del(t: Tokens) -> IResult<Tokens, Command> {
    map((token_eq(b"DEL"), many1(any_token)), |(_, v)| {
        Command::Del {
            keys: Arc::from(v.into_boxed_slice()),
        }
    })
    .parse(t)
}

fn parse_pexpire(t: Tokens) -> IResult<Tokens, Command> {
    map_res(
        (token_eq(b"PEXPIRE"), any_token, any_token),
        |(_, key, s_ttl)| {
            u64::from_str_radix(String::from_utf8_lossy(&s_ttl).as_ref(), 10).map(|ttl| {
                Command::Pexpire {
                    key,
                    expire_ms: ttl,
                }
            })
        },
    )
    .parse(t)
}

fn parse_pttl(t: Tokens) -> IResult<Tokens, Command> {
    map((token_eq(b"PTTL"), any_token), |(_, key)| Command::Pttl {
        key,
    })
    .parse(t)
}

fn parse_zadd(t: Tokens) -> IResult<Tokens, Command> {
    map_res(
        (token_eq(b"ZADD"), any_token, any_token, any_token),
        |(_, key, s, name)| {
            f64::from_str(String::from_utf8_lossy(&s).as_ref()).map(|score| Command::Zadd {
                key,
                score,
                name,
            })
        },
    )
    .parse(t)
}

fn parse_zscore(t: Tokens) -> IResult<Tokens, Command> {
    map(
        (token_eq(b"ZSCORE"), any_token, any_token),
        |(_, key, name)| Command::Zscore { key, name },
    )
    .parse(t)
}

fn parse_zrem(t: Tokens) -> IResult<Tokens, Command> {
    map(
        (token_eq(b"ZREM"), any_token, any_token),
        |(_, key, name)| Command::Zrem { key, name },
    )
    .parse(t)
}

fn parse_zquery(t: Tokens) -> IResult<Tokens, Command> {
    map_opt((token_eq(b"ZQUERY"), count(any_token, 5)), |(_, v)| {
        f64::from_str(String::from_utf8_lossy(&v[1]).as_ref())
            .ok()
            .zip(i64::from_str_radix(String::from_utf8_lossy(&v[3]).as_ref(), 10).ok())
            .zip(usize::from_str_radix(String::from_utf8_lossy(&v[4]).as_ref(), 10).ok())
            .map(|((score, offset), limit)| Command::Zquery {
                key: Arc::clone(&v[0]),
                score,
                name: Arc::clone(&v[2]),
                offset,
                limit,
            })
    })
    .parse(t)
}

// Helper to match a specific token (case-insensitive for Redis commands)
fn token_eq<'a, E>(expected: &[u8]) -> impl Parser<Tokens<'a>, Output = Arc<[u8]>, Error = E>
where
    E: ParseError<Tokens<'a>>,
{
    |input: Tokens<'a>| {
        if let Some(first) = input.0.first() {
            if first.as_ref().eq_ignore_ascii_case(expected) {
                let (rest, matched) = input.take_split(1);
                Ok((rest, Arc::clone(&matched.0[0])))
            } else {
                Err(nom::Err::Error(E::from_error_kind(input, ErrorKind::Tag)))
            }
        } else {
            Err(nom::Err::Error(E::from_error_kind(input, ErrorKind::Eof)))
        }
    }
}

// Any single token (for key, value, and arguments)
fn any_token<'a, E>(input: Tokens<'a>) -> IResult<Tokens<'a>, Arc<[u8]>, E>
where
    E: ParseError<Tokens<'a>>,
{
    if input.0.first().is_some() {
        let (rest, matched) = input.take_split(1);
        Ok((rest, Arc::clone(&matched.0[0])))
    } else {
        Err(nom::Err::Error(E::from_error_kind(
            input,
            nom::error::ErrorKind::Eof,
        )))
    }
}
