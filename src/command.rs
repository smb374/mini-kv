use std::{
    iter::{Cloned, Enumerate},
    sync::Arc,
};

use nom::{
    IResult, Input, Parser,
    branch::alt,
    combinator::{map, map_res},
    error::{ErrorKind, ParseError},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetCond {
    XX,
    NX,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireArg {
    EX(u64),
    PX(u64),
    EXAT(u64),
    PXAT(u64),
    KEEPTTL,
}

#[derive(Debug, Clone)]
pub struct SetArgs {
    pub key: Arc<[u8]>,
    pub val: Arc<[u8]>,
    pub cond: Option<SetCond>,
    pub get_old: bool,
    pub expire: Option<ExpireArg>,
}

impl SetArgs {
    pub fn new(key: Arc<[u8]>, val: Arc<[u8]>) -> Self {
        Self {
            key,
            val,
            cond: None,
            get_old: false,
            expire: None,
        }
    }
}

pub enum Command {
    Get {
        key: Arc<[u8]>,
    },
    OldSet {
        key: Arc<[u8]>,
        val: Arc<[u8]>,
    },
    Set(SetArgs),
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
    let tokens = Tokens(&valid_args);
    alt([parse_get, parse_set]).parse(tokens).ok().map(|x| x.1)
}

fn parse_get(t: Tokens) -> IResult<Tokens, Command> {
    map((token_eq(b"GET"), any_token), |(_, key)| Command::Get {
        key,
    })
    .parse(t)
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
fn parse_set(t: Tokens) -> IResult<Tokens, Command> {
    let (mut left, mut args) = map((token_eq(b"SET"), any_token, any_token), |(_, key, val)| {
        SetArgs::new(key, val)
    })
    .parse(t)?;
    if args.cond.is_none() {
        if let Ok((rest, cond)) = parse_set_cond(left.clone()) {
            args.cond = Some(cond);
            left = rest;
        }
    }
    if !args.get_old {
        if let Ok((rest, _)) = parse_get_old(left.clone()) {
            args.get_old = true;
            left = rest;
        }
    }
    if args.expire.is_none() {
        if let Ok((rest, expire)) = parse_expire_arg(left.clone()) {
            args.expire = Some(expire);
            left = rest;
        }
    }

    Ok((left, Command::Set(args)))
}

fn parse_set_cond<'a>(input: Tokens<'a>) -> IResult<Tokens<'a>, SetCond> {
    alt((
        map(token_eq(b"NX"), |_| SetCond::NX),
        map(token_eq(b"XX"), |_| SetCond::XX),
    ))
    .parse(input)
}

fn parse_get_old<'a>(input: Tokens<'a>) -> IResult<Tokens<'a>, ()> {
    map(token_eq(b"GET"), |_| ()).parse(input)
}

fn parse_expire_arg<'a>(input: Tokens<'a>) -> IResult<Tokens<'a>, ExpireArg> {
    alt((
        map_res((token_eq(b"EX"), any_token), |(_, s)| {
            u64::from_str_radix(String::from_utf8_lossy(&s).as_ref(), 10).map(|s| ExpireArg::EX(s))
        }),
        map_res((token_eq(b"PX"), any_token), |(_, s)| {
            u64::from_str_radix(String::from_utf8_lossy(&s).as_ref(), 10).map(|s| ExpireArg::PX(s))
        }),
        map_res((token_eq(b"EXAT"), any_token), |(_, s)| {
            u64::from_str_radix(String::from_utf8_lossy(&s).as_ref(), 10)
                .map(|s| ExpireArg::EXAT(s))
        }),
        map_res((token_eq(b"PXAT"), any_token), |(_, s)| {
            u64::from_str_radix(String::from_utf8_lossy(&s).as_ref(), 10)
                .map(|s| ExpireArg::PXAT(s))
        }),
        map(token_eq(b"KEEPTTL"), |_| ExpireArg::KEEPTTL),
    ))
    .parse(input)
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
