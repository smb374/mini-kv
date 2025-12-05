use std::{error, fmt};

use crate::sync::Arc;

use bytes::{BufMut, BytesMut};
use nom::{
    IResult, Parser,
    bytes::streaming::{tag, take_until},
    character::streaming::anychar,
    combinator::{map, map_res, peek},
    multi::many_m_n,
};

#[derive(Debug)]
pub enum ParseProtocolError {
    InvalidProtocolPrefix(char),
    InvalidLength,
}

impl fmt::Display for ParseProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseProtocolError::InvalidLength => write!(f, "Invalid length for BulkString/Array"),
            ParseProtocolError::InvalidProtocolPrefix(c) => {
                write!(f, "Invalid protocol tag: {}", c)
            }
        }
    }
}

impl error::Error for ParseProtocolError {}

#[derive(Debug)]
pub enum ProtocolData {
    SimpleString(Arc<str>),
    SimpleError(Arc<str>),
    Integer(i64),
    BulkString(Option<Arc<[u8]>>),      // None -> NULL
    Array(Option<Arc<[ProtocolData]>>), // None -> NULL
}

impl ProtocolData {
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            ProtocolData::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put(s.as_bytes());
                buf.put("\r\n".as_bytes());
            }
            ProtocolData::SimpleError(s) => {
                buf.put_u8(b'-');
                buf.put(s.as_bytes());
                buf.put("\r\n".as_bytes());
            }
            ProtocolData::Integer(i) => {
                buf.put_u8(b':');
                buf.put(i.to_string().as_bytes());
                buf.put("\r\n".as_bytes());
            }
            ProtocolData::BulkString(bs) => {
                buf.put_u8(b'$');
                if let Some(s) = bs {
                    let len = s.len();
                    buf.put(len.to_string().as_bytes());
                    buf.put("\r\n".as_bytes());
                    buf.put(s.as_ref());
                } else {
                    buf.put("-1".as_bytes());
                }
                buf.put("\r\n".as_bytes());
            }
            ProtocolData::Array(arr) => {
                buf.put_u8(b'*');
                if let Some(protos) = arr {
                    let len = protos.len();
                    buf.put(len.to_string().as_bytes());
                    buf.put("\r\n".as_bytes());
                    for p in protos.iter() {
                        p.encode(buf);
                    }
                } else {
                    buf.put("-1".as_bytes());
                    buf.put("\r\n".as_bytes());
                }
            }
        }
    }
}

pub fn parse_protocol(s: &[u8]) -> IResult<&[u8], ProtocolData> {
    let (_, tag) = map_res(peek(anychar), |x| match x {
        '+' | '-' | ':' | '$' | '*' => Ok(x),
        _ => Err(ParseProtocolError::InvalidProtocolPrefix(x)),
    })
    .parse(s)?;

    match tag {
        '+' => parse_simple_string(s),
        '-' => parse_simple_error(s),
        ':' => parse_integer(s),
        '$' => parse_bulk_string(s),
        '*' => parse_array(s),
        _ => unreachable!(),
    }
}

fn parse_line(s: &[u8]) -> IResult<&[u8], &[u8]> {
    map((take_until("\r\n"), tag("\r\n")), |(x, _)| x).parse(s)
}

fn parse_simple_string(s: &[u8]) -> IResult<&[u8], ProtocolData> {
    map((tag("+"), parse_line), |(_, x)| {
        ProtocolData::SimpleString(Arc::from(String::from_utf8_lossy(x)))
    })
    .parse(s)
}

fn parse_simple_error(s: &[u8]) -> IResult<&[u8], ProtocolData> {
    map((tag("-"), parse_line), |(_, x)| {
        ProtocolData::SimpleError(Arc::from(String::from_utf8_lossy(x)))
    })
    .parse(s)
}

fn parse_integer(s: &[u8]) -> IResult<&[u8], ProtocolData> {
    map_res((tag(":"), parse_line), |(_, x)| {
        let x_str = String::from_utf8_lossy(x);
        i64::from_str_radix(&x_str, 10).map(|v| ProtocolData::Integer(v))
    })
    .parse(s)
}

fn parse_bulk_string(s: &[u8]) -> IResult<&[u8], ProtocolData> {
    let (left, len) = map_res((tag("$"), parse_line), |(_, x)| {
        let x_str = String::from_utf8_lossy(x);
        i64::from_str_radix(&x_str, 10)
    })
    .parse(s)?;
    if len == -1 {
        return Ok((left, ProtocolData::BulkString(None)));
    }
    map_res(parse_line, |x| {
        if x.len() != len as usize {
            Err(ParseProtocolError::InvalidLength)
        } else {
            Ok(ProtocolData::BulkString(Some(Arc::from(x))))
        }
    })
    .parse(left)
}

fn parse_array(s: &[u8]) -> IResult<&[u8], ProtocolData> {
    let (left, len) = map_res((tag("*"), parse_line), |(_, x)| {
        let x_str = String::from_utf8_lossy(x);
        i64::from_str_radix(&x_str, 10)
    })
    .parse(s)?;
    if len == -1 {
        return Ok((left, ProtocolData::Array(None)));
    }
    let size = len as usize;
    map_res(many_m_n(size, size, parse_protocol), |v| {
        if v.len() != size {
            Err(ParseProtocolError::InvalidLength)
        } else {
            Ok(ProtocolData::Array(Some(Arc::from(v.into_boxed_slice()))))
        }
    })
    .parse(left)
}
