use std::{ffi::OsString, net::SocketAddr};

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

struct Args {
    host: String,
    port: u16,
    workers: usize,
}

pub fn parse_args(
    raw: impl IntoIterator<Item = impl Into<OsString>>,
) -> Result<Option<(SocketAddr, usize)>, BoxedError> {
    let mut args = Args {
        host: String::from("0.0.0.0"),
        port: 6379,
        workers: 4,
    };

    let raw = clap_lex::RawArgs::new(raw);
    let mut cursor = raw.cursor();
    raw.next(&mut cursor);
    while let Some(arg) = raw.next(&mut cursor) {
        if let Some((long, val)) = arg.to_long() {
            match long {
                Ok("host") => {
                    if let Some(host) = val.and_then(|x| x.to_str()) {
                        args.host = host.to_string();
                    }
                }
                Ok("port") => {
                    if let Some(port) = val
                        .and_then(|x| x.to_str())
                        .and_then(|x| u16::from_str_radix(x, 10).ok())
                    {
                        args.port = port;
                    }
                }
                Ok("workers") => {
                    if let Some(workers) = val
                        .and_then(|x| x.to_str())
                        .and_then(|x| usize::from_str_radix(x, 10).ok())
                    {
                        args.workers = workers;
                    }
                }
                Ok("help") => return Ok(None),
                _ => return Err(format!("Unexpected Flag: --{}", arg.display()).into()),
            }
        } else if let Some(mut shorts) = arg.to_short() {
            while let Some(short) = shorts.next_flag() {
                match short {
                    Ok('H') => {
                        let val = shorts.next_value_os();
                        if let Some(host) = val.and_then(|x| x.to_str()) {
                            args.host = host.to_string();
                        } else if let Some(arg) = raw.peek(&cursor) {
                            if !arg.is_short() || !arg.is_long() {
                                if let Some(host) = arg.to_value_os().to_str() {
                                    args.host = host.to_string();
                                }
                            }
                        }
                    }
                    Ok('p') => {
                        let val = shorts.next_value_os();
                        if let Some(port) = val
                            .and_then(|x| x.to_str())
                            .and_then(|x| u16::from_str_radix(x, 10).ok())
                        {
                            args.port = port;
                        } else if let Some(arg) = raw.peek(&cursor) {
                            if !arg.is_short() || !arg.is_long() {
                                if let Some(port) = arg
                                    .to_value_os()
                                    .to_str()
                                    .and_then(|x| u16::from_str_radix(x, 10).ok())
                                {
                                    args.port = port;
                                }
                            }
                        }
                    }
                    Ok('w') => {
                        let val = shorts.next_value_os();
                        if let Some(workers) = val
                            .and_then(|x| x.to_str())
                            .and_then(|x| usize::from_str_radix(x, 10).ok())
                        {
                            args.workers = workers;
                        } else if let Some(arg) = raw.peek(&cursor) {
                            if !arg.is_short() || !arg.is_long() {
                                if let Some(workers) = arg
                                    .to_value_os()
                                    .to_str()
                                    .and_then(|x| usize::from_str_radix(x, 10).ok())
                                {
                                    args.workers = workers;
                                }
                            }
                        }
                    }
                    Ok('h') => return Ok(None),
                    Ok(c) => {
                        return Err(format!("Unexpected flag: -{c}").into());
                    }
                    Err(e) => {
                        return Err(format!("Unexpected Flag: -{}", e.to_string_lossy()).into());
                    }
                }
            }
        }
    }

    let addr = format!("{}:{}", args.host, args.port).parse()?;
    Ok(Some((addr, args.workers)))
}
