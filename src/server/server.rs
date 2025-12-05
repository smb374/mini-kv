use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    os::raw::c_int,
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam::channel::{self, Receiver, Sender};
use mini_kv::{
    command,
    proto::{self, ProtocolData},
    store::Store,
};
use mio::{Events, Interest, Poll, Token, Waker, event::Event, net::TcpListener};
use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal};
use slab::Slab;

use crate::connection::ConnState;

const MAX_TOKENS: usize = 0x80000000; // i32 max + 1
const SERVER_TOKEN: Token = Token(MAX_TOKENS);
const WAKER_TOKEN: Token = Token(MAX_TOKENS + 1);
const EVENT_CAPACITY: usize = 4096;
const CONNECTION_TIMEOUT_MS: u64 = 5000;

static START_TIME: OnceLock<Instant> = OnceLock::new();
static WAKER: OnceLock<Waker> = OnceLock::new();

enum Work {
    HandleConnection((Event, Arc<Mutex<Option<ConnState>>>)),
    Shutdown,
}

struct Worker {
    tx: Sender<Work>,
    handle: JoinHandle<()>,
}

struct Connection {
    last_access: u64,
    expect_expire: u64,
    state: Arc<Mutex<Option<ConnState>>>,
}

pub struct Server {
    listener: TcpListener,
    poll: Poll,
    conns: Slab<Connection>,
    workers: Vec<Worker>,
    next_worker: usize,
    expiry: VecDeque<usize>,
    _store: Arc<Store>,
}

impl Server {
    pub fn new(addr: SocketAddr, nworkers: usize) -> io::Result<Self> {
        let handler = SigHandler::Handler(shutdown);
        let action = SigAction::new(handler, SaFlags::empty(), SigSet::empty());
        unsafe {
            nix::sys::signal::sigaction(Signal::SIGINT, &action)
                .expect("Failed to set SIGINT handler.");
            nix::sys::signal::sigaction(Signal::SIGTERM, &action)
                .expect("Failed to set SIGINT handler.");
        }
        let mut listener = TcpListener::bind(addr)?;
        let poll = Poll::new()?;

        poll.registry()
            .register(&mut listener, SERVER_TOKEN, Interest::READABLE)?;
        let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;

        WAKER.get_or_init(|| waker);

        let mut workers = Vec::with_capacity(nworkers);
        let store = Arc::new(Store::new());

        for _ in 0..nworkers {
            let (tx, rx) = channel::unbounded();
            let cstore = Arc::clone(&store);
            let handle = thread::spawn(move || worker_f(cstore, rx));
            workers.push(Worker { tx, handle });
        }

        Ok(Self {
            listener,
            poll,
            conns: Slab::with_capacity(4096),
            workers,
            next_worker: 0,
            expiry: VecDeque::with_capacity(4096),
            _store: store,
        })
    }

    pub fn main_loop(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(EVENT_CAPACITY);
        let mut timeout_ms = 100;

        'outer: loop {
            match self
                .poll
                .poll(&mut events, Some(Duration::from_millis(timeout_ms)))
            {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => break Err(e),
            }

            for ev in events.iter() {
                match ev.token() {
                    SERVER_TOKEN => match self.listener.accept() {
                        Ok((mut conn, addr)) => {
                            eprintln!("Got connection from {:?}", addr);
                            let ent = self.conns.vacant_entry();
                            let key = ent.key();
                            if key >= MAX_TOKENS {
                                continue;
                            }

                            self.poll.registry().register(
                                &mut conn,
                                Token(key),
                                Interest::READABLE | Interest::WRITABLE,
                            )?;

                            let state = Arc::new(Mutex::new(Some(ConnState::new(conn))));
                            let ts = get_time();
                            let conn = Connection {
                                last_access: ts,
                                expect_expire: ts + CONNECTION_TIMEOUT_MS,
                                state,
                            };

                            ent.insert(conn);
                            self.expiry.push_back(key);
                        }
                        Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                        Err(err) => return Err(err),
                    },
                    WAKER_TOKEN => break 'outer Ok(()),
                    Token(t) if self.conns.contains(t) => {
                        let conn = self.conns.get_mut(t).unwrap();
                        conn.last_access = get_time();
                        let state = Arc::clone(&conn.state);
                        let worker_id = self.next_worker;
                        self.next_worker = (self.next_worker + 1) % self.workers.len();
                        self.workers[worker_id]
                            .tx
                            .send(Work::HandleConnection((ev.clone(), state)))
                            .map_err(io::Error::other)?;
                    }
                    Token(t) => {
                        eprintln!("WARN: unexpected token={}, ignore for now", t);
                    }
                }
            }
            timeout_ms = self.process_expiry();
        }
    }

    pub fn cleanup(&mut self) -> io::Result<()> {
        for w in self.workers.drain(..) {
            w.tx.send(Work::Shutdown).map_err(io::Error::other)?;
            let _ = w.handle.join();
        }

        let d = self.conns.drain();
        drop(d);

        Ok(())
    }

    fn process_expiry(&mut self) -> u64 {
        let ts = get_time();
        let mut next_check: Option<u64> = None;
        let mut remove_list = Vec::new();
        while let Some(id) = self.expiry.pop_front() {
            let Some(conn) = self.conns.get_mut(id) else {
                continue;
            };

            if ts >= conn.expect_expire {
                let inactive_time = ts - conn.last_access;
                if inactive_time >= CONNECTION_TIMEOUT_MS {
                    // Evict
                    let mut guard = conn.state.lock().unwrap_or_else(|_| {
                        conn.state.clear_poison();
                        conn.state.lock().expect("Mutex poisoned again")
                    });
                    if let Some(s) = guard.take() {
                        drop(s);
                    }
                    remove_list.push(id);
                } else {
                    conn.expect_expire = conn.last_access + CONNECTION_TIMEOUT_MS;
                    self.expiry.push_back(id);
                }
            } else {
                let nts = conn.expect_expire - ts;
                next_check = next_check.map(|t| t.min(nts)).or(Some(nts));
                self.expiry.push_front(id);
                break;
            }
        }

        remove_list.into_iter().for_each(|id| {
            eprintln!("Dropping connection with id={}", id);
            let _ = self.conns.remove(id);
        });

        next_check.unwrap_or(100)
    }
}

fn worker_f(store: Arc<Store>, rx: Receiver<Work>) {
    loop {
        match rx.recv() {
            Ok(Work::HandleConnection((ev, state))) => {
                let mut guard = state.lock().unwrap_or_else(|_| {
                    state.clear_poison();
                    state.lock().expect("Mutex poisoned again...")
                });
                let Some(state) = guard.as_mut() else {
                    continue;
                };

                if ev.is_readable() {
                    let Ok((_, is_eof)) = state.read_sock() else {
                        continue;
                    };
                    if is_eof {
                        let s = guard.take();
                        eprintln!("Dropping connection with id={}", ev.token().0);
                        drop(s);
                        continue;
                    }
                }
                process_reqs(&store, state);
                if ev.is_writable() {
                    if state.write_sock().is_err() {
                        let s = guard.take();
                        eprintln!("Dropping connection with id={}", ev.token().0);
                        drop(s);
                        continue;
                    }
                }
            }
            Ok(Work::Shutdown) => break,
            Err(_) => break,
        }
    }
}

fn process_reqs(store: &Store, state: &mut ConnState) {
    if !state.read_buf.is_empty() {
        let mut datav: Vec<ProtocolData> = Vec::new();
        loop {
            let sz = state.read_buf.len();
            match proto::parse_protocol(&state.read_buf) {
                Ok((left, dat)) => {
                    let processed = sz - left.len();
                    let _ = state.read_buf.split_to(processed);
                    datav.push(dat);
                }
                Err(e) => match e {
                    nom::Err::Incomplete(_) => break,
                    _ => {
                        datav.push(ProtocolData::SimpleError(Arc::from(
                            format!("Failed to parse protocol: {}", e).as_str(),
                        )));
                        break;
                    }
                },
            }
        }
        datav
            .into_iter()
            .map(|dat| {
                if let Some(cmd) = command::parse_command(&dat) {
                    store.handle(&cmd)
                } else {
                    if let ProtocolData::SimpleError(_) = &dat {
                        dat
                    } else {
                        ProtocolData::SimpleError(Arc::from("Invalid protocol data for command"))
                    }
                }
            })
            .for_each(|r| r.encode(&mut state.write_buf));
    }
}

fn get_time() -> u64 {
    let start = START_TIME.get_or_init(|| Instant::now());
    start.elapsed().as_millis() as u64
}

extern "C" fn shutdown(_: c_int) {
    eprintln!("Shutting down...");
    let waker = WAKER.wait();
    waker.wake().expect("Waker should success...");
}
