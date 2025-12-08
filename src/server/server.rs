use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    os::raw::c_int,
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::{
    channel::{self, Receiver, Sender},
    epoch,
};
use mini_kv::{
    command, get_time,
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
const EVENT_CAPACITY: usize = 65536;
const CONNECTION_TIMEOUT_MS: u64 = 30000;

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
    conn_expiry: VecDeque<usize>,
    ent_expiry: VecDeque<(Arc<str>, u64)>,
    exp_rx: Receiver<(Arc<str>, u64)>,
    store: Arc<Store>,
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
        let (exp_tx, exp_rx) = channel::bounded(65536);
        let store = Arc::new(Store::new(exp_tx));

        for _ in 0..nworkers {
            let (tx, rx) = channel::bounded(65536);
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
            conn_expiry: VecDeque::with_capacity(EVENT_CAPACITY),
            ent_expiry: VecDeque::with_capacity(4096),
            exp_rx,
            store,
        })
    }

    pub fn main_loop(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(EVENT_CAPACITY);
        let mut timeout_ms = 100;
        let mut accept_ready = false;

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
                    SERVER_TOKEN => {
                        accept_ready = true;
                    }
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
                        log::warn!("Unexpected token={}, ignore for now", t);
                    }
                }
            }
            if accept_ready {
                for _ in 0..4096 {
                    match self.listener.accept() {
                        Ok((mut conn, addr)) => {
                            log::info!("Got connection from {addr}");
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
                            self.conn_expiry.push_back(key);
                        }
                        Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                            accept_ready = false;
                            break;
                        }
                        Err(err) => return Err(err),
                    }
                }
            }

            if self.ent_expiry.is_empty() {
                timeout_ms = self.process_conn_expiry();
            } else {
                let nts1 = self.process_conn_expiry();
                let nts2 = self.process_ent_expiry();
                timeout_ms = nts1.min(nts2);
            }
            // At most 4096 receives to mitigate churns
            if !self.exp_rx.is_empty() {
                for _ in 0..4096 {
                    match self.exp_rx.try_recv() {
                        Ok(item) => self.ent_expiry.push_back(item),
                        Err(e) => match e {
                            channel::TryRecvError::Empty => break,
                            channel::TryRecvError::Disconnected => {
                                log::debug!(
                                    "Channel disconnected while still running, exit event loop..."
                                );
                                break 'outer Err(io::Error::other(e));
                            }
                        },
                    }
                }
            }
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

    fn process_conn_expiry(&mut self) -> u64 {
        let ts = get_time();
        let mut next_check: Option<u64> = None;
        let mut remove_list = Vec::new();
        while let Some(id) = self.conn_expiry.pop_front() {
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
                        log::info!("Dropping connection with id={id}");
                        drop(s);
                    }
                    remove_list.push(id);
                } else {
                    conn.expect_expire = conn.last_access + CONNECTION_TIMEOUT_MS;
                    self.conn_expiry.push_back(id);
                }
            } else {
                let nts = conn.expect_expire - ts;
                next_check = next_check.map(|t| t.min(nts)).or(Some(nts));
                self.conn_expiry.push_front(id);
                break;
            }
        }

        remove_list.into_iter().for_each(|id| {
            let _ = self.conns.remove(id);
        });

        next_check.unwrap_or(100)
    }

    fn process_ent_expiry(&mut self) -> u64 {
        let map = self.store.get_map();
        let ts = get_time();
        let guard = &epoch::pin();
        let mut next_check: Option<u64> = None;
        let mut remove_list = Vec::new();

        while let Some((key, expect_expire)) = self.ent_expiry.pop_front() {
            let Some((_, ent)) = map.lookup(&key, guard) else {
                continue;
            };

            if ts >= expect_expire {
                // exp_time may be updated by PEXPIRE before processing
                // double check to make sure
                let eguard = ent.lock().unwrap_or_else(|_| {
                    ent.clear_poison();
                    ent.lock().expect("Mutex poisoned again")
                });
                let exp_time = eguard.expire;
                if exp_time == 0 {
                    continue;
                }
                if ts >= exp_time {
                    remove_list.push(key);
                } else {
                    self.ent_expiry.push_back((key, exp_time));
                }
            } else {
                let nts = expect_expire - ts;
                next_check = next_check.map(|t| t.min(nts)).or(Some(nts));
                self.ent_expiry.push_front((key, expect_expire));
                break;
            }
        }

        remove_list.into_iter().for_each(|key| {
            let _ = map.remove(&key, guard);
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
                    let Ok((nread, is_eof)) = state.read_sock() else {
                        continue;
                    };
                    if nread == 0 && is_eof {
                        let s = guard.take();
                        drop(s);
                        continue;
                    }
                    process_reqs(&store, state);
                }
                // Try to write anyways
                if !state.write_buf.is_empty() {
                    let Ok((nwrite, is_eof)) = state.write_sock() else {
                        continue;
                    };
                    if nwrite == 0 && is_eof {
                        let s = guard.take();
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
                        ProtocolData::SimpleError(Arc::from("Invalid command"))
                    }
                }
            })
            .for_each(|r| r.encode(&mut state.write_buf));
    }
}

extern "C" fn shutdown(_: c_int) {
    log::info!("Shutting down...");
    let waker = WAKER.wait();
    waker.wake().expect("Waker should success...");
}
