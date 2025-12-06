use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use crossbeam::{channel::Sender, epoch};

use crate::{command::Command, get_time, hashtable::ConcurrentHashMap, proto::ProtocolData};

pub enum EntryData {
    Data(Arc<[u8]>),
    ZSet(()), // TODO: implement ZSet
}

impl AsRef<EntryData> for EntryData {
    fn as_ref(&self) -> &EntryData {
        self
    }
}

impl AsMut<EntryData> for EntryData {
    fn as_mut(&mut self) -> &mut EntryData {
        self
    }
}

pub struct Entry {
    pub data: Mutex<EntryData>,
    pub expire: AtomicU64,
}

pub struct Store {
    map: ConcurrentHashMap<Arc<str>, Entry>,
    exp_tx: Sender<(Arc<str>, u64)>,
}

impl Store {
    pub fn new(exp_tx: Sender<(Arc<str>, u64)>) -> Self {
        Self {
            map: ConcurrentHashMap::new(),
            exp_tx,
        }
    }

    pub fn handle(&self, cmd: &Command) -> ProtocolData {
        match cmd {
            Command::Get { key } => match self.get(key) {
                Ok(None) => ProtocolData::BulkString(None),
                Ok(Some(s)) => ProtocolData::BulkString(Some(s)),
                Err(()) => ProtocolData::SimpleError(Arc::from("GET on a ZSET entry")),
            },
            Command::Set { key, val } => {
                if self.set(key.clone(), val.clone()) {
                    ProtocolData::SimpleString(Arc::from("OK"))
                } else {
                    ProtocolData::BulkString(None)
                }
            }
            Command::Del { key } => {
                if self.del(key) {
                    ProtocolData::Integer(1)
                } else {
                    ProtocolData::Integer(0)
                }
            }
            Command::Hello => ProtocolData::SimpleString(Arc::from("OK")),
            Command::Keys => {
                let keys = self.keys();
                let v: Box<[ProtocolData]> = keys
                    .into_iter()
                    .map(|k| ProtocolData::BulkString(Some(k)))
                    .collect();
                ProtocolData::Array(Some(Arc::from(v)))
            }
            Command::Pexpire { key, expire_ms } => {
                if self.pexpire(key, *expire_ms) {
                    ProtocolData::SimpleString(Arc::from("OK"))
                } else {
                    ProtocolData::BulkString(None)
                }
            }
            Command::Pttl { key } => match self.pttl(key) {
                Some(ts) => ProtocolData::Integer(ts as i64),
                None => ProtocolData::BulkString(None),
            },
            _ => ProtocolData::SimpleError(Arc::from("Command not implemented")),
        }
    }

    pub fn get_map(&self) -> &ConcurrentHashMap<Arc<str>, Entry> {
        &self.map
    }

    fn get(&self, key: &Arc<str>) -> Result<Option<Arc<[u8]>>, ()> {
        let guard = &epoch::pin();
        if let Some((_, v)) = self.map.lookup(key, guard) {
            let expire = v.expire.load(Ordering::Acquire);
            if expire != 0 && get_time() >= expire {
                self.map.remove(key, guard);
                return Ok(None);
            }
            let mut vguard = v.data.lock().unwrap_or_else(|_| {
                v.data.clear_poison();
                v.data.lock().expect("Mutex poisoned again")
            });
            match vguard.as_mut() {
                EntryData::Data(s) => Ok(Some(Arc::clone(s))),
                EntryData::ZSet(_) => Err(()),
            }
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: Arc<str>, val: Arc<[u8]>) -> bool {
        let guard = &epoch::pin();
        let ent = Entry {
            data: Mutex::new(EntryData::Data(val)),
            expire: AtomicU64::new(0),
        };
        self.map.find_or_insert(key, ent, guard).1
    }

    fn del(&self, key: &Arc<str>) -> bool {
        let guard = &epoch::pin();
        self.map.remove(&key, guard).is_some()
    }

    fn keys(&self) -> Vec<Arc<[u8]>> {
        let guard = &epoch::pin();
        let mut keys = Vec::new();
        let ts = get_time();
        self.map.fold(&mut keys, guard, |acc, (k, ent)| {
            let expire = ent.expire.load(Ordering::Acquire);
            if expire != 0 && ts >= expire {
                self.map.remove(k, guard);
            } else {
                acc.push(Arc::from(k.as_bytes()));
            }
            true
        });
        keys
    }

    fn pexpire(&self, key: &Arc<str>, expire_ms: u64) -> bool {
        let guard = &epoch::pin();
        if let Some((_, ent)) = self.map.lookup(key, guard) {
            let ts = get_time() + expire_ms;
            if ent.expire.swap(ts, Ordering::AcqRel) == 0 {
                // Send on fresh set to insert into expiry on main thread
                self.exp_tx
                    .clone() // Clone on send to not use the same tx for every worker, maybe use TLS instead?
                    .send((Arc::clone(&key), ts))
                    .expect("Channel should be connected before worker shutdown...");
            }
            true
        } else {
            false
        }
    }

    fn pttl(&self, key: &Arc<str>) -> Option<u64> {
        self.map.lookup(key, &epoch::pin()).and_then(|(_, ent)| {
            let ts = ent.expire.load(Ordering::Acquire);
            if ts == 0 { None } else { Some(ts - get_time()) }
        })
    }
}
