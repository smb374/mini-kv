use std::sync::{Arc, Mutex};

use crossbeam::{channel::Sender, epoch};

use crate::{
    command::Command, get_time, hashtable::ConcurrentHashMap, proto::ProtocolData, zset::ZSet,
};

pub enum EntryData {
    Data(Arc<[u8]>),
    ZSet(ZSet),
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
    pub data: EntryData,
    pub expire: u64,
}

pub struct Store {
    map: ConcurrentHashMap<Arc<str>, Mutex<Entry>>,
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
                if self.set(Arc::clone(key), Arc::clone(val)) {
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
            Command::Zadd { key, score, name } => {
                match self.zadd(Arc::clone(key), *score, Arc::clone(name)) {
                    Ok(()) => ProtocolData::SimpleString(Arc::from("OK")),
                    Err(()) => ProtocolData::SimpleError(Arc::from("Not a ZSet entry")),
                }
            }
            Command::Zscore { key, name } => match self.zscore(key, name) {
                Ok(Some(f)) => ProtocolData::BulkString(Some(Arc::from(f.to_string().as_bytes()))),
                Ok(None) => ProtocolData::BulkString(None),
                Err(()) => ProtocolData::SimpleError(Arc::from("Not a ZSet entry")),
            },
            Command::Zrem { key, name } => match self.zrem(key, Arc::clone(name)) {
                Ok(deleted) => {
                    if deleted {
                        ProtocolData::Integer(1)
                    } else {
                        ProtocolData::Integer(0)
                    }
                }
                Err(()) => ProtocolData::SimpleError(Arc::from("Not a ZSet entry")),
            },
            Command::Zquery {
                key,
                score,
                name,
                offset,
                limit,
            } => match self.zquery(key, *score, Arc::clone(name), *offset, *limit) {
                Ok(Some(v)) => {
                    let size = v.len() * 2;
                    let dat =
                        v.into_iter()
                            .fold(Vec::with_capacity(size), |mut acc, (score, name)| {
                                acc.push(ProtocolData::BulkString(Some(Arc::from(
                                    name.as_bytes(),
                                ))));
                                acc.push(ProtocolData::BulkString(Some(Arc::from(
                                    score.to_string().as_bytes(),
                                ))));
                                acc
                            });
                    ProtocolData::Array(Some(Arc::from(dat.into_boxed_slice())))
                }
                Ok(None) => ProtocolData::Array(None),
                Err(()) => ProtocolData::SimpleError(Arc::from("Not a ZSet entry")),
            },
        }
    }

    pub fn get_map(&self) -> &ConcurrentHashMap<Arc<str>, Mutex<Entry>> {
        &self.map
    }

    fn get(&self, key: &Arc<str>) -> Result<Option<Arc<[u8]>>, ()> {
        let guard = &epoch::pin();
        if let Some((_, ent)) = self.map.lookup(key, guard) {
            let eguard = ent.lock().unwrap_or_else(|_| {
                ent.clear_poison();
                ent.lock().expect("Mutex poisoned again")
            });
            if eguard.expire != 0 && get_time() >= eguard.expire {
                self.map.remove(key, guard);
                return Ok(None);
            }
            match eguard.data {
                EntryData::Data(ref s) => Ok(Some(Arc::clone(s))),
                EntryData::ZSet(_) => Err(()),
            }
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: Arc<str>, val: Arc<[u8]>) -> bool {
        let guard = &epoch::pin();
        let ent = Mutex::new(Entry {
            data: EntryData::Data(Arc::clone(&val)),
            expire: 0,
        });
        let ((_, eref), inserted) = self.map.find_or_insert(key, ent, guard);
        if !inserted {
            let mut eguard = eref.lock().unwrap_or_else(|_| {
                eref.clear_poison();
                eref.lock().expect("Mutex poisoned again")
            });
            if let EntryData::ZSet(_) = eguard.data {
                return false;
            }

            eguard.data = EntryData::Data(val);
            eguard.expire = 0;
        }
        true
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
            let eguard = ent.lock().unwrap_or_else(|_| {
                ent.clear_poison();
                ent.lock().expect("Mutex poisoned again")
            });
            if eguard.expire != 0 && ts >= eguard.expire {
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
            let mut eguard = ent.lock().unwrap_or_else(|_| {
                ent.clear_poison();
                ent.lock().expect("Mutex poisoned again")
            });
            let old_ts = eguard.expire;
            eguard.expire = ts;
            if old_ts == 0 {
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
            let eguard = ent.lock().unwrap_or_else(|_| {
                ent.clear_poison();
                ent.lock().expect("Mutex poisoned again")
            });
            let ts = eguard.expire;
            if ts == 0 { None } else { Some(ts - get_time()) }
        })
    }

    fn zadd(&self, key: Arc<str>, score: f64, name: Arc<str>) -> Result<(), ()> {
        let ent = Mutex::new(Entry {
            data: EntryData::ZSet(ZSet::new()),
            expire: 0,
        });
        let guard = &epoch::pin();
        let (_, eref) = self.map.find_or_insert(key, ent, guard).0;
        let mut eguard = eref.lock().unwrap_or_else(|_| {
            eref.clear_poison();
            eref.lock().expect("Mutex poisoned again")
        });
        if let EntryData::ZSet(zs) = eguard.data.as_mut() {
            zs.add(score, name);
            Ok(())
        } else {
            Err(())
        }
    }

    fn zscore(&self, key: &Arc<str>, name: &Arc<str>) -> Result<Option<f64>, ()> {
        let guard = &epoch::pin();
        let Some((_, eref)) = self.map.lookup(key, guard) else {
            return Ok(None);
        };
        let eguard = eref.lock().unwrap_or_else(|_| {
            eref.clear_poison();
            eref.lock().expect("Mutex poisoned again")
        });
        if let EntryData::ZSet(zs) = eguard.data.as_ref() {
            Ok(zs.score(name))
        } else {
            Err(())
        }
    }

    fn zrem(&self, key: &Arc<str>, name: Arc<str>) -> Result<bool, ()> {
        let guard = &epoch::pin();
        let Some((_, eref)) = self.map.lookup(key, guard) else {
            return Ok(false);
        };
        let mut eguard = eref.lock().unwrap_or_else(|_| {
            eref.clear_poison();
            eref.lock().expect("Mutex poisoned again")
        });
        if let EntryData::ZSet(zs) = eguard.data.as_mut() {
            Ok(zs.del(name))
        } else {
            Err(())
        }
    }

    fn zquery(
        &self,
        key: &Arc<str>,
        score: f64,
        name: Arc<str>,
        offset: i64,
        limit: usize,
    ) -> Result<Option<Vec<(f64, Arc<str>)>>, ()> {
        let guard = &epoch::pin();
        let Some((_, eref)) = self.map.lookup(key, guard) else {
            return Ok(None);
        };
        let eguard = eref.lock().unwrap_or_else(|_| {
            eref.clear_poison();
            eref.lock().expect("Mutex poisoned again")
        });
        if let EntryData::ZSet(zs) = eguard.data.as_ref() {
            Ok(Some(zs.query(score, name, offset, limit)))
        } else {
            Err(())
        }
    }
}
