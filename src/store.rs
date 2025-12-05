use std::sync::{Arc, Mutex};

use crossbeam::epoch;

use crate::{command::Command, hashtable::ConcurrentHashMap, proto::ProtocolData};

enum EntryData {
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

struct Entry {
    data: Mutex<EntryData>,
}

pub struct Store {
    map: ConcurrentHashMap<Arc<str>, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            map: ConcurrentHashMap::new(),
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
            _ => ProtocolData::SimpleError(Arc::from("Command not implemented")),
        }
    }

    fn get(&self, key: &Arc<str>) -> Result<Option<Arc<[u8]>>, ()> {
        let guard = &epoch::pin();
        if let Some((_, v)) = self.map.lookup(key, guard) {
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
        };
        self.map.find_or_insert(key, ent, guard).1
    }

    fn del(&self, key: &Arc<str>) -> bool {
        let guard = &epoch::pin();
        self.map.remove(&key, guard).is_some()
    }

    fn keys(&self) -> Vec<Arc<[u8]>> {
        let mut keys = Vec::new();
        self.map.fold(&mut keys, |acc, (k, _)| {
            acc.push(Arc::from(k.as_bytes()));
            true
        });
        keys
    }
}
