pub mod command;
pub mod hashtable;
pub mod proto;
pub mod store;

use std::{sync::OnceLock, time::Instant};

static START_TIME: OnceLock<Instant> = OnceLock::new();

pub fn get_time() -> u64 {
    let start = START_TIME.get_or_init(|| Instant::now());
    start.elapsed().as_millis() as u64
}
