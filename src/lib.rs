pub mod command;
pub mod hashtable;
pub mod persistence;
pub mod proto;
pub mod store;
pub mod zset;

use std::{
    alloc::{self, Layout},
    sync::OnceLock,
    time::Instant,
};

static START_TIME: OnceLock<Instant> = OnceLock::new();

pub fn get_time() -> u64 {
    let start = START_TIME.get_or_init(|| Instant::now());
    start.elapsed().as_millis() as u64
}

pub fn encode_float(x: f64) -> [u8; 8] {
    let bits = x.to_bits();

    let y = if bits & 0x8000000000000000 != 0 {
        !bits
    } else {
        bits ^ 0x8000000000000000
    };
    y.to_be_bytes()
}

pub fn decode_float(x: [u8; 8]) -> f64 {
    let bits = u64::from_be_bytes(x);

    let y = if bits & 0x8000000000000000 != 0 {
        bits ^ 0x8000000000000000
    } else {
        !bits
    };
    f64::from_bits(y)
}

pub fn alloc_box_buffer(len: usize) -> Box<[u8]> {
    if len == 0 {
        return <Box<[u8]>>::default();
    }
    let layout = Layout::array::<u8>(len).unwrap();
    let ptr = unsafe { alloc::alloc_zeroed(layout) };
    let slice_ptr = core::ptr::slice_from_raw_parts_mut(ptr, len);
    unsafe { Box::from_raw(slice_ptr) }
}
