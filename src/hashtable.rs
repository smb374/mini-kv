use crate::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use crossbeam::epoch::{self, Atomic};
use crossbeam::utils::Backoff;
use std::{
    hash::{Hash, Hasher},
    time::Duration,
};
use twox_hash::XxHash3_64;

const MIN_CAPACITY: u64 = 1024;
const HOP_RANGE: u64 = 64;
const SEGMENT_SIZE: u64 = 128;
struct Segment {
    lock: Mutex<()>,
    ts: AtomicU64,
}

struct Bucket<K, V> {
    hop: AtomicU64,
    in_use: AtomicBool,
    slot: Atomic<(K, V)>,
}

struct ConcurrentHashTable<K, V> {
    next: Atomic<Self>,
    segments: Box<[Segment]>,
    buckets: Box<[Bucket<K, V>]>,
    mask: u64,
    nsegs: u64,
    overflow_size: u64,
    size: AtomicU64,
}

pub struct ConcurrentHashMap<K, V> {
    active: Atomic<ConcurrentHashTable<K, V>>,
    mpos: AtomicU64,
    mthreads: AtomicU64,
    mstarted: AtomicBool,
    size: AtomicU64,
    epoch: AtomicU64,
}

impl<K, V> ConcurrentHashTable<K, V> {
    fn new(cap: u64) -> Self {
        let mut capacity = if !cap.is_power_of_two() {
            cap.next_power_of_two()
        } else {
            cap
        };
        if capacity < MIN_CAPACITY {
            capacity = MIN_CAPACITY;
        }

        let len = capacity + capacity / 8;
        let nsegs = len / SEGMENT_SIZE + (len % SEGMENT_SIZE != 0) as u64;
        Self {
            next: Atomic::null(),
            segments: Box::from_iter((0..nsegs).map(|_| Segment {
                lock: Mutex::new(()),
                ts: AtomicU64::new(0),
            })),
            buckets: Box::from_iter((0..len).map(|_| Bucket {
                hop: AtomicU64::new(0),
                in_use: AtomicBool::new(false),
                slot: Atomic::null(),
            })),
            nsegs: nsegs,
            mask: capacity - 1,
            overflow_size: capacity / 8,
            size: AtomicU64::new(0),
        }
    }
}

impl<K, V> Default for ConcurrentHashTable<K, V> {
    fn default() -> Self {
        Self::new(MIN_CAPACITY)
    }
}

#[allow(dead_code)]
impl<K, V> ConcurrentHashTable<K, V>
where
    K: PartialEq + Eq + Hash,
{
    // lookups need to be guarded with Guard, use epoch::pin() to get it.
    fn lookup<'a>(&self, key: &K, guard: &'a epoch::Guard) -> Option<&'a (K, V)> {
        let mut state = XxHash3_64::new();
        key.hash(&mut state);
        let hash = state.finish();
        let o_buc = hash & self.mask;
        let o_seg = o_buc / SEGMENT_SIZE;

        let mut ts_before = self.segments[o_seg as usize].ts.load(Ordering::Acquire);
        'outer: loop {
            let mut hop = self.buckets[o_buc as usize].hop.load(Ordering::Relaxed);
            while hop > 0 {
                let lowest_set: u64 = hop.trailing_zeros().into();
                let curr_idx = (o_buc + lowest_set) as usize;
                let slot = unsafe { self.buckets[curr_idx].slot.load_consume(guard).as_ref() };
                if slot.is_some_and(|s| key.eq(&s.0)) {
                    break 'outer slot;
                }

                hop &= !(1 << lowest_set);
            }
            let ts_after = self.segments[o_seg as usize].ts.load(Ordering::Acquire);
            if ts_before != ts_after {
                ts_before = ts_after;
                continue;
            }

            break None;
        }
    }

    fn remove<'a>(&self, key: &K, guard: &'a epoch::Guard) -> Option<&'a (K, V)> {
        let mut state = XxHash3_64::new();
        key.hash(&mut state);
        let hash = state.finish();
        let o_buc = hash & self.mask;
        let o_seg = o_buc / SEGMENT_SIZE;

        let s_ref = &self.segments[o_seg as usize];
        let _guard = s_ref.lock.lock().expect("Segment lock poisoned");

        let mut hop = self.buckets[o_buc as usize].hop.load(Ordering::Relaxed);
        while hop > 0 {
            let lowest_set: u64 = hop.trailing_zeros().into();
            let curr_idx = (o_buc + lowest_set) as usize;

            let slot = self.buckets[curr_idx as usize].slot.load_consume(guard);
            let sref = unsafe { slot.as_ref() };

            if let Some(s) = sref.filter(|&s| key.eq(&s.0)) {
                self.buckets[curr_idx]
                    .slot
                    .store(epoch::Shared::null(), Ordering::Relaxed);
                self.buckets[curr_idx]
                    .in_use
                    .store(false, Ordering::Release);

                self.buckets[o_buc as usize]
                    .hop
                    .fetch_and(!(1 << lowest_set), Ordering::Relaxed);
                self.size.fetch_sub(1, Ordering::AcqRel);

                unsafe {
                    guard.defer_destroy(slot);
                }
                return Some(s);
            }

            hop &= !(1 << lowest_set);
        }

        None
    }

    fn find_or_insert<'a>(
        &self,
        key: K,
        val: V,
        guard: &'a epoch::Guard,
    ) -> Result<&'a (K, V), ()> {
        let node = epoch::Owned::new((key, val));
        let nref = node.into_shared(guard);
        self.find_or_insert_internal(nref, guard).map(|x| x.0)
    }

    fn find_or_insert_internal<'a>(
        &self,
        node: epoch::Shared<'a, (K, V)>,
        guard: &'a epoch::Guard,
    ) -> Result<(&'a (K, V), bool), ()> {
        let key = unsafe { &node.as_ref().expect("Input node should be non-null").0 };
        let mut state = XxHash3_64::new();
        key.hash(&mut state);
        let hash = state.finish();
        let o_buc = hash & self.mask;
        let o_seg = o_buc / SEGMENT_SIZE;

        if !self.next.load(Ordering::Acquire, guard).is_null() {
            return Err(());
        }

        let s_ref = &self.segments[o_seg as usize];
        let _guard = s_ref.lock.lock().expect("Segment lock poisoned");

        let mut hop = self.buckets[o_buc as usize].hop.load(Ordering::Relaxed);
        while hop > 0 {
            let lowest_set: u64 = hop.trailing_zeros().into();
            let curr_idx = (o_buc + lowest_set) as usize;

            let slot = self.buckets[curr_idx as usize].slot.load_consume(guard);
            let sref = unsafe { slot.as_ref() };
            if let Some(s) = sref.filter(|&s| key.eq(&s.0)) {
                return Ok((s, false));
            }
            hop &= !(1 << lowest_set);
        }

        let mut offset = 0;
        let mut res_buc = o_buc;
        while offset < self.overflow_size {
            if !self.buckets[res_buc as usize]
                .in_use
                .load(Ordering::Relaxed)
                && !self.buckets[res_buc as usize]
                    .in_use
                    .swap(true, Ordering::Relaxed)
            {
                break;
            }
            offset += 1;
            res_buc += 1;
        }

        if offset < self.overflow_size {
            while offset >= HOP_RANGE {
                if !self.find_closer_bucket(o_seg, &mut res_buc, &mut offset, guard) {
                    self.buckets[res_buc as usize]
                        .slot
                        .store(epoch::Shared::null(), Ordering::Relaxed);
                    self.buckets[res_buc as usize]
                        .in_use
                        .store(false, Ordering::Release);
                    return Err(());
                }
            }

            self.buckets[res_buc as usize]
                .slot
                .store(node, Ordering::Relaxed);
            self.buckets[o_buc as usize]
                .hop
                .fetch_or(1 << offset, Ordering::Relaxed);
            self.size.fetch_add(1, Ordering::AcqRel);
            return Ok((unsafe { node.as_ref() }.unwrap(), true));
        }

        Err(())
    }

    fn find_closer_bucket(
        &self,
        free_seg: u64,
        free_buc: &mut u64,
        free_dist: &mut u64,
        guard: &epoch::Guard,
    ) -> bool {
        'outer: loop {
            for dist in (0..HOP_RANGE).rev() {
                let curr_buc = *free_buc - dist;
                let hop = self.buckets[curr_buc as usize].hop.load(Ordering::Relaxed);
                if hop > 0 {
                    let mut lock_guard = None;
                    let moved_off: u64 = hop.trailing_zeros().into();
                    let index = curr_buc + moved_off;
                    if index >= *free_buc {
                        continue;
                    }

                    let curr_seg = index / SEGMENT_SIZE;
                    let cs_ref = &self.segments[curr_seg as usize];
                    if free_seg != curr_seg {
                        lock_guard = Some(cs_ref.lock.lock().expect("Segment lock poisoned"));
                    }

                    let hop_after = self.buckets[curr_buc as usize].hop.load(Ordering::Relaxed);
                    if hop != hop_after {
                        if free_seg != curr_seg {
                            drop(lock_guard);
                            continue 'outer;
                        }
                    }
                    let slot = self.buckets[index as usize].slot.load_consume(guard);
                    self.buckets[*free_buc as usize]
                        .slot
                        .store(slot, Ordering::Relaxed);
                    self.buckets[curr_buc as usize]
                        .hop
                        .fetch_or(1 << dist, Ordering::Relaxed);
                    self.buckets[curr_buc as usize]
                        .hop
                        .fetch_and(!(1 << moved_off), Ordering::Relaxed);
                    self.segments[curr_seg as usize]
                        .ts
                        .fetch_add(1, Ordering::Relaxed);

                    *free_dist -= *free_buc - index;
                    *free_buc = index;

                    if free_seg != curr_seg {
                        drop(lock_guard);
                    }

                    break 'outer true;
                }
            }

            break false;
        }
    }
}

impl<K, V> ConcurrentHashMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: u64) -> Self {
        Self {
            active: Atomic::new(ConcurrentHashTable::new(cap)),
            mpos: AtomicU64::new(0),
            mthreads: AtomicU64::new(0),
            mstarted: AtomicBool::new(false),
            size: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
        }
    }
}

impl<K, V> Default for ConcurrentHashMap<K, V> {
    fn default() -> Self {
        Self {
            active: Atomic::new(ConcurrentHashTable::default()),
            mpos: AtomicU64::new(0),
            mthreads: AtomicU64::new(0),
            mstarted: AtomicBool::new(false),
            size: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
        }
    }
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: PartialEq + Eq + Hash,
{
    fn migrate_seg(
        tref: &ConcurrentHashTable<K, V>,
        nref: &ConcurrentHashTable<K, V>,
        seg: u64,
        guard: &epoch::Guard,
    ) {
        let _guard = tref.segments[seg as usize]
            .lock
            .lock()
            .expect("Segment poisoned");

        let start = seg * SEGMENT_SIZE;
        for i in 0..SEGMENT_SIZE {
            let node = tref.buckets[(start + i) as usize]
                .slot
                .load(Ordering::Relaxed, guard);
            if !node.is_null() {
                nref.find_or_insert_internal(node, guard)
                    .expect("Migrate insert should always success");
            }
        }
    }

    fn migrate<'a>(
        &self,
        tref: &ConcurrentHashTable<K, V>,
        nxt: epoch::Shared<'a, ConcurrentHashTable<K, V>>,
        guard: &'a epoch::Guard,
    ) {
        if let Some(nref) = unsafe { nxt.as_ref() } {
            self.mthreads.fetch_add(1, Ordering::AcqRel);
            loop {
                let seg = self.mpos.fetch_add(1, Ordering::AcqRel);
                if seg >= tref.nsegs {
                    break;
                }
                Self::migrate_seg(tref, nref, seg, guard);
            }

            if self.mthreads.fetch_sub(1, Ordering::AcqRel) == 1 {
                let old = self.active.swap(nxt, Ordering::AcqRel, guard);
                self.mstarted.store(false, Ordering::Release);
                unsafe {
                    guard.defer_destroy(old);
                }
                return;
            }
        }

        let e = self.epoch.load(Ordering::Acquire);
        let mut dur: u64 = 0;
        let b = Backoff::new();
        while self.mstarted.load(Ordering::Acquire) && e == self.epoch.load(Ordering::Acquire) {
            if b.is_completed() {
                let duration = dur.min(10u64);
                crate::thread::sleep(Duration::from_millis(1 << duration));
                dur += 1;
            } else {
                b.snooze();
            }
        }
    }

    pub fn remove<'a>(&self, key: &K, guard: &'a epoch::Guard) -> Option<&'a (K, V)> {
        let mut active = self.active.load(Ordering::Acquire, guard);
        loop {
            let tref = unsafe { active.as_ref() }.expect("Active should be always non-null");
            let nxt = tref.next.load(Ordering::Acquire, guard);
            if !nxt.is_null() {
                self.migrate(tref, nxt, guard);
                active = self.active.load(Ordering::Acquire, guard);
                continue;
            }
            break;
        }
        let tref = unsafe { active.as_ref() }.expect("Active should be always non-null");
        let res = tref.remove(key, guard);
        if res.is_some() {
            self.size.fetch_sub(1, Ordering::Release);
            self.epoch.fetch_add(1, Ordering::Release);
        }
        res
    }

    pub fn lookup<'a>(&self, key: &K, guard: &'a epoch::Guard) -> Option<&'a (K, V)> {
        let mut e_before = self.epoch.load(Ordering::Acquire);
        loop {
            let table = unsafe { self.active.load(Ordering::Acquire, guard).as_ref() };
            let Some(tref) = table else {
                break None;
            };
            if let Some(r) = tref.lookup(key, guard) {
                break Some(r);
            }

            let e_after = self.epoch.load(Ordering::Acquire);
            if e_before != e_after {
                e_before = e_after;
                continue;
            }

            break None;
        }
    }

    pub fn find_or_insert<'a>(&self, key: K, val: V, guard: &'a epoch::Guard) -> &'a (K, V) {
        let node = epoch::Owned::new((key, val)).into_shared(guard);
        let mut active = self.active.load(Ordering::Acquire, guard);
        let res;
        loop {
            let tref = unsafe { active.as_ref() }.expect("Active should be always non-null");
            let nxt = tref.next.load(Ordering::Acquire, guard);
            if !nxt.is_null() {
                self.migrate(tref, nxt, guard);
                active = self.active.load(Ordering::Acquire, guard);
                continue;
            }

            if let Ok(r) = tref.find_or_insert_internal(node, guard) {
                res = r;
                break;
            }
        }
        let tref = unsafe { active.as_ref() }.expect("Active should be always non-null");

        if res.1 {
            let sz = tref.size.load(Ordering::Acquire);
            let cap = tref.mask + 1;
            if cap - sz <= (cap >> 2) + (cap >> 3) || sz >= cap {
                let nt = epoch::Owned::new(ConcurrentHashTable::new(cap << 1));
                self.mpos.store(0, Ordering::Release);
                self.mstarted.store(true, Ordering::Release);
                let _ = tref.next.compare_exchange(
                    epoch::Shared::null(),
                    nt,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                ); // Idempotent fail
            }
            self.size.fetch_add(1, Ordering::Relaxed);
            self.epoch.fetch_add(1, Ordering::Release);
        }

        res.0
    }
}

#[cfg(all(not(feature = "shuttle"), test))]
mod tests {
    use super::*;
    use crate::sync::Arc;

    const KEYS_PER_THREAD: usize = 10000;
    #[test]
    fn concurrent_insert_all_present() {
        let map: Arc<ConcurrentHashMap<usize, usize>> = Arc::new(ConcurrentHashMap::new());
        let mut threads = Vec::new();
        for i in 0..8 {
            let cmap = Arc::clone(&map);
            let h = crate::thread::spawn(move || {
                let guard = &epoch::pin();

                let start = i * KEYS_PER_THREAD;
                let end = start + KEYS_PER_THREAD;
                for k in start..end {
                    cmap.find_or_insert(k, k, guard);
                }
            });
            threads.push(h);
        }

        for t in threads.into_iter() {
            let _ = t.join();
        }

        let guard = &epoch::pin();
        for k in 0..8 * KEYS_PER_THREAD {
            let res = map.lookup(&k, guard);
            assert!(res.is_some());
        }
    }
}

#[cfg(all(feature = "shuttle", test))]
mod tests {
    use super::*;
    use shuttle::sync::Arc;
    use shuttle::thread;

    /// Test 1: Concurrent insert of different keys
    #[test]
    fn concurrent_insert_different_keys() {
        shuttle::check_random(
            || {
                let map: Arc<ConcurrentHashMap<i32, i32>> = Arc::new(ConcurrentHashMap::new());
                let map1 = Arc::clone(&map);
                let map2 = Arc::clone(&map);

                let t1 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    map1.find_or_insert(1, 100, &guard).clone()
                });

                let t2 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    map2.find_or_insert(2, 200, &guard).clone()
                });

                let r1 = t1.join().unwrap();
                let r2 = t2.join().unwrap();

                // Both inserts should succeed
                assert_eq!(r1, (1, 100));
                assert_eq!(r2, (2, 200));

                // Verify both entries are visible
                let guard = crossbeam::epoch::pin();
                let v1 = map.lookup(&1, &guard);
                let v2 = map.lookup(&2, &guard);

                assert!(v1.is_some());
                assert!(v2.is_some());
                assert_eq!(v1.unwrap().1, 100);
                assert_eq!(v2.unwrap().1, 200);
            },
            256,
        );
    }

    /// Test 2: Concurrent insert of same key
    #[test]
    fn concurrent_insert_same_key() {
        shuttle::check_random(
            || {
                let map: Arc<ConcurrentHashMap<i32, i32>> = Arc::new(ConcurrentHashMap::new());
                let map1 = Arc::clone(&map);
                let map2 = Arc::clone(&map);

                let t1 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    map1.find_or_insert(1, 100, &guard).clone()
                });

                let t2 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    map2.find_or_insert(1, 200, &guard).clone()
                });

                let r1 = t1.join().unwrap();
                let r2 = t2.join().unwrap();

                // Both should see the same value (the winner's value)
                assert_eq!(r1, r2);
                assert!(r1 == (1, 100) || r1 == (1, 200));

                // Verify only one entry exists
                let guard = crossbeam::epoch::pin();
                let v = map.lookup(&1, &guard);
                assert!(v.is_some());
                assert_eq!(*v.unwrap(), r1);
            },
            256,
        );
    }

    /// Test 3: Lookup during concurrent inserts
    #[test]
    fn lookup_during_insert() {
        shuttle::check_random(
            || {
                let map: Arc<ConcurrentHashMap<i32, i32>> = Arc::new(ConcurrentHashMap::new());
                let map1 = Arc::clone(&map);
                let map2 = Arc::clone(&map);

                let t1 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    map1.find_or_insert(1, 100, &guard).clone()
                });

                let t2 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    map2.lookup(&1, &guard).copied()
                });

                let insert_result = t1.join().unwrap();
                let lookup_result = t2.join().unwrap();

                // Insert should succeed
                assert_eq!(insert_result, (1, 100));

                // If lookup saw something, it must be the correct value
                if let Some(val) = lookup_result {
                    assert_eq!(val, (1, 100));
                }

                // After both operations, entry must exist
                let guard = crossbeam::epoch::pin();
                let final_lookup = map.lookup(&1, &guard);
                assert!(final_lookup.is_some());
                assert_eq!(final_lookup.unwrap().1, 100);
            },
            256,
        );
    }

    /// Test 4: Concurrent inserts triggering migration
    /// Insert many keys to force table resize and migration multiple times
    #[test]
    fn concurrent_insert_with_migration() {
        shuttle::check_random(
            || {
                let map: Arc<ConcurrentHashMap<i32, i32>> = Arc::new(ConcurrentHashMap::new());
                let map1 = Arc::clone(&map);
                let map2 = Arc::clone(&map);
                let map3 = Arc::clone(&map);
                let map4 = Arc::clone(&map);

                let t1 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    for i in 0..2048 {
                        map1.find_or_insert(i, i * 10, &guard);
                    }
                });

                let t2 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    for i in 2048..4096 {
                        map2.find_or_insert(i, i * 10, &guard);
                    }
                });

                let t3 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    for i in 4096..6144 {
                        map3.find_or_insert(i, i * 10, &guard);
                    }
                });

                let t4 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    for i in 6144..8192 {
                        map4.find_or_insert(i, i * 10, &guard);
                    }
                });

                t1.join().unwrap();
                t2.join().unwrap();
                t3.join().unwrap();
                t4.join().unwrap();

                // Verify all entries are visible
                let guard = crossbeam::epoch::pin();
                for i in 0..8192 {
                    let val = map.lookup(&i, &guard);
                    assert!(val.is_some(), "Key {} should exist", i);
                    assert_eq!(val.unwrap().1, i * 10);
                }
            },
            64,
        );
    }

    /// Test 5: Lookup during migration
    #[test]
    fn lookup_during_migration() {
        shuttle::check_random(
            || {
                let map: Arc<ConcurrentHashMap<i32, i32>> = Arc::new(ConcurrentHashMap::new());
                let map1 = Arc::clone(&map);
                let map2 = Arc::clone(&map);
                let map3 = Arc::clone(&map);
                let map4 = Arc::clone(&map);

                // Pre-insert some values to establish baseline
                {
                    let guard = crossbeam::epoch::pin();
                    for i in 0..100 {
                        map.find_or_insert(i, i * 10, &guard);
                    }
                }

                let t1 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    // Trigger multiple migrations with many inserts
                    for i in 1000..3048 {
                        map1.find_or_insert(i, i * 10, &guard);
                    }
                });

                let t2 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    for i in 3048..5096 {
                        map2.find_or_insert(i, i * 10, &guard);
                    }
                });

                let t3 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    for i in 5096..7144 {
                        map3.find_or_insert(i, i * 10, &guard);
                    }
                });

                let t4 = thread::spawn(move || {
                    let guard = crossbeam::epoch::pin();
                    // Continuously lookup old values during migration
                    let mut results = Vec::new();
                    for _ in 0..100 {
                        for i in 0..100 {
                            results.push(map4.lookup(&i, &guard).copied());
                        }
                    }
                    results
                });

                t1.join().unwrap();
                t2.join().unwrap();
                t3.join().unwrap();
                let lookups = t4.join().unwrap();

                // All lookups should succeed (data should not be lost during migration)
                for (idx, result) in lookups.iter().enumerate() {
                    let i = idx % 100;
                    assert!(result.is_some(), "Key {} should exist during migration", i);
                    assert_eq!(result.unwrap().1, (i * 10) as i32);
                }
            },
            64,
        );
    }
}
