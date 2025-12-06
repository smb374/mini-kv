use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use crate::{decode_float, encode_float};

#[derive(Debug, Default)]
pub struct ZSet {
    index: HashMap<Arc<str>, f64>,
    ordered: BTreeSet<([u8; 8], Arc<str>)>,
}

impl ZSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, score: f64, name: Arc<str>) {
        if let Some(old_score) = self.index.insert(Arc::clone(&name), score) {
            self.ordered
                .remove(&(encode_float(old_score), Arc::clone(&name)));
        }
        self.ordered.insert((encode_float(score), name));
    }

    pub fn score(&self, name: &Arc<str>) -> Option<f64> {
        self.index.get(name).copied()
    }

    pub fn del(&mut self, name: Arc<str>) -> bool {
        if let Some(score) = self.index.remove(&name) {
            self.ordered.remove(&(encode_float(score), name));
            true
        } else {
            false
        }
    }

    pub fn query(
        &self,
        score: f64,
        name: Arc<str>,
        offset: i64,
        limit: usize,
    ) -> Vec<(f64, Arc<str>)> {
        let base = (encode_float(score), name);
        let range: &mut dyn Iterator<Item = &([u8; 8], Arc<str>)> = if offset < 0 {
            let mut range = self.ordered.range(..=base).rev().skip((-offset) as usize);
            match range.next() {
                Some(b) => &mut self.ordered.range(b..),
                None => &mut self.ordered.iter(),
            }
        } else {
            &mut self.ordered.range(base..).skip(offset as usize)
        };
        let mut res = Vec::new();
        for _ in 0..limit {
            if let Some((bits, name)) = range.next() {
                res.push((decode_float(*bits), Arc::clone(name)));
            } else {
                break;
            }
        }
        res
    }
}
