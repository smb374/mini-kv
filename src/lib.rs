pub mod hashtable;

#[cfg(all(feature = "shuttle", test))]
pub use shuttle::{hint, sync, thread};

#[cfg(not(all(feature = "shuttle", test)))]
pub use std::{hint, sync, thread};
